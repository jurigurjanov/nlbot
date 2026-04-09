from __future__ import annotations

from datetime import datetime, timezone
import threading
import time
from typing import Callable
from unittest.mock import patch

import pytest

from xtb_bot.client import BrokerError, MockBrokerClient
from xtb_bot.config import RiskConfig
from xtb_bot.models import ConnectivityStatus, NewsEvent, PendingOpen, Position, PriceTick, RunMode, Side, Signal, StreamHealthStatus, SymbolSpec
from xtb_bot.models import AccountSnapshot
from xtb_bot.multi_strategy import (
    AggregatorLifecycleState,
    NetIntentDecision,
    NormalizedIntent,
    RebalanceMode,
    StrategyIntent,
)
from xtb_bot.position_book import PositionBook
from xtb_bot.risk_manager import RiskManager
from xtb_bot.state_store import StateStore
from xtb_bot.worker import SymbolWorker


class _ExplodingDeque(list):
    def __iter__(self):
        raise RuntimeError("deque mutated during iteration")


class _FakeStrategy:
    min_history = 1

    def __init__(self, side: Side):
        self.side = side

    def generate_signal(self, ctx):
        _ = ctx
        return Signal(
            side=self.side,
            confidence=1.0,
            stop_loss_pips=10,
            take_profit_pips=20,
            metadata={"source": "test"},
        )


class _FixedSignalStrategy:
    min_history = 1

    def __init__(self, signal: Signal):
        self.signal = signal

    def generate_signal(self, ctx):
        _ = ctx
        return self.signal


class _NamedFakeStrategy:
    def __init__(
        self,
        *,
        name: str,
        side: Side,
        confidence: float,
        stop_loss_pips: float,
        take_profit_pips: float,
        min_history: int = 1,
        metadata: dict[str, object] | None = None,
    ) -> None:
        self.name = name
        self.side = side
        self.confidence = confidence
        self.stop_loss_pips = stop_loss_pips
        self.take_profit_pips = take_profit_pips
        self.min_history = max(1, int(min_history))
        self.metadata = dict(metadata or {})

    def supports_symbol(self, symbol: str) -> bool:
        _ = symbol
        return True

    def generate_signal(self, ctx):
        _ = ctx
        return Signal(
            side=self.side,
            confidence=self.confidence,
            stop_loss_pips=self.stop_loss_pips,
            take_profit_pips=self.take_profit_pips,
            metadata={
                "indicator": self.name,
                "reason": f"{self.name}_signal",
                **self.metadata,
            },
        )


class _HighTfContextCaptureStrategy:
    min_history = 10
    candle_timeframe_sec = 900

    def __init__(self) -> None:
        self.seen_context = None

    def generate_signal(self, ctx):
        self.seen_context = ctx
        return Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"reason": "test"},
        )


def _make_worker(
    tmp_path,
    symbol: str = "EURUSD",
    strategy_name: str = "momentum",
    mode: RunMode = RunMode.PAPER,
    min_stop_loss_pips: float = 10.0,
    min_tp_sl_ratio: float = 2.0,
    trailing_activation_ratio: float = 0.5,
    trailing_distance_pips: float = 10.0,
    trailing_breakeven_offset_pips: float = 0.0,
    trailing_breakeven_offset_pips_fx: float | None = None,
    trailing_breakeven_offset_pips_index: float | None = None,
    trailing_breakeven_offset_pips_commodity: float | None = None,
    adaptive_trailing_enabled: bool = False,
    adaptive_trailing_atr_base_multiplier: float = 1.0,
    adaptive_trailing_heat_trigger: float = 0.45,
    adaptive_trailing_heat_full: float = 0.90,
    adaptive_trailing_distance_factor_at_full: float = 0.60,
    adaptive_trailing_profit_lock_r_at_full: float = 0.60,
    adaptive_trailing_profit_lock_min_progress: float = 0.25,
    adaptive_trailing_min_distance_stop_ratio: float = 0.20,
    adaptive_trailing_min_distance_spread_multiplier: float = 1.50,
    session_close_buffer_min: int = 15,
    news_event_buffer_min: int = 5,
    news_filter_enabled: bool = True,
    news_event_action: str = "breakeven",
    spread_filter_enabled: bool = True,
    spread_anomaly_multiplier: float = 3.0,
    spread_avg_window: int = 50,
    spread_min_samples: int = 20,
    spread_pct_filter_enabled: bool = True,
    spread_max_pct: float = 0.0,
    spread_max_pct_cfd: float = 0.20,
    spread_max_pct_crypto: float = 0.50,
    connectivity_check_enabled: bool = True,
    connectivity_max_latency_ms: float = 500.0,
    connectivity_pong_timeout_sec: float = 2.0,
    stream_health_check_enabled: bool = True,
    stream_max_tick_age_sec: float = 15.0,
    stream_event_cooldown_sec: float = 60.0,
    hold_reason_log_interval_sec: float = 60.0,
    momentum_trade_cooldown_sec: float = 0.0,
    momentum_trade_cooldown_win_sec: float | None = None,
    momentum_trade_cooldown_loss_sec: float | None = None,
    momentum_trade_cooldown_flat_sec: float | None = None,
    min_confidence_for_entry: float = 0.0,
    multi_strategy_enabled: bool | None = False,
    strategy_param_overrides: dict[str, object] | None = None,
    strategy_params_map: dict[str, dict[str, object]] | None = None,
    bot_magic_prefix: str = "XTBBOT",
    bot_magic_instance: str = "TEST01",
    db_first_reads_enabled: bool = False,
    db_first_tick_max_age_sec: float | None = None,
    risk_entry_tick_max_age_sec: float = 0.0,
    poll_interval_sec: float = 1.0,
    latest_tick_getter: Callable[[str, float], PriceTick | None] | None = None,
    latest_tick_updater: Callable[[PriceTick], None] | None = None,
) -> SymbolWorker:
    store = StateStore(tmp_path / "worker.db")
    fx_offset = (
        trailing_breakeven_offset_pips
        if trailing_breakeven_offset_pips_fx is None
        else trailing_breakeven_offset_pips_fx
    )
    index_offset = (
        trailing_breakeven_offset_pips
        if trailing_breakeven_offset_pips_index is None
        else trailing_breakeven_offset_pips_index
    )
    commodity_offset = (
        trailing_breakeven_offset_pips
        if trailing_breakeven_offset_pips_commodity is None
        else trailing_breakeven_offset_pips_commodity
    )
    risk = RiskManager(
        RiskConfig(
            min_stop_loss_pips=min_stop_loss_pips,
            min_tp_sl_ratio=min_tp_sl_ratio,
            trailing_activation_ratio=trailing_activation_ratio,
            trailing_distance_pips=trailing_distance_pips,
            trailing_breakeven_offset_pips=trailing_breakeven_offset_pips,
            trailing_breakeven_offset_pips_fx=fx_offset,
            trailing_breakeven_offset_pips_index=index_offset,
            trailing_breakeven_offset_pips_commodity=commodity_offset,
            adaptive_trailing_enabled=adaptive_trailing_enabled,
            adaptive_trailing_atr_base_multiplier=adaptive_trailing_atr_base_multiplier,
            adaptive_trailing_heat_trigger=adaptive_trailing_heat_trigger,
            adaptive_trailing_heat_full=adaptive_trailing_heat_full,
            adaptive_trailing_distance_factor_at_full=adaptive_trailing_distance_factor_at_full,
            adaptive_trailing_profit_lock_r_at_full=adaptive_trailing_profit_lock_r_at_full,
            adaptive_trailing_profit_lock_min_progress=adaptive_trailing_profit_lock_min_progress,
            adaptive_trailing_min_distance_stop_ratio=adaptive_trailing_min_distance_stop_ratio,
            adaptive_trailing_min_distance_spread_multiplier=adaptive_trailing_min_distance_spread_multiplier,
            session_close_buffer_min=session_close_buffer_min,
            news_event_buffer_min=news_event_buffer_min,
            news_filter_enabled=news_filter_enabled,
            news_event_action=news_event_action,
            spread_filter_enabled=spread_filter_enabled,
            spread_anomaly_multiplier=spread_anomaly_multiplier,
            spread_avg_window=spread_avg_window,
            spread_min_samples=spread_min_samples,
            spread_pct_filter_enabled=spread_pct_filter_enabled,
            spread_max_pct=spread_max_pct,
            spread_max_pct_cfd=spread_max_pct_cfd,
            spread_max_pct_crypto=spread_max_pct_crypto,
            connectivity_check_enabled=connectivity_check_enabled,
            connectivity_max_latency_ms=connectivity_max_latency_ms,
            connectivity_pong_timeout_sec=connectivity_pong_timeout_sec,
            stream_health_check_enabled=stream_health_check_enabled,
            stream_max_tick_age_sec=stream_max_tick_age_sec,
            entry_tick_max_age_sec=risk_entry_tick_max_age_sec,
            stream_event_cooldown_sec=stream_event_cooldown_sec,
            hold_reason_log_interval_sec=hold_reason_log_interval_sec,
        ),
        store,
    )
    broker = MockBrokerClient(start_balance=10_000.0)
    broker.connect()

    strategy_params = {
        "fast_window": 3,
        "slow_window": 5,
        "stop_loss_pips": 10,
        "take_profit_pips": 20,
        "momentum_trade_cooldown_sec": momentum_trade_cooldown_sec,
        "min_confidence_for_entry": min_confidence_for_entry,
    }
    if multi_strategy_enabled is not None:
        strategy_params["multi_strategy_enabled"] = bool(multi_strategy_enabled)
    if momentum_trade_cooldown_win_sec is not None:
        strategy_params["momentum_trade_cooldown_win_sec"] = momentum_trade_cooldown_win_sec
    if momentum_trade_cooldown_loss_sec is not None:
        strategy_params["momentum_trade_cooldown_loss_sec"] = momentum_trade_cooldown_loss_sec
    if momentum_trade_cooldown_flat_sec is not None:
        strategy_params["momentum_trade_cooldown_flat_sec"] = momentum_trade_cooldown_flat_sec
    if strategy_param_overrides:
        strategy_params.update(strategy_param_overrides)

    worker = SymbolWorker(
        symbol=symbol,
        mode=mode,
        strategy_name=strategy_name,
        strategy_params=strategy_params,
        broker=broker,
        store=store,
        risk=risk,
        position_book=PositionBook(),
        stop_event=threading.Event(),
        poll_interval_sec=poll_interval_sec,
        poll_jitter_sec=0.0,
        default_volume=0.0,
        bot_magic_prefix=bot_magic_prefix,
        bot_magic_instance=bot_magic_instance,
        db_first_reads_enabled=db_first_reads_enabled,
        db_first_tick_max_age_sec=db_first_tick_max_age_sec,
        strategy_params_map=strategy_params_map,
        latest_tick_getter=latest_tick_getter,
        latest_tick_updater=latest_tick_updater,
    )
    worker.symbol_spec = broker.get_symbol_spec(symbol)
    return worker


def test_worker_run_reraises_unexpected_programming_error(tmp_path):
    worker = _make_worker(tmp_path, poll_interval_sec=0.0)
    try:
        def _boom(*args, **kwargs):
            _ = (args, kwargs)
            raise KeyError("bad signal state")

        worker._generate_signal = _boom  # type: ignore[assignment]

        with pytest.raises(KeyError, match="bad signal state"):
            worker.run()

        assert worker.stop_event.is_set()
        events = [event for event in worker.store.load_events(limit=10) if event.get("message") == "Fatal worker error"]
        assert events
        payload = events[0].get("payload") or {}
        assert payload.get("error_type") == "KeyError"
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_netting_aggregates_to_buy_signal(tmp_path, monkeypatch: pytest.MonkeyPatch):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=12.0,
            take_profit_pips=24.0,
        ),
        "g1": _NamedFakeStrategy(
            name="g1",
            side=Side.SELL,
            confidence=1.0,
            stop_loss_pips=30.0,
            take_profit_pips=60.0,
        ),
        "index_hybrid": _NamedFakeStrategy(
            name="index_hybrid",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=20.0,
            take_profit_pips=40.0,
            metadata={
                "regime": "trend_following",
                "zscore": 2.8,
                "zscore_effective_threshold": 1.6,
                "breakout_distance_ratio": 0.009,
                "index_min_breakout_distance_ratio": 0.004,
                "atr_pips": 18.0,
            },
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies["momentum" if name == "momentum_fx" else name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g1,index_hybrid",
            "multi_strategy_weights": {"momentum": 1.0, "g1": 0.5, "index_hybrid": 1.0},
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
        },
    )
    try:
        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.0,
        )
        assert worker._multi_strategy_enabled is True
        assert signal.side == Side.BUY
        assert signal.stop_loss_pips == pytest.approx(16.0)
        assert signal.take_profit_pips == pytest.approx(32.0)
        assert signal.metadata.get("indicator") == "multi_strategy_netting"
        assert signal.metadata.get("multi_component_count") == 3
        assert signal.metadata.get("conflict_detected") is False
        assert signal.metadata.get("multi_representative_strategy") == "index_hybrid"
        assert float(signal.metadata.get("multi_weighted_stop_loss_pips") or 0.0) == pytest.approx(16.0)
        assert float(signal.metadata.get("multi_weighted_take_profit_pips") or 0.0) == pytest.approx(32.0)
        assert float(signal.metadata.get("multi_weighted_stop_coverage") or 0.0) == pytest.approx(1.0)
        assert float(signal.metadata.get("multi_weighted_take_profit_coverage") or 0.0) == pytest.approx(1.0)
        assert signal.metadata.get("multi_entry_strategy") == "momentum"
        assert signal.metadata.get("multi_entry_strategy_component") == "momentum"
        assert signal.metadata.get("multi_dominant_component_strategy") == "momentum"
        assert worker._effective_entry_strategy_from_signal(signal) == "momentum"
        assert worker._effective_entry_component_from_signal(signal) == "momentum"
        assert signal.metadata.get("regime") == "trend_following"
        assert signal.metadata.get("zscore") == pytest.approx(2.8)
        assert signal.metadata.get("zscore_effective_threshold") == pytest.approx(1.6)
        assert signal.metadata.get("breakout_distance_ratio") == pytest.approx(0.009)
        assert signal.metadata.get("index_min_breakout_distance_ratio") == pytest.approx(0.004)
        assert signal.metadata.get("atr_pips") == pytest.approx(18.0)

        virtual_rows = worker.store.load_virtual_positions_state(symbol="EURUSD")
        by_strategy = {str(item["strategy_id"]): float(item["qty_lots"]) for item in virtual_rows}
        assert by_strategy == {
            "g1": pytest.approx(-0.75),
            "index_hybrid": pytest.approx(1.5),
            "momentum": pytest.approx(1.5),
        }
        real_rows = worker.store.load_real_positions_cache()
        assert len(real_rows) == 1
        assert str(real_rows[0]["symbol"]) == "EURUSD"
        assert float(real_rows[0]["real_qty_lots"]) == pytest.approx(0.0)
        assert worker.store.load_system_errors() == []
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_blocks_unconfirmed_secondary_donchian_breakout_open(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=12.0,
            take_profit_pips=24.0,
        ),
        "g1": _NamedFakeStrategy(
            name="g1",
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=20.0,
            take_profit_pips=40.0,
        ),
        "donchian_breakout": _NamedFakeStrategy(
            name="donchian_breakout",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=18.0,
            take_profit_pips=54.0,
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies["momentum" if name == "momentum_fx" else name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g1,donchian_breakout",
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
            "multi_strategy_secondary_weights": {"donchian_breakout": 0.35},
        },
    )
    try:
        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.0,
        )

        assert signal.side == Side.HOLD
        assert signal.metadata.get("reason") == "multi_secondary_breakout_unconfirmed"
        assert signal.metadata.get("multi_dominant_component_strategy") == "donchian_breakout"
        assert signal.metadata.get("multi_secondary_breakout_support", {}).get("secondary_breakout_support_ratio") == pytest.approx(0.0)
        components = signal.metadata.get("multi_components") or {}
        assert float(components["donchian_breakout"]["secondary_weight_multiplier"]) == pytest.approx(0.35)
        assert worker._effective_entry_strategy_from_signal(signal) == "momentum"
        assert worker._effective_entry_component_from_signal(signal) == "donchian_breakout"
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_netting_conflict_returns_hold(tmp_path, monkeypatch: pytest.MonkeyPatch):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
        "g1": _NamedFakeStrategy(
            name="g1",
            side=Side.SELL,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies["momentum" if name == "momentum_fx" else name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g1",
            "multi_strategy_weights": {"momentum": 1.0, "g1": 1.0},
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
            "multi_strategy_min_conflict_power": 0.05,
            "multi_strategy_conflict_ratio_low": 0.8,
            "multi_strategy_conflict_ratio_high": 1.25,
        },
    )
    try:
        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.0,
        )
        assert signal.side == Side.HOLD
        assert signal.metadata.get("reason") == "multi_conflict_hold"
        assert signal.metadata.get("conflict_detected") is True
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_all_hold_is_classified_with_directional_summary(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"reason": "no_signal", "indicator": "momentum"},
        ),
        "g1": _NamedFakeStrategy(
            name="g1",
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"reason": "adx_below_threshold", "indicator": "g1"},
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g1",
            "multi_strategy_weights": {"momentum": 1.0, "g1": 1.0},
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
        },
    )
    try:
        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.0,
        )
        assert signal.side == Side.HOLD
        assert signal.metadata.get("reason") == "multi_all_hold"
        assert int(signal.metadata.get("multi_directional_component_count", -1)) == 0
        assert int(signal.metadata.get("multi_hold_component_count", -1)) == 2
        assert int(signal.metadata.get("multi_neutral_component_count", -1)) == 1
        assert int(signal.metadata.get("multi_blocked_component_count", -1)) == 1
        assert int(signal.metadata.get("multi_unavailable_component_count", -1)) == 0
        assert int(signal.metadata.get("multi_hold_reason_count", -1)) == 2
        assert signal.metadata.get("multi_top_hold_reasons") == "adx_below_threshold(1), no_signal(1)"
        assert signal.metadata.get("multi_top_neutral_reasons") == "no_signal(1)"
        assert signal.metadata.get("multi_top_blocked_reasons") == "adx_below_threshold(1)"
        assert signal.metadata.get("multi_hold_reason_by_strategy") == "momentum=no_signal; g1=adx_below_threshold"
        assert float(signal.metadata.get("multi_dominant_power") or 0.0) == pytest.approx(0.0)
        assert "multi_representative_strategy" not in signal.metadata
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_all_neutral_hold_is_classified_as_no_setup(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"reason": "no_signal", "indicator": "momentum"},
        ),
        "g1": _NamedFakeStrategy(
            name="g1",
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"reason": "no_ema_cross", "indicator": "g1"},
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g1",
            "multi_strategy_weights": {"momentum": 1.0, "g1": 1.0},
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
        },
    )
    try:
        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.0,
        )
        assert signal.side == Side.HOLD
        assert signal.metadata.get("reason") == "multi_no_setup"
        assert signal.metadata.get("hold_state") == "neutral"
        assert int(signal.metadata.get("multi_neutral_component_count", -1)) == 2
        assert signal.metadata.get("multi_top_neutral_reasons") == "no_ema_cross(1), no_signal(1)"
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_summarizes_soft_and_state_specific_reasons(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "reason": "no_signal",
                "indicator": "momentum",
                "soft_filter_reasons": ["higher_tf_bias_mismatch"],
            },
        ),
        "g1": _NamedFakeStrategy(
            name="g1",
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "reason": "adx_below_threshold",
                "indicator": "g1",
                "soft_filter_reasons": ["kama_chop_regime"],
            },
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g1",
            "multi_strategy_weights": {"momentum": 1.0, "g1": 1.0},
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
        },
    )
    try:
        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.0,
        )
        assert signal.side == Side.HOLD
        assert signal.metadata.get("reason") == "multi_all_hold"
        assert signal.metadata.get("multi_top_soft_reasons") == "higher_tf_bias_mismatch(1), kama_chop_regime(1)"
        assert signal.metadata.get("multi_soft_reason_by_strategy") == (
            "g1=kama_chop_regime; momentum=higher_tf_bias_mismatch"
        )
        assert signal.metadata.get("multi_blocked_reason_by_strategy") == "g1=adx_below_threshold"
        assert signal.metadata.get("multi_neutral_reason_by_strategy") == "momentum=no_signal"
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_unavailable_components_are_classified_separately(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"reason": "insufficient_candle_history", "indicator": "momentum"},
        ),
        "g2": _NamedFakeStrategy(
            name="g2",
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"reason": "higher_tf_bias_unavailable", "indicator": "g2"},
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g2",
            "multi_strategy_weights": {"momentum": 1.0, "g2": 1.0},
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
        },
    )
    try:
        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.0,
        )
        assert signal.side == Side.HOLD
        assert signal.metadata.get("reason") == "multi_components_unavailable"
        assert signal.metadata.get("hold_state") == "unavailable"
        assert int(signal.metadata.get("multi_unavailable_component_count", -1)) == 2
        assert int(signal.metadata.get("multi_active_component_count", -1)) == 0
        assert signal.metadata.get("multi_top_unavailable_reasons") == (
            "higher_tf_bias_unavailable(1), insufficient_candle_history(1)"
        )
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_balanced_flat_is_classified_without_conflict_hold(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "alpha": _NamedFakeStrategy(
            name="alpha",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
        "beta": _NamedFakeStrategy(
            name="beta",
            side=Side.SELL,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="alpha",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "alpha,beta",
            "multi_strategy_weights": {"alpha": 1.0, "beta": 0.96},
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
            "multi_strategy_min_conflict_power": 2.0,
        },
    )
    try:
        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.0,
        )
        assert signal.side == Side.HOLD
        assert signal.metadata.get("reason") == "multi_balanced_flat"
        assert int(signal.metadata.get("multi_directional_component_count") or -1) == 2
        assert float(signal.metadata.get("buy_power") or 0.0) == pytest.approx(1.0)
        assert float(signal.metadata.get("sell_power") or 0.0) == pytest.approx(0.96)
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_confidence_uses_representative_signal_strength(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.BUY,
            confidence=0.9,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
        "g1": _NamedFakeStrategy(
            name="g1",
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g1",
            "multi_strategy_weights": {"momentum": 1.0, "g1": 1.0},
            "multi_strategy_normalizer_min_samples": 32,
            "multi_strategy_normalizer_default": 0.5,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
        },
    )
    try:
        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.0,
        )
        assert signal.side == Side.BUY
        assert signal.confidence == pytest.approx(0.9)
        assert float(signal.metadata.get("multi_confidence_net_qty") or 0.0) == pytest.approx(0.5)
        assert float(signal.metadata.get("multi_confidence_representative") or 0.0) == pytest.approx(0.9)
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_confidence_is_capped_by_representative_signal(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
        "index_hybrid": _NamedFakeStrategy(
            name="index_hybrid",
            side=Side.BUY,
            confidence=0.46,
            stop_loss_pips=24.0,
            take_profit_pips=48.0,
            metadata={
                "regime": "mean_reversion",
                "confidence_threshold_cap": 0.58,
            },
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        symbol="US30",
        strategy_name="momentum",
        mode=RunMode.EXECUTION,
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,index_hybrid",
            "multi_strategy_weights": {"momentum": 1.0, "index_hybrid": 1.45},
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
            "momentum_execution_min_confidence_for_entry": 0.65,
            "index_hybrid_execution_min_confidence_for_entry": 0.70,
        },
    )
    try:
        worker.prices.append(46000.0)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=2.0,
            ),
            real_position_lots=0.0,
        )
        assert signal.side == Side.BUY
        assert signal.confidence == pytest.approx(0.46)
        assert signal.metadata.get("confidence_threshold_cap") == pytest.approx(0.58)

        threshold, cap = worker._effective_min_confidence_for_signal(signal)
        assert threshold == pytest.approx(0.58)
        assert cap == pytest.approx(0.58)
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_default_multi_strategy_names_include_donchian_and_exclude_oil(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
        },
    )
    try:
        names = worker._resolve_multi_strategy_names({"multi_strategy_enabled": True})

        assert "donchian_breakout" in names
        assert "oil" not in names
        assert names[0] == "momentum"
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_derives_oil_confidence_cap_for_oil_contract_signal(tmp_path):
    worker = _make_worker(
        tmp_path,
        symbol="WTI",
        min_confidence_for_entry=0.68,
    )
    try:
        signal = Signal(
            side=Side.BUY,
            confidence=0.46,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "indicator": "oil",
                "oil_contrarian_reversal": True,
                "base_indicator": "g1",
                "base_trend_signal": "ema_trend_up",
            },
        )

        threshold, cap = worker._effective_min_confidence_for_signal(signal)
        assert threshold == pytest.approx(0.45)
        assert cap == pytest.approx(0.45)
        assert worker._confidence_allows_open(signal) is True
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_propagates_index_trend_guard_metadata_from_representative(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
        "index_hybrid": _NamedFakeStrategy(
            name="index_hybrid",
            side=Side.BUY,
            confidence=0.74,
            stop_loss_pips=24.0,
            take_profit_pips=48.0,
            metadata={
                "regime": "trend_following",
                "breakout_distance_ratio": 0.006,
                "breakout_distance_pips": 3.0,
                "entry_quality_status": "penalized",
                "entry_quality_penalty": 0.22,
                "entry_quality_reasons": ["choppy_path"],
                "index_trend_session_open_delay_minutes": 15,
                "minutes_since_trend_session_open": 23,
                "trend_session_open_delay_active": False,
            },
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        symbol="DE40",
        strategy_name="momentum",
        mode=RunMode.EXECUTION,
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,index_hybrid",
            "multi_strategy_weights": {"momentum": 1.0, "index_hybrid": 1.45},
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
            "entry_index_trend_min_breakout_to_spread_ratio": 2.0,
            "entry_index_trend_max_entry_quality_penalty": 0.12,
        },
    )
    try:
        worker.prices.append(24000.0)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=1.0,
            ),
            real_position_lots=0.0,
        )
        assert signal.side == Side.BUY
        assert worker._effective_entry_component_from_signal(signal) == "index_hybrid"
        assert signal.metadata.get("breakout_distance_pips") == pytest.approx(3.0)
        assert signal.metadata.get("entry_quality_status") == "penalized"
        assert signal.metadata.get("entry_quality_penalty") == pytest.approx(0.22)
        assert signal.metadata.get("entry_quality_reasons") == ["choppy_path"]
        assert signal.metadata.get("index_trend_session_open_delay_minutes") == 15
        assert signal.metadata.get("minutes_since_trend_session_open") == 23
        assert signal.metadata.get("trend_session_open_delay_active") is False
        assert worker._index_trend_structure_allows_open(signal, current_spread_pips=1.0) is False
    finally:
        worker.broker.close()
        worker.store.close()


def test_effective_min_confidence_uses_index_hybrid_mean_reversion_override(tmp_path):
    worker = _make_worker(
        tmp_path,
        symbol="US30",
        strategy_name="index_hybrid",
        mode=RunMode.EXECUTION,
        strategy_param_overrides={
            "index_hybrid_execution_min_confidence_for_entry": 0.70,
            "index_hybrid_mean_reversion_execution_min_confidence_for_entry": 0.42,
            "index_hybrid_mean_reversion_execution_confidence_hard_floor": 0.55,
        },
    )
    try:
        threshold, cap = worker._effective_min_confidence_for_signal(
            Signal(
                side=Side.BUY,
                confidence=0.55,
                stop_loss_pips=20.0,
                take_profit_pips=40.0,
                metadata={"regime": "mean_reversion"},
            )
        )
        assert threshold == pytest.approx(0.55)
        assert cap is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_effective_min_confidence_hard_floor_overrides_lower_cap_for_index_hybrid_mean_reversion(tmp_path):
    worker = _make_worker(
        tmp_path,
        symbol="US30",
        strategy_name="index_hybrid",
        mode=RunMode.EXECUTION,
        strategy_param_overrides={
            "index_hybrid_execution_min_confidence_for_entry": 0.70,
            "index_hybrid_mean_reversion_execution_min_confidence_for_entry": 0.42,
            "index_hybrid_mean_reversion_execution_confidence_hard_floor": 0.55,
        },
    )
    try:
        threshold, cap = worker._effective_min_confidence_for_signal(
            Signal(
                side=Side.BUY,
                confidence=0.51,
                stop_loss_pips=20.0,
                take_profit_pips=40.0,
                metadata={
                    "regime": "mean_reversion",
                    "confidence_threshold_cap": 0.48,
                },
            )
        )
        assert threshold == pytest.approx(0.55)
        assert cap == pytest.approx(0.48)
    finally:
        worker.broker.close()
        worker.store.close()


def test_effective_min_confidence_uses_index_hybrid_trend_default_when_no_regime_override(tmp_path):
    worker = _make_worker(
        tmp_path,
        symbol="US30",
        strategy_name="index_hybrid",
        mode=RunMode.EXECUTION,
        strategy_param_overrides={
            "index_hybrid_execution_min_confidence_for_entry": 0.70,
            "index_hybrid_mean_reversion_execution_min_confidence_for_entry": 0.42,
        },
    )
    try:
        threshold, cap = worker._effective_min_confidence_for_signal(
            Signal(
                side=Side.BUY,
                confidence=0.61,
                stop_loss_pips=20.0,
                take_profit_pips=40.0,
                metadata={
                    "regime": "trend_following",
                    "confidence_threshold_cap": 0.58,
                },
            )
        )
        assert threshold == pytest.approx(0.58)
        assert cap == pytest.approx(0.58)
    finally:
        worker.broker.close()
        worker.store.close()


def test_effective_min_confidence_derives_index_hybrid_mean_reversion_floor_from_cap_when_missing(tmp_path):
    worker = _make_worker(
        tmp_path,
        symbol="US30",
        strategy_name="index_hybrid",
        mode=RunMode.EXECUTION,
        strategy_param_overrides={
            "index_hybrid_execution_min_confidence_for_entry": 0.70,
            "index_hybrid_mean_reversion_execution_min_confidence_for_entry": 0.42,
        },
    )
    try:
        threshold, cap = worker._effective_min_confidence_for_signal(
            Signal(
                side=Side.BUY,
                confidence=0.54,
                stop_loss_pips=20.0,
                take_profit_pips=40.0,
                metadata={
                    "regime": "mean_reversion",
                    "confidence_threshold_cap": 0.58,
                },
            )
        )
        assert threshold == pytest.approx(0.55)
        assert cap == pytest.approx(0.58)
    finally:
        worker.broker.close()
        worker.store.close()


def test_resolve_mode_scoped_strategy_param_returns_none_for_missing_entry_params(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        assert worker._resolve_mode_scoped_strategy_param(None, "g1", "min_confidence_for_entry") is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_latest_price_helper_handles_empty_and_non_finite_samples(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        assert worker._latest_price() is None
        worker.prices.append(float("nan"))
        assert worker._latest_price() is None
        worker.prices.clear()
        worker.prices.append(1.2345)
        assert worker._latest_price() == pytest.approx(1.2345)
    finally:
        worker.broker.close()
        worker.store.close()


def test_confidence_allows_open_blocks_index_hybrid_mean_reversion_below_derived_floor(tmp_path):
    worker = _make_worker(
        tmp_path,
        symbol="US30",
        strategy_name="index_hybrid",
        mode=RunMode.EXECUTION,
        strategy_param_overrides={
            "index_hybrid_execution_min_confidence_for_entry": 0.70,
            "index_hybrid_mean_reversion_execution_min_confidence_for_entry": 0.42,
        },
    )
    try:
        signal = Signal(
            side=Side.BUY,
            confidence=0.547,
            stop_loss_pips=20.0,
            take_profit_pips=40.0,
            metadata={
                "regime": "mean_reversion",
                "confidence_threshold_cap": 0.58,
            },
        )
        assert worker._confidence_allows_open(signal) is False
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_restores_more_than_5000_samples_when_buffer_is_larger(tmp_path):
    worker = _make_worker(tmp_path, symbol="US30", strategy_name="g2")
    try:
        for idx in range(7000):
            worker.store.append_price_sample(
                "US30",
                ts=1_700_000_000.0 + idx,
                close=40_000.0 + idx * 0.1,
                volume=1.0,
                max_rows_per_symbol=20_000,
            )
    finally:
        worker.broker.close()
        worker.store.close()

    restored_worker = _make_worker(tmp_path, symbol="US30", strategy_name="g2")
    try:
        restored_worker.prices.clear()
        restored_worker.price_timestamps.clear()
        restored_worker.volumes.clear()

        restored_worker._restore_price_history()

        assert restored_worker.prices.maxlen == 20_000
        assert restored_worker.price_history_restore_rows == 20_000
        assert len(restored_worker.prices) == 7000
        assert len(restored_worker.price_timestamps) == 7000
    finally:
        restored_worker.broker.close()
        restored_worker.store.close()


def test_worker_multi_strategy_smart_netting_uses_hard_trend_regime_multiplier(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
        "mean_reversion_bb": _NamedFakeStrategy(
            name="mean_reversion_bb",
            side=Side.SELL,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "regime": "mean_reversion",
                "distance_sigma": 2.2,
                "mean_reversion_extreme_abs_zscore": 2.0,
            },
        ),
        "index_hybrid": _NamedFakeStrategy(
            name="index_hybrid",
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=12.0,
            take_profit_pips=24.0,
            metadata={
                "regime": "trend_following",
                "trend_regime": True,
                "mean_reversion_regime": False,
                "trend_regime_strict": True,
                "mean_reversion_regime_strict": False,
                "breakout_up": True,
            },
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,mean_reversion_bb,index_hybrid",
            "multi_strategy_weights": {
                "momentum": 1.0,
                "mean_reversion_bb": 1.0,
                "index_hybrid": 1.0,
            },
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
            "multi_strategy_min_conflict_power": 0.05,
            "multi_strategy_conflict_ratio_low": 0.8,
            "multi_strategy_conflict_ratio_high": 1.25,
        },
    )
    try:
        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.0,
        )
        assert signal.side == Side.BUY
        assert signal.metadata.get("conflict_detected") is False
        assert signal.metadata.get("multi_regime_mode") == "hard_trend"
        assert float(signal.metadata.get("target_net_qty_lots") or 0.0) == pytest.approx(1.3)

        components = signal.metadata.get("multi_components") or {}
        momentum = components["momentum"]
        mean_reversion = components["mean_reversion_bb"]
        assert momentum.get("regime_family") == "trend"
        assert mean_reversion.get("regime_family") == "mean_reversion"
        assert float(momentum.get("regime_multiplier") or 0.0) == pytest.approx(1.5)
        assert float(mean_reversion.get("regime_multiplier") or 0.0) == pytest.approx(0.2)
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_normalizer_warm_start_uses_state_store_history(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
        "g1": _NamedFakeStrategy(
            name="g1",
            side=Side.SELL,
            confidence=0.1,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    def _seeded_confidence(self, *, symbol: str, strategy_entry: str, limit: int = 256, mode: str | None = None):
        _ = (self, symbol, limit, mode)
        if strategy_entry == "momentum":
            return [0.0] * 64
        if strategy_entry == "g1":
            return [1.0] * 64
        return []

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    monkeypatch.setattr(
        StateStore,
        "load_recent_strategy_entry_confidence_samples",
        _seeded_confidence,
    )
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g1",
            "multi_strategy_weights": {"momentum": 1.0, "g1": 1.0},
            "multi_strategy_normalizer_min_samples": 32,
            "multi_strategy_normalizer_default": 0.5,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
            "multi_strategy_min_conflict_power": 0.05,
            "multi_strategy_conflict_ratio_low": 0.8,
            "multi_strategy_conflict_ratio_high": 1.25,
        },
    )
    try:
        normalizer = worker._multi_strategy_aggregator.normalizer  # type: ignore[union-attr]
        assert len(normalizer._history["momentum"]) == 64
        assert len(normalizer._history["g1"]) == 64

        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.0,
        )
        assert signal.side == Side.BUY
        assert signal.metadata.get("conflict_detected") is False
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_scale_out_allocation_prioritizes_senior_strategy(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g1",
            "multi_strategy_seniority": {"momentum": 2.0, "g1": 1.0},
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
        },
    )
    try:
        now = time.time()
        momentum_intent = StrategyIntent.new(
            strategy_id="momentum",
            symbol="EURUSD",
            seq=1,
            market_data_seq=1,
            target_qty_lots=1.0,
            max_abs_qty_lots=1.0,
            confidence_raw=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            ttl_sec=60.0,
            created_ts=now,
        )
        g1_intent = StrategyIntent.new(
            strategy_id="g1",
            symbol="EURUSD",
            seq=1,
            market_data_seq=1,
            target_qty_lots=0.5,
            max_abs_qty_lots=1.0,
            confidence_raw=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            ttl_sec=60.0,
            created_ts=now,
        )
        worker._last_multi_strategy_decision = NetIntentDecision(
            symbol="EURUSD",
            state=AggregatorLifecycleState.LIVE_SYNCED,
            mode=RebalanceMode.NORMAL,
            reduce_only=False,
            requested_net_qty_lots=1.5,
            target_net_qty_lots=1.5,
            execution_delta_lots=0.0,
            buy_power=1.5,
            sell_power=0.0,
            weighted_stop_loss_pips=10.0,
            weighted_take_profit_pips=20.0,
            weighted_stop_coverage=1.0,
            weighted_take_profit_coverage=1.0,
            conflict_detected=False,
            intents=(
                NormalizedIntent(
                    intent=momentum_intent,
                    confidence_norm=1.0,
                    effective_weight=1.0,
                    weighted_qty=1.0,
                ),
                NormalizedIntent(
                    intent=g1_intent,
                    confidence_norm=1.0,
                    effective_weight=1.0,
                    weighted_qty=0.5,
                ),
            ),
        )
        worker._multi_strategy_scale_out_offset_by_name = {"momentum": 0.0, "g1": 0.0}

        position = Position(
            position_id="pos-seniority-fill",
            symbol="EURUSD",
            side=Side.BUY,
            volume=1.0,
            open_price=1.1000,
            stop_loss=1.0950,
            take_profit=1.1100,
            opened_at=now - 60.0,
            status="open",
        )
        worker._apply_multi_strategy_scale_out_allocation(position, closed_volume=0.2)

        assert worker._multi_strategy_scale_out_offset_by_name["momentum"] == pytest.approx(0.2)
        assert worker._multi_strategy_scale_out_offset_by_name["g1"] == pytest.approx(0.0)
        events = worker.store.load_events(limit=10)
        allocation_events = [
            event
            for event in events
            if event.get("message") == "Multi-strategy scale-out allocation applied"
        ]
        assert allocation_events
        payload = allocation_events[0].get("payload") or {}
        assert str(payload.get("allocation_mode") or "") == "seniority_first"
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_representative_selection_prefers_lower_execution_drag(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=30.0,
            metadata={
                "suggested_order_type": "limit",
                "entry_quality_penalty": 0.30,
            },
        ),
        "index_hybrid": _NamedFakeStrategy(
            name="index_hybrid",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=25.0,
            take_profit_pips=75.0,
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,index_hybrid",
            "multi_strategy_weights": {"momentum": 1.0, "index_hybrid": 1.0},
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
        },
    )
    try:
        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=2.0,
            ),
            real_position_lots=0.0,
        )
        assert signal.side == Side.BUY
        assert signal.stop_loss_pips == pytest.approx(17.5)
        assert signal.take_profit_pips == pytest.approx(52.5)
        assert signal.metadata.get("multi_representative_strategy") == "index_hybrid"
        assert float(signal.metadata.get("multi_weighted_stop_loss_pips") or 0.0) == pytest.approx(17.5)
        assert float(signal.metadata.get("multi_weighted_take_profit_pips") or 0.0) == pytest.approx(52.5)
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_soft_overlap_returns_hold_when_flat(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
        "g1": _NamedFakeStrategy(
            name="g1",
            side=Side.SELL,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g1",
            "multi_strategy_weights": {"momentum": 1.0, "g1": 0.62},
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
            "multi_strategy_min_conflict_power": 0.05,
            "multi_strategy_conflict_ratio_low": 0.8,
            "multi_strategy_conflict_ratio_high": 1.25,
        },
    )
    try:
        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.0,
        )
        assert signal.side == Side.HOLD
        assert signal.metadata.get("reason") == "multi_overlap_hold"
        assert signal.metadata.get("multi_soft_overlap_detected") is True
        assert float(signal.metadata.get("multi_soft_overlap_ratio", 0.0)) > 0.58
        assert signal.metadata.get("conflict_detected") is False
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_deweights_strategy_with_poor_recent_fill_quality(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
        "g1": _NamedFakeStrategy(
            name="g1",
            side=Side.SELL,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g1",
            "multi_strategy_weights": {"momentum": 1.0, "g1": 0.96},
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
        },
    )
    try:
        for idx in range(3):
            closed = Position(
                position_id=f"fill-quality-{idx}",
                symbol="EURUSD",
                side=Side.BUY,
                volume=0.10,
                open_price=1.1000,
                stop_loss=1.0950,
                take_profit=1.1100,
                opened_at=1_800_000_000.0 + idx * 100.0,
                status="closed",
                close_price=1.1020,
                closed_at=1_800_000_050.0 + idx * 100.0,
                pnl=20.0,
            )
            worker.store.upsert_trade(
                closed,
                worker.name,
                worker.strategy_name,
                worker.mode.value,
                strategy_entry="momentum",
            )
            worker.store.update_trade_execution_quality(
                position_id=closed.position_id,
                symbol=closed.symbol,
                entry_adverse_slippage_pips=0.4,
                exit_adverse_slippage_pips=0.4,
            )

        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.5,
            ),
            real_position_lots=0.0,
        )
        assert signal.side == Side.SELL
        components = signal.metadata.get("multi_components") or {}
        momentum_component = components["momentum"]
        assert float(momentum_component["fill_quality_weight_multiplier"]) < 1.0
        assert int(momentum_component["fill_quality_sample_count"]) == 3
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_index_family_weights_shift_ownership_to_index_hybrid(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
        "g1": _NamedFakeStrategy(
            name="g1",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
        "index_hybrid": _NamedFakeStrategy(
            name="index_hybrid",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=24.0,
            take_profit_pips=60.0,
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        symbol="DE40",
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g1,index_hybrid",
            "multi_strategy_weights": {"momentum": 1.0, "g1": 1.0, "index_hybrid": 1.0},
            "multi_strategy_index_weights": {"momentum": 0.9, "g1": 0.35, "index_hybrid": 1.4},
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
        },
    )
    try:
        worker.prices.append(21000.0)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=1.0,
            ),
            real_position_lots=0.0,
        )
        assert signal.side == Side.BUY
        assert signal.metadata.get("multi_asset_class") == "index"
        assert signal.metadata.get("multi_representative_strategy") == "index_hybrid"
        family_weights = signal.metadata.get("multi_family_weight_by_strategy") or {}
        assert family_weights == {
            "momentum": pytest.approx(0.9),
            "g1": pytest.approx(0.35),
            "index_hybrid": pytest.approx(1.4),
        }

        virtual_rows = worker.store.load_virtual_positions_state(symbol="DE40")
        by_strategy = {str(item["strategy_id"]): float(item["qty_lots"]) for item in virtual_rows}
        assert by_strategy["index_hybrid"] > by_strategy["momentum"] > by_strategy["g1"]
        assert by_strategy["index_hybrid"] / by_strategy["momentum"] == pytest.approx(1.4 / 0.9)
        assert by_strategy["g1"] / by_strategy["momentum"] == pytest.approx(0.35 / 0.9)
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_fx_family_weights_preserve_fx_ownership_bias(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=12.0,
            take_profit_pips=24.0,
        ),
        "g1": _NamedFakeStrategy(
            name="g1",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
        "index_hybrid": _NamedFakeStrategy(
            name="index_hybrid",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=20.0,
            take_profit_pips=50.0,
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        symbol="EURUSD",
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g1,index_hybrid",
            "multi_strategy_weights": {"momentum": 1.0, "g1": 1.0, "index_hybrid": 1.0},
            "multi_strategy_fx_weights": {"momentum": 1.2, "g1": 1.05, "index_hybrid": 0.1},
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
        },
    )
    try:
        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.0,
        )
        assert signal.side == Side.BUY
        assert signal.metadata.get("multi_asset_class") == "fx"
        assert signal.metadata.get("multi_representative_strategy") == "momentum"
        family_weights = signal.metadata.get("multi_family_weight_by_strategy") or {}
        assert family_weights == {
            "momentum": pytest.approx(1.2),
            "g1": pytest.approx(1.05),
            "index_hybrid": pytest.approx(0.1),
        }
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_enabled_by_default_when_flag_not_provided(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
        "g1": _NamedFakeStrategy(
            name="g1",
            side=Side.SELL,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        multi_strategy_enabled=None,
        strategy_param_overrides={
            "multi_strategy_names": "momentum,g1",
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
        },
    )
    try:
        assert worker._multi_strategy_enabled is True
        assert [name for name, _strategy in worker._multi_strategy_components] == ["momentum", "g1"]
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_default_components_include_g2(tmp_path):
    worker = _make_worker(
        tmp_path,
        symbol="US100",
        strategy_name="momentum",
        multi_strategy_enabled=None,
    )
    try:
        assert worker._multi_strategy_enabled is True
        component_names = [name for name, _strategy in worker._multi_strategy_components]
        assert "g2" in component_names
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_components_use_per_strategy_params(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    created_params: dict[str, dict[str, object]] = {}

    def _factory(name: str, params: dict[str, object]):
        normalized = str(name).strip().lower()
        created_params[normalized] = dict(params)
        return _NamedFakeStrategy(
            name=normalized,
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        )

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        multi_strategy_enabled=True,
        strategy_param_overrides={
            "base_unique_marker": 11,
            "multi_strategy_names": "momentum,g1,mean_breakout_v2",
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
        },
        strategy_params_map={
            "g1": {
                "g1_unique_marker": 22,
                "g1_fast_window": 7,
                "g1_slow_window": 14,
            },
            "mean_breakout_v2": {
                "mb_unique_marker": 33,
                "mb_candle_timeframe_sec": 60,
            },
        },
    )
    try:
        assert worker._multi_strategy_enabled is True
        assert [name for name, _strategy in worker._multi_strategy_components] == [
            "momentum",
            "g1",
            "mean_breakout_v2",
        ]

        assert created_params["momentum"]["base_unique_marker"] == 11
        assert created_params["g1"]["g1_unique_marker"] == 22
        assert created_params["mean_breakout_v2"]["mb_unique_marker"] == 33
        assert created_params["mean_breakout_v2"]["mb_candle_timeframe_sec"] == 60
        assert created_params["g1"].get("base_unique_marker") is None
        assert created_params["g1"]["_worker_mode"] == RunMode.PAPER.value
    finally:
        worker.broker.close()
        worker.store.close()


@pytest.mark.parametrize(
    ("symbol", "expected_runtime_name", "alias_marker_key", "alias_marker_value"),
    [
        ("EURUSD", "momentum_fx", "fx_unique_marker", 44),
        ("US100", "momentum_index", "index_unique_marker", 55),
    ],
)
def test_worker_multi_strategy_momentum_uses_asset_specific_runtime_alias(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    symbol: str,
    expected_runtime_name: str,
    alias_marker_key: str,
    alias_marker_value: int,
):
    created_params: dict[str, dict[str, object]] = {}

    def _factory(name: str, params: dict[str, object]):
        normalized = str(name).strip().lower()
        created_params[normalized] = dict(params)
        return _NamedFakeStrategy(
            name=normalized,
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        )

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        symbol=symbol,
        strategy_name="momentum",
        multi_strategy_enabled=True,
        strategy_param_overrides={
            "base_unique_marker": 11,
            "multi_strategy_names": "momentum,g1",
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
        },
        strategy_params_map={
            expected_runtime_name: {
                alias_marker_key: alias_marker_value,
            },
            "g1": {
                "g1_unique_marker": 22,
            },
        },
    )
    try:
        assert worker._multi_strategy_enabled is True
        assert [name for name, _strategy in worker._multi_strategy_components] == ["momentum", "g1"]
        component_map = dict(worker._multi_strategy_components)
        assert component_map["momentum"].name == expected_runtime_name
        assert created_params[expected_runtime_name][alias_marker_key] == alias_marker_value
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_default_intent_ttl_scales_with_poll_interval(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
        "g1": _NamedFakeStrategy(
            name="g1",
            side=Side.SELL,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        multi_strategy_enabled=None,
        poll_interval_sec=4.0,
        strategy_param_overrides={
            "multi_strategy_names": "momentum,g1",
        },
    )
    try:
        assert worker._multi_strategy_enabled is True
        assert worker._multi_strategy_intent_ttl_sec == pytest.approx(20.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_carrier_uses_base_component_namespace(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="multi_strategy",
        mode=RunMode.EXECUTION,
        multi_strategy_enabled=None,
        strategy_param_overrides={
            "_multi_strategy_base_component": "momentum",
            "multi_strategy_names": "momentum,g1",
            "momentum_execution_min_confidence_for_entry": 0.73,
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
        },
    )
    try:
        assert worker.strategy_name == "multi_strategy"
        assert worker._multi_strategy_carrier_enabled is True
        assert worker._base_strategy_label() == "momentum"
        assert [name for name, _strategy in worker._multi_strategy_components] == ["momentum", "g1"]
        assert worker.min_confidence_for_entry == pytest.approx(0.73)
        worker._save_state(last_price=1.1000, last_error=None)
        persisted = worker.store.load_worker_state("EURUSD") or {}
        assert str(persisted.get("strategy") or "") == "multi_strategy"
        assert str(persisted.get("strategy_base") or "") == "momentum"
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_state_persists_active_position_component_for_multi_strategy_carrier(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="multi_strategy",
        mode=RunMode.PAPER,
        multi_strategy_enabled=None,
        strategy_param_overrides={
            "_multi_strategy_base_component": "momentum",
            "multi_strategy_names": "momentum,g1",
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
        },
    )
    try:
        position = Position(
            position_id="paper-multi-open",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0950,
            take_profit=1.1100,
            opened_at=time.time(),
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(
            position,
            worker.name,
            "multi_strategy",
            worker.mode.value,
            strategy_entry="momentum",
            strategy_entry_component="g1",
            strategy_entry_signal="ema_cross_up",
        )

        worker._save_state(last_price=1.1010, last_error=None)
        persisted = worker.store.load_worker_state("EURUSD") or {}

        assert str(persisted.get("strategy") or "") == "multi_strategy"
        assert str(persisted.get("strategy_base") or "") == "momentum"
        assert str(persisted.get("position_strategy_entry") or "") == "momentum"
        assert str(persisted.get("position_strategy_component") or "") == "g1"
        assert str(persisted.get("position_strategy_signal") or "") == "ema_cross_up"
        position_payload = persisted.get("position") or {}
        assert str(position_payload.get("side") or "") == "buy"
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_synthetic_exit_forces_virtual_flat(tmp_path, monkeypatch: pytest.MonkeyPatch):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=5.0,
            take_profit_pips=15.0,
        ),
        "g1": _NamedFakeStrategy(
            name="g1",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=5.0,
            take_profit_pips=15.0,
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g1",
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
        },
    )
    try:
        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)
        first = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.0,
        )
        assert first.side == Side.BUY

        # 5 pips SL for BUY at 1.1000 with EURUSD pip=0.0001 => stop around 1.0995.
        worker.prices.append(1.0994)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)
        second = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.0,
        )

        assert second.side == Side.HOLD
        components = second.metadata.get("multi_components") or {}
        assert str((components.get("momentum") or {}).get("intent_reason") or "") == "synthetic_exit_stop_loss"
        assert str((components.get("g1") or {}).get("intent_reason") or "") == "synthetic_exit_stop_loss"
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_emergency_logs_critical_event_and_flags_exit(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
        "g1": _NamedFakeStrategy(
            name="g1",
            side=Side.SELL,
            confidence=1.0,
            stop_loss_pips=12.0,
            take_profit_pips=24.0,
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g1",
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
            "multi_strategy_emergency_event_cooldown_sec": 0.0,
        },
    )
    try:
        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)
        worker._last_stream_health = StreamHealthStatus(
            healthy=False,
            connected=False,
            reason="stream_disconnected",
            symbol="EURUSD",
            last_tick_age_sec=30.0,
        )

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.6,
        )

        assert signal.side == Side.HOLD
        assert signal.metadata.get("reason") == "multi_emergency_hold"
        assert worker._multi_strategy_emergency_exit_reason(signal) == "multi_strategy_emergency_mode"

        emergency_events = [
            event
            for event in worker.store.load_events(limit=40)
            if event.get("message") == "Multi-strategy emergency mode active"
        ]
        assert emergency_events
        payload = emergency_events[0].get("payload") or {}
        assert payload.get("manual_intervention_required") is True
        assert str((payload.get("health") or {}).get("stream_reason") or "") == "stream_disconnected"

        worker._save_state(last_price=1.1000, last_error=None)
        persisted = worker.store.load_worker_state("EURUSD") or {}
        assert str(persisted.get("last_error") or "").startswith("multi_strategy_emergency_mode")
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_emergency_marker_clears_after_recovery(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
        "g1": _NamedFakeStrategy(
            name="g1",
            side=Side.SELL,
            confidence=1.0,
            stop_loss_pips=12.0,
            take_profit_pips=24.0,
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g1",
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
            "multi_strategy_emergency_event_cooldown_sec": 0.0,
        },
    )
    try:
        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)

        worker._last_stream_health = StreamHealthStatus(
            healthy=False,
            connected=False,
            reason="stream_disconnected",
            symbol="EURUSD",
            last_tick_age_sec=30.0,
        )
        _ = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.6,
        )
        worker._save_state(last_price=1.1000, last_error=None)
        emergency_state = worker.store.load_worker_state("EURUSD") or {}
        assert str(emergency_state.get("last_error") or "").startswith("multi_strategy_emergency_mode")

        worker._last_stream_health = StreamHealthStatus(
            healthy=True,
            connected=True,
            reason="ok",
            symbol="EURUSD",
            last_tick_age_sec=0.1,
        )
        _ = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.6,
        )
        worker._save_state(last_price=1.1000, last_error=None)
        recovered_state = worker.store.load_worker_state("EURUSD") or {}
        assert "multi_strategy_emergency_mode" not in str(recovered_state.get("last_error") or "")
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_stale_market_data_degrades_without_emergency_when_stream_healthy(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
        "g1": _NamedFakeStrategy(
            name="g1",
            side=Side.SELL,
            confidence=1.0,
            stop_loss_pips=12.0,
            take_profit_pips=24.0,
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g1",
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
            "multi_strategy_emergency_event_cooldown_sec": 0.0,
        },
    )
    try:
        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time() - 45.0)
        worker.volumes.append(0.0)
        worker._last_stream_health = StreamHealthStatus(
            healthy=True,
            connected=True,
            reason="ok",
            symbol="EURUSD",
            last_tick_age_sec=45.0,
        )

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.6,
        )

        assert signal.side == Side.HOLD
        assert signal.metadata.get("reason") == "multi_degraded_hold"
        assert worker._multi_strategy_emergency_exit_reason(signal) is None

        health = signal.metadata.get("multi_runtime_health") or {}
        assert health.get("market_data_fresh") is False
        assert health.get("heartbeat_ok") is True

        emergency_events = [
            event
            for event in worker.store.load_events(limit=40)
            if event.get("message") == "Multi-strategy emergency mode active"
        ]
        assert not emergency_events
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_high_latency_connectivity_does_not_trigger_emergency(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
        "g1": _NamedFakeStrategy(
            name="g1",
            side=Side.SELL,
            confidence=1.0,
            stop_loss_pips=12.0,
            take_profit_pips=24.0,
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g1",
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
            "multi_strategy_emergency_event_cooldown_sec": 0.0,
        },
    )
    try:
        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)
        worker._last_stream_health = StreamHealthStatus(
            healthy=True,
            connected=True,
            reason="ok",
            symbol="EURUSD",
            last_tick_age_sec=0.2,
        )
        worker._cached_connectivity_status = ConnectivityStatus(
            healthy=False,
            reason="latency_too_high:900.0ms>350.0ms",
            latency_ms=900.0,
            pong_ok=True,
        )
        worker._next_connectivity_probe_ts = time.time() + 60.0

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.6,
        )

        health = signal.metadata.get("multi_runtime_health") or {}
        assert health.get("heartbeat_ok") is True
        assert health.get("connectivity_healthy") is False
        assert health.get("connectivity_effective_healthy") is True
        assert worker._multi_strategy_emergency_exit_reason(signal) is None
        assert signal.metadata.get("reason") != "multi_emergency_hold"

        emergency_events = [
            event
            for event in worker.store.load_events(limit=40)
            if event.get("message") == "Multi-strategy emergency mode active"
        ]
        assert not emergency_events
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_dispatch_timeout_connectivity_does_not_trigger_emergency(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
        "g1": _NamedFakeStrategy(
            name="g1",
            side=Side.SELL,
            confidence=1.0,
            stop_loss_pips=12.0,
            take_profit_pips=24.0,
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g1",
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
            "multi_strategy_emergency_event_cooldown_sec": 0.0,
        },
    )
    try:
        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)
        worker._last_stream_health = StreamHealthStatus(
            healthy=True,
            connected=True,
            reason="ok",
            symbol="EURUSD",
            last_tick_age_sec=0.2,
        )
        worker._cached_connectivity_status = ConnectivityStatus(
            healthy=False,
            reason=(
                "connectivity_check_failed:IG request dispatch timeout "
                "(worker=app_non_trading, method=GET, path=/session)"
            ),
            latency_ms=None,
            pong_ok=False,
        )
        worker._next_connectivity_probe_ts = time.time() + 60.0

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.6,
        )

        health = signal.metadata.get("multi_runtime_health") or {}
        assert health.get("heartbeat_ok") is True
        assert health.get("connectivity_healthy") is False
        assert health.get("connectivity_effective_healthy") is True
        assert worker._multi_strategy_emergency_exit_reason(signal) is None
        assert signal.metadata.get("reason") != "multi_emergency_hold"

        emergency_events = [
            event
            for event in worker.store.load_events(limit=40)
            if event.get("message") == "Multi-strategy emergency mode active"
        ]
        assert not emergency_events
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_multi_strategy_refreshes_connectivity_health_without_open_checks(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    strategies = {
        "momentum": _NamedFakeStrategy(
            name="momentum",
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
        ),
        "g1": _NamedFakeStrategy(
            name="g1",
            side=Side.SELL,
            confidence=1.0,
            stop_loss_pips=12.0,
            take_profit_pips=24.0,
        ),
    }

    def _factory(name: str, _params: dict[str, object]):
        return strategies[name]

    monkeypatch.setattr("xtb_bot.worker.create_strategy", _factory)
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g1",
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
            "multi_strategy_emergency_event_cooldown_sec": 0.0,
        },
    )
    try:
        worker.prices.append(1.1000)
        worker.price_timestamps.append(time.time())
        worker.volumes.append(0.0)
        worker._last_stream_health = StreamHealthStatus(
            healthy=True,
            connected=True,
            reason="ok",
            symbol="EURUSD",
            last_tick_age_sec=0.2,
        )
        worker._cached_connectivity_status = ConnectivityStatus(
            healthy=False,
            reason="connectivity_check_failed:timeout",
            latency_ms=None,
            pong_ok=False,
        )
        worker._next_connectivity_probe_ts = 0.0

        calls = {"count": 0}

        def _fake_connectivity(max_latency_ms, pong_timeout_sec):
            _ = (max_latency_ms, pong_timeout_sec)
            calls["count"] += 1
            return ConnectivityStatus(
                healthy=True,
                reason="ok",
                latency_ms=120.0,
                pong_ok=True,
            )

        worker.broker.get_connectivity_status = _fake_connectivity  # type: ignore[assignment]

        signal = worker._generate_signal(
            context=worker._build_strategy_context(
                current_volume=None,
                current_spread_pips=0.2,
            ),
            real_position_lots=0.6,
        )

        health = signal.metadata.get("multi_runtime_health") or {}
        assert calls["count"] == 1
        assert health.get("connectivity_healthy") is True
        assert health.get("connectivity_effective_healthy") is True
        assert health.get("connectivity_reason") == "ok"
        assert health.get("heartbeat_ok") is True
        assert worker._multi_strategy_emergency_exit_reason(signal) is None

        emergency_events = [
            event
            for event in worker.store.load_events(limit=40)
            if event.get("message") == "Multi-strategy emergency mode active"
        ]
        assert not emergency_events
    finally:
        worker.broker.close()
        worker.store.close()


def test_build_sl_tp_enforces_min_tp_sl_ratio_and_min_stop(tmp_path):
    worker = _make_worker(tmp_path, min_stop_loss_pips=10.0, min_tp_sl_ratio=2.0)
    try:
        sl, tp = worker._build_sl_tp(side=Side.BUY, entry=1.1000, stop_pips=5.0, take_pips=6.0)

        # Effective SL must be at least 10 pips.
        assert sl == pytest.approx(1.0990)
        # Effective TP must be at least 2x SL distance => 20 pips.
        assert tp == pytest.approx(1.1020)
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_pip_size_fallback_uses_one_point_for_index_and_energy_symbols(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.symbol = "DE40"
        assert worker._pip_size_fallback() == pytest.approx(1.0)
        worker.symbol = "US30"
        assert worker._pip_size_fallback() == pytest.approx(1.0)
        worker.symbol = "AUS200"
        assert worker._pip_size_fallback() == pytest.approx(1.0)
        worker.symbol = "JPN225"
        assert worker._pip_size_fallback() == pytest.approx(1.0)
        worker.symbol = "WTI"
        assert worker._pip_size_fallback() == pytest.approx(1.0)
        worker.symbol = "BRENT"
        assert worker._pip_size_fallback() == pytest.approx(1.0)
        worker.symbol = "GOLD"
        assert worker._pip_size_fallback() == pytest.approx(0.1)
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_pip_size_fallback_prefers_db_cached_value(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.symbol = "US100"
        worker.symbol_spec = None
        worker.store.upsert_broker_symbol_pip_size(
            symbol="US100",
            pip_size=0.1,
            source="test",
        )
        assert worker._pip_size_fallback() == pytest.approx(0.1)
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_context_pip_size_normalizes_fx_tick_cache_value(tmp_path):
    seed_store = StateStore(tmp_path / "worker.db")
    try:
        seed_store.upsert_broker_symbol_pip_size(
            symbol="EURUSD",
            pip_size=0.00001,
            source="test_tick_step",
        )
    finally:
        seed_store.close()

    worker = _make_worker(tmp_path)
    try:
        assert worker._context_pip_size() == pytest.approx(0.0001)
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_context_pip_size_normalizes_fx_symbol_spec_tick_size(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker._symbol_pip_size = None
        worker.symbol_spec = SymbolSpec(
            symbol="EURUSD",
            tick_size=0.00001,
            tick_value=10.0,
            contract_size=100000.0,
            lot_min=0.01,
            lot_max=100.0,
            lot_step=0.01,
        )
        assert worker._context_pip_size() == pytest.approx(0.0001)
    finally:
        worker.broker.close()
        worker.store.close()


def test_execution_open_persists_pending_open_and_clears_it_after_trade_insert(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)

    class _FakeExecBroker:
        def open_position(self, **kwargs):
            _ = kwargs
            return "VH39DRAH"

        def close(self):
            return None

    try:
        worker.broker.close()
        worker.broker = _FakeExecBroker()  # type: ignore[assignment]
        worker._open_position(
            side=Side.BUY,
            entry=1.3342,
            stop_loss=1.3300,
            take_profit=1.3400,
            volume=0.2,
            confidence=0.73,
            trailing_override={
                "trailing_activation_ratio": 0.5,
                "trailing_distance_pips": 12.0,
            },
        )

        assert worker.store.load_pending_opens(mode="execution") == []
        record = worker.store.get_trade_record("VH39DRAH")
        assert record is not None
        assert float(record.get("entry_confidence") or 0.0) == pytest.approx(0.73)
        assert str(record.get("deal_reference") or "") != ""
    finally:
        worker.store.close()


def test_execution_open_keeps_pending_open_on_confirm_timeout_pending_recovery(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)

    class _FakeExecBroker:
        def open_position(self, **kwargs):
            _ = kwargs
            raise BrokerError(
                "IG open position confirm timed out before dealId was available; "
                "pending recovery required | deal_reference=XTBBOTTIMEOUT001 epic=CC.D.CL.UNC.IP "
                "direction=BUY size=0.2 currency=USD stop=1.33 limit=1.34"
            )

        def close(self):
            return None

    try:
        worker.broker.close()
        worker.broker = _FakeExecBroker()  # type: ignore[assignment]

        with pytest.raises(BrokerError, match="confirm timed out before dealId was available"):
            worker._open_position(
                side=Side.BUY,
                entry=1.3342,
                stop_loss=1.3300,
                take_profit=1.3400,
                volume=0.2,
                confidence=0.73,
            )

        pending = worker.store.load_pending_opens(mode="execution")
        assert len(pending) == 1
        assert pending[0].position_id is None
        assert pending[0].symbol == worker.symbol

        events = worker.store.load_events(limit=20)
        retained = [event for event in events if event.get("message") == "Pending open retained for broker recovery"]
        assert retained
    finally:
        worker.store.close()


def test_execution_open_persists_entry_signal_for_multi_strategy_component(tmp_path):
    worker = _make_worker(
        tmp_path,
        mode=RunMode.EXECUTION,
        strategy_param_overrides={
            "multi_strategy_enabled": True,
            "multi_strategy_names": "momentum,g1",
        },
    )

    class _FakeExecBroker:
        def open_position(self, **kwargs):
            _ = kwargs
            return "VHSIGNAL01"

        def close(self):
            return None

    try:
        worker.broker.close()
        worker.broker = _FakeExecBroker()  # type: ignore[assignment]
        signal = Signal(
            side=Side.BUY,
            confidence=0.73,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "indicator": "multi_strategy_netting",
                "multi_entry_strategy": "momentum",
                "multi_entry_strategy_component": "g1",
                "multi_entry_strategy_signal": "ema_cross_up",
            },
        )
        worker._open_position(
            side=Side.BUY,
            entry=1.3342,
            stop_loss=1.3300,
            take_profit=1.3400,
            volume=0.2,
            confidence=0.73,
            entry_strategy="momentum",
            entry_strategy_component="g1",
            signal=signal,
        )

        record = worker.store.get_trade_record("VHSIGNAL01")
        assert record is not None
        assert str(record.get("strategy_entry") or "") == "momentum"
        assert str(record.get("strategy_entry_component") or "") == "g1"
        assert str(record.get("strategy_entry_signal") or "") == "ema_cross_up"
    finally:
        worker.store.close()


def test_execution_open_persists_pending_open_entry_metadata_for_multi_strategy_carrier(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    worker = _make_worker(
        tmp_path,
        strategy_name="multi_strategy",
        mode=RunMode.EXECUTION,
        multi_strategy_enabled=None,
        strategy_param_overrides={
            "_multi_strategy_base_component": "momentum",
            "multi_strategy_names": "momentum,g1",
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
        },
    )

    class _FakeExecBroker:
        def open_position(self, **kwargs):
            _ = kwargs
            return "VHCARRIER01"

        def close(self):
            return None

    captured_pending: dict[str, PendingOpen] = {}
    original_upsert_pending_open = worker.store.upsert_pending_open

    def _capture_pending_open(pending: PendingOpen) -> None:
        captured_pending["value"] = pending
        original_upsert_pending_open(pending)

    try:
        worker.broker.close()
        worker.broker = _FakeExecBroker()  # type: ignore[assignment]
        monkeypatch.setattr(worker.store, "upsert_pending_open", _capture_pending_open)
        signal = Signal(
            side=Side.BUY,
            confidence=0.74,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "indicator": "multi_strategy_netting",
                "multi_entry_strategy": "momentum",
                "multi_entry_strategy_component": "g1",
                "multi_entry_strategy_signal": "ema_cross_up",
            },
        )

        worker._open_position(
            side=Side.BUY,
            entry=1.3342,
            stop_loss=1.3300,
            take_profit=1.3400,
            volume=0.2,
            confidence=0.74,
            entry_strategy="momentum",
            entry_strategy_component="g1",
            signal=signal,
        )

        pending = captured_pending["value"]
        assert pending.strategy == "multi_strategy"
        assert pending.strategy_entry == "momentum"
        assert pending.strategy_entry_component == "g1"
        assert pending.strategy_entry_signal == "ema_cross_up"
    finally:
        worker.store.close()


def test_same_side_reentry_cooldown_blocks_repeat_entry_after_win(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        strategy_param_overrides={
            "momentum_same_side_reentry_win_cooldown_sec": 300.0,
            "momentum_same_side_reentry_reset_on_opposite_signal": True,
        },
    )
    try:
        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])

        position = Position(
            position_id="paper-win-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=now["ts"] - 60.0,
            status="open",
        )

        worker._finalize_position_close(position, close_price=1.1015, reason="take_profit")

        assert worker._same_side_reentry_block_side == Side.BUY
        assert worker._same_side_reentry_allows_open(Side.BUY) is False
        assert worker._same_side_reentry_allows_open(Side.SELL) is True
        assert worker._same_side_reentry_block_side is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_profitable_close_arms_continuation_reentry_guard_without_time_cooldown(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        strategy_param_overrides={
            "momentum_same_side_reentry_win_cooldown_sec": 0.0,
            "momentum_trade_cooldown_win_sec": 0.0,
            "momentum_continuation_reentry_guard_enabled": True,
            "momentum_continuation_reentry_reset_on_opposite_signal": True,
        },
    )
    try:
        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])

        position = Position(
            position_id="paper-win-2",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=now["ts"] - 60.0,
            status="open",
        )

        worker._finalize_position_close(position, close_price=1.1015, reason="profit_lock_peak_drawdown:price_retrace")

        assert worker._same_side_reentry_block_side is None
        assert worker._continuation_reentry_block_side == Side.BUY
        continuation_signal = Signal(
            side=Side.BUY,
            confidence=0.9,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"trend_signal": "ma_trend_up"},
        )
        cross_signal = Signal(
            side=Side.BUY,
            confidence=0.9,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"trend_signal": "ma_cross_up"},
        )
        worker.price_timestamps.extend(
            [
                now["ts"] + 30.0,
                now["ts"] + 60.0,
                now["ts"] + 90.0,
            ]
        )
        worker.prices.extend([1.0996, 1.0994, 1.1008])

        assert worker._cooldown_allows_open(Side.BUY, continuation_signal) is False
        assert worker._cooldown_allows_open(Side.BUY, cross_signal) is True
        assert worker._continuation_reentry_block_side is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_entry_signal_persistence_uses_stricter_index_trend_requirements(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        symbol="US500",
        strategy_param_overrides={
            "entry_signal_persistence_enabled": True,
            "entry_signal_persistence_trend_only": True,
            "entry_signal_min_persistence_sec": 2.0,
            "entry_signal_min_consecutive_evals": 2,
            "entry_signal_index_trend_min_persistence_sec": 8.0,
            "entry_signal_index_trend_min_consecutive_evals": 3,
        },
    )
    try:
        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])
        signal = Signal(
            side=Side.BUY,
            confidence=0.9,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "indicator": "index_hybrid",
                "regime": "trend_following",
                "trend": "up",
            },
        )

        assert worker._entry_signal_persistence_allows_open(signal) is False
        assert worker._entry_signal_persistence_allows_open(signal) is False
        now["ts"] += 2.1
        assert worker._entry_signal_persistence_allows_open(signal) is False

        now["ts"] += 6.1
        assert worker._entry_signal_persistence_allows_open(signal) is True
    finally:
        worker.broker.close()
        worker.store.close()


def test_entry_signal_persistence_relaxes_index_trend_requirements_near_session_open(
    tmp_path,
    monkeypatch,
):
    worker = _make_worker(
        tmp_path,
        symbol="US500",
        strategy_param_overrides={
            "entry_signal_persistence_enabled": True,
            "entry_signal_persistence_trend_only": True,
            "entry_signal_min_persistence_sec": 2.0,
            "entry_signal_min_consecutive_evals": 2,
            "entry_signal_index_trend_min_persistence_sec": 8.0,
            "entry_signal_index_trend_min_consecutive_evals": 3,
        },
    )
    try:
        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])
        signal = Signal(
            side=Side.BUY,
            confidence=0.9,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "indicator": "index_hybrid",
                "regime": "trend_following",
                "trend": "up",
                "minutes_since_trend_session_open": 23.0,
                "index_trend_session_open_delay_minutes": 15.0,
            },
        )

        assert worker._entry_signal_persistence_allows_open(signal) is False

        events = [
            event
            for event in worker.store.load_events()
            if event["message"] == "Entry waiting for signal persistence"
        ]
        assert len(events) == 1
        payload = events[0]["payload"]
        assert payload["required_count"] == 2
        assert payload["required_persistence_sec"] == pytest.approx(4.0)
        assert payload["session_open_persistence_relief_active"] is True
        assert payload["minutes_since_trend_session_open"] == pytest.approx(23.0)

        now["ts"] += 4.1
        assert worker._entry_signal_persistence_allows_open(signal) is True
    finally:
        worker.broker.close()
        worker.store.close()


def test_pending_open_guard_blocks_new_entry_for_same_symbol(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        mode=RunMode.EXECUTION,
        symbol="US500",
        strategy_param_overrides={
            "entry_pending_open_block_window_sec": 45.0,
        },
    )
    try:
        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])
        worker.store.upsert_pending_open(
            PendingOpen(
                pending_id="PENDING-US500-1",
                symbol="US500",
                side=Side.BUY,
                volume=1.0,
                entry=6500.0,
                stop_loss=6480.0,
                take_profit=6550.0,
                created_at=now["ts"],
                thread_name=worker.name,
                strategy=worker.strategy_name,
                mode=worker.mode.value,
            )
        )

        assert worker._pending_open_conflicts_with_new_entry() is True

        now["ts"] += 60.0
        assert worker._pending_open_conflicts_with_new_entry() is False
    finally:
        worker.broker.close()
        worker.store.close()


def test_micro_chop_close_activates_extended_entry_cooldown(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        symbol="US500",
        strategy_param_overrides={
            "momentum_trade_cooldown_sec": 0.0,
            "micro_chop_cooldown_sec": 480.0,
            "micro_chop_trade_max_age_sec": 90.0,
            "micro_chop_max_favorable_pips": 1.0,
            "micro_chop_max_tp_progress": 0.08,
        },
    )
    try:
        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])
        position = Position(
            position_id="micro-chop-1",
            symbol="US500",
            side=Side.BUY,
            volume=2.0,
            open_price=6500.0,
            stop_loss=6490.0,
            take_profit=6520.0,
            opened_at=now["ts"] - 25.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(
            position,
            worker.name,
            worker.strategy_name,
            worker.mode.value,
            strategy_entry="momentum",
            strategy_entry_component="index_hybrid",
        )
        worker._position_peak_favorable_pips[position.position_id] = 0.0
        worker._position_peak_adverse_pips[position.position_id] = 0.5

        worker._finalize_position_close(
            position,
            close_price=6499.5,
            reason="strategy_exit:protective:trend_reversal",
        )

        assert worker._active_entry_cooldown_outcome == "micro_chop"
        assert worker._active_entry_cooldown_sec == pytest.approx(480.0)
        assert worker._next_entry_allowed_ts == pytest.approx(now["ts"] + 480.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_multi_strategy_protective_close_persists_component_exit_attribution(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        strategy_name="multi_strategy",
        mode=RunMode.PAPER,
        multi_strategy_enabled=None,
        strategy_param_overrides={
            "_multi_strategy_base_component": "momentum",
            "multi_strategy_names": "momentum,g1",
            "multi_strategy_normalizer_min_samples": 1,
            "multi_strategy_normalizer_default": 1.0,
            "multi_strategy_lot_step_lots": 0.1,
            "multi_strategy_min_open_lot": 0.1,
            "multi_strategy_deadband_lots": 0.0,
        },
    )
    try:
        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])
        position = Position(
            position_id="multi-protective-close-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0950,
            take_profit=1.1100,
            opened_at=now["ts"] - 25.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(
            position,
            worker.name,
            worker.strategy_name,
            worker.mode.value,
            strategy_entry="momentum",
            strategy_entry_component="g1",
            strategy_entry_signal="ema_cross_up",
        )

        worker._finalize_position_close(
            position,
            close_price=1.0990,
            reason="strategy_exit:protective:trend_reversal",
        )

        trade_record = worker.store.get_trade_record(position.position_id)
        assert trade_record is not None
        assert str(trade_record.get("strategy_exit") or "") == "g1"
        assert str(trade_record.get("strategy_exit_component") or "") == "g1"
        assert str(trade_record.get("strategy_exit_signal") or "") == "protective:trend_reversal"

        close_events = [
            event
            for event in worker.store.load_events(limit=20)
            if event.get("message") == "Position closed"
        ]
        assert close_events
        payload = close_events[0].get("payload") or {}
        assert payload.get("strategy_entry") == "momentum"
        assert payload.get("strategy_entry_component") == "g1"
        assert payload.get("strategy_base") == "momentum"
        assert payload.get("strategy_exit") == "g1"
        assert payload.get("strategy_exit_component") == "g1"
        assert payload.get("strategy_exit_signal") == "protective:trend_reversal"
    finally:
        worker.broker.close()
        worker.store.close()


def test_index_trend_structure_blocks_breakout_inside_spread_noise(tmp_path):
    worker = _make_worker(
        tmp_path,
        symbol="US500",
        strategy_param_overrides={
            "entry_index_trend_min_breakout_to_spread_ratio": 2.0,
            "entry_index_trend_max_entry_quality_penalty": 0.12,
        },
    )
    try:
        signal = Signal(
            side=Side.BUY,
            confidence=0.9,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "indicator": "index_hybrid",
                "regime": "trend_following",
                "trend": "up",
                "breakout_distance_pips": 0.5,
                "breakout_distance_ratio": 0.00008,
                "entry_quality_penalty": 0.05,
            },
        )

        assert worker._index_trend_structure_allows_open(signal, current_spread_pips=0.4) is False
    finally:
        worker.broker.close()
        worker.store.close()


def test_index_trend_reversal_grace_blocks_micro_exit(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        symbol="US500",
        strategy_param_overrides={
            "protective_exit_on_trend_reversal": True,
            "protective_index_trend_reversal_grace_sec": 12.0,
            "protective_index_trend_reversal_grace_max_adverse_spread_ratio": 2.0,
            "protective_exit_require_armed_profit": False,
        },
    )
    try:
        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])
        position = Position(
            position_id="index-hybrid-grace-1",
            symbol="US500",
            side=Side.BUY,
            volume=2.0,
            open_price=6500.0,
            stop_loss=6490.0,
            take_profit=6520.0,
            opened_at=now["ts"] - 4.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(
            position,
            worker.name,
            worker.strategy_name,
            worker.mode.value,
            strategy_entry="momentum",
            strategy_entry_component="index_hybrid",
        )

        reason = worker._generic_protective_dynamic_exit_reason(
            position,
            bid=6499.6,
            ask=6500.0,
            metadata={
                "indicator": "index_hybrid",
                "regime": "trend_following",
                "fast_ma": 6499.5,
                "slow_ma": 6499.8,
                "trend": "down",
            },
        )

        assert reason is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_global_fresh_reversal_grace_blocks_micro_trend_reversal_exit(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "momentum_protective_exit_enabled": True,
            "momentum_protective_exit_on_trend_reversal": True,
            "momentum_protective_exit_require_armed_profit": False,
            "protective_fresh_reversal_grace_sec": 12.0,
            "protective_fresh_reversal_grace_max_adverse_spread_ratio": 2.5,
            "protective_fresh_reversal_grace_max_adverse_stop_ratio": 0.12,
        },
    )
    try:
        now = {"ts": 1_700_100_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "ma_cross",
                    "reason": "no_ma_cross",
                    "fast_ma": 1.0990,
                    "slow_ma": 1.1000,
                },
            )
        )
        worker.prices.append(1.1000)
        worker.spreads_pips.append(0.8)
        position = Position(
            position_id="global-reversal-grace-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1030,
            opened_at=now["ts"] - 1.0,
            status="open",
        )

        signal = worker._strategy_exit_signal()
        reason = worker._strategy_dynamic_exit_reason(position, bid=1.0999, ask=1.1001, signal=signal)
        assert reason is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_continuation_reentry_guard_blocks_repeat_continuation_same_leg(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_param_overrides={
            "momentum_continuation_reentry_guard_enabled": True,
            "momentum_continuation_reentry_reset_on_opposite_signal": True,
        },
    )
    try:
        continuation_signal = Signal(
            side=Side.BUY,
            confidence=0.8,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"indicator": "ma_cross", "trend_signal": "ma_trend_up"},
        )

        assert worker._cooldown_allows_open(Side.BUY, signal=continuation_signal) is True

        worker._register_continuation_reentry_on_open(continuation_signal)
        assert worker._continuation_reentry_block_side == Side.BUY

        assert worker._cooldown_allows_open(Side.BUY, signal=continuation_signal) is False
    finally:
        worker.broker.close()
        worker.store.close()


def test_continuation_reentry_guard_resets_on_cross_signal(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_param_overrides={
            "momentum_continuation_reentry_guard_enabled": True,
            "momentum_continuation_reentry_reset_on_opposite_signal": True,
        },
    )
    try:
        continuation_signal = Signal(
            side=Side.BUY,
            confidence=0.8,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"indicator": "ma_cross", "trend_signal": "ma_trend_up"},
        )
        cross_signal = Signal(
            side=Side.BUY,
            confidence=0.8,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"indicator": "ma_cross", "trend_signal": "ma_cross_up"},
        )

        worker._register_continuation_reentry_on_open(continuation_signal)
        assert worker._cooldown_allows_open(Side.BUY, signal=continuation_signal) is False

        assert worker._cooldown_allows_open(Side.BUY, signal=cross_signal) is True
        assert worker._continuation_reentry_block_side is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_post_win_reentry_reset_guard_blocks_same_side_cross_without_reset(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        strategy_param_overrides={
            "momentum_same_side_reentry_win_cooldown_sec": 0.0,
            "momentum_trade_cooldown_win_sec": 0.0,
            "momentum_continuation_reentry_guard_enabled": True,
            "momentum_continuation_reentry_reset_on_opposite_signal": True,
            "continuation_reentry_post_win_reset_guard_enabled": True,
            "continuation_reentry_post_win_min_reset_stop_ratio": 0.25,
            "continuation_reentry_post_win_max_age_sec": 1200.0,
        },
    )
    try:
        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])

        position = Position(
            position_id="paper-win-reset-block",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=now["ts"] - 120.0,
            status="open",
        )
        worker._finalize_position_close(position, close_price=1.1015, reason="profit_lock_peak_drawdown:price_retrace")

        worker.price_timestamps.extend(
            [
                now["ts"] + 30.0,
                now["ts"] + 60.0,
                now["ts"] + 90.0,
            ]
        )
        worker.prices.extend([1.1014, 1.10135, 1.1018])
        now["ts"] += 90.0

        cross_signal = Signal(
            side=Side.BUY,
            confidence=0.9,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"trend_signal": "ma_cross_up"},
        )

        assert worker._cooldown_allows_open(Side.BUY, cross_signal) is False
        assert worker._continuation_reentry_block_side == Side.BUY
    finally:
        worker.broker.close()
        worker.store.close()


def test_execution_open_applies_broker_open_sync_to_local_trade(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)

    class _FakeExecBroker:
        def open_position(self, **kwargs):
            _ = kwargs
            return "DIAAAASYNC123"

        def get_position_open_sync(self, position_id: str):
            assert position_id == "DIAAAASYNC123"
            return {
                "position_id": position_id,
                "source": "ig_confirm",
                "open_price": 10059.0,
                "stop_loss": 9534.0,
                "take_profit": 10584.0,
            }

        def close(self):
            return None

    try:
        worker.broker.close()
        worker.broker = _FakeExecBroker()  # type: ignore[assignment]
        worker.symbol = "WTI"
        worker._open_position(
            side=Side.BUY,
            entry=491.7,
            stop_loss=439.2,
            take_profit=544.2,
            volume=0.05,
            confidence=0.81,
        )

        record = worker.store.get_trade_record("DIAAAASYNC123")
        assert record is not None
        assert float(record.get("open_price") or 0.0) == pytest.approx(10059.0)
        assert float(record.get("stop_loss") or 0.0) == pytest.approx(9534.0)
        assert float(record.get("take_profit") or 0.0) == pytest.approx(10584.0)
        assert str(record.get("deal_reference") or "") != ""
    finally:
        worker.store.close()


def test_execution_open_sync_records_entry_fill_quality(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)

    class _FakeExecBroker:
        def open_position(self, **kwargs):
            _ = kwargs
            return "DIAAAAFILLQ001"

        def get_position_open_sync(self, position_id: str):
            assert position_id == "DIAAAAFILLQ001"
            return {
                "position_id": position_id,
                "source": "ig_confirm",
                "open_price": 1.1005,
                "stop_loss": 1.0950,
                "take_profit": 1.1100,
            }

        def close(self):
            return None

    try:
        worker.broker.close()
        worker.broker = _FakeExecBroker()  # type: ignore[assignment]
        worker._open_position(
            side=Side.BUY,
            entry=1.1000,
            stop_loss=1.0950,
            take_profit=1.1100,
            volume=0.10,
            confidence=0.81,
        )

        performance = worker.store.load_trade_performance("DIAAAAFILLQ001")
        assert performance is not None
        assert float(performance["entry_reference_price"]) == pytest.approx(1.1000)
        assert float(performance["entry_fill_price"]) == pytest.approx(1.1005)
        assert float(performance["entry_slippage_pips"]) == pytest.approx(5.0)
        assert float(performance["entry_adverse_slippage_pips"]) == pytest.approx(5.0)
    finally:
        worker.store.close()


def test_execution_open_increases_mean_breakout_buffer_on_high_entry_slippage(tmp_path):
    worker = _make_worker(
        tmp_path,
        mode=RunMode.EXECUTION,
        strategy_name="mean_breakout_v2",
        strategy_param_overrides={
            "mb_breakout_min_buffer_pips": 0.4,
        },
    )

    class _FakeExecBroker:
        def open_position(self, **kwargs):
            _ = kwargs
            return "DIAAAAMB001"

        def get_position_open_sync(self, position_id: str):
            assert position_id == "DIAAAAMB001"
            return {
                "position_id": position_id,
                "source": "ig_confirm",
                "open_price": 1.1012,
                "stop_loss": 1.0950,
                "take_profit": 1.1100,
            }

        def close(self):
            return None

    try:
        worker.broker.close()
        worker.broker = _FakeExecBroker()  # type: ignore[assignment]
        worker._open_position(
            side=Side.BUY,
            entry=1.1000,
            stop_loss=1.0950,
            take_profit=1.1100,
            volume=0.10,
            confidence=0.82,
            signal=Signal(
                side=Side.BUY,
                confidence=0.82,
                stop_loss_pips=20.0,
                take_profit_pips=40.0,
                metadata={
                    "indicator": "mean_breakout_v2",
                    "atr_pips": 10.0,
                },
            ),
        )

        assert float(getattr(worker.strategy, "breakout_min_buffer_pips", 0.0)) == pytest.approx(12.0)
        events = worker.store.load_events(limit=20)
        slippage_adjust_events = [
            event
            for event in events
            if event.get("message") == "Mean breakout buffer auto-increased after slippage"
        ]
        assert slippage_adjust_events
        payload = slippage_adjust_events[0].get("payload") or {}
        assert float(payload.get("entry_adverse_slippage_pips") or 0.0) == pytest.approx(12.0)
        assert float(payload.get("slippage_threshold_pips") or 0.0) == pytest.approx(5.0)
    finally:
        worker.store.close()


def test_execution_stop_slippage_marks_symbol_for_guaranteed_stop(tmp_path):
    worker = _make_worker(
        tmp_path,
        mode=RunMode.EXECUTION,
        strategy_param_overrides={
            "stop_slippage_auto_guaranteed_stop_enabled": True,
            "stop_slippage_auto_guaranteed_stop_ratio": 1.2,
        },
    )

    class _FakeExecBroker:
        def __init__(self):
            self.mark_calls: list[tuple[str, str | None]] = []

        def mark_symbol_guaranteed_stop_required(self, symbol: str, source: str | None = None):
            self.mark_calls.append((symbol, source))

        def close(self):
            return None

    try:
        fake_broker = _FakeExecBroker()
        worker.broker.close()
        worker.broker = fake_broker  # type: ignore[assignment]
        worker.symbol = "WTI"

        position = Position(
            position_id="DIAAAASLIP001",
            symbol="WTI",
            side=Side.SELL,
            volume=0.3,
            open_price=8716.2,
            stop_loss=8718.0,
            take_profit=8675.0,
            opened_at=time.time() - 60.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)

        # Adverse move is significantly larger than planned stop distance.
        worker._finalize_position_close(
            position,
            close_price=8737.0,
            reason="stop_loss",
            broker_sync={
                "position_id": position.position_id,
                "source": "ig_confirm",
                "close_price": 8737.0,
                "realized_pnl": -57.56,
                "closed_at": time.time(),
            },
        )

        assert fake_broker.mark_calls
        symbol, source = fake_broker.mark_calls[-1]
        assert symbol == "WTI"
        assert source is not None and source.startswith("runtime_stop_slippage_ratio_")
    finally:
        worker.store.close()


def test_finalize_position_close_records_exit_fill_quality(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    position = Position(
        position_id="DIAAAAEXITQ001",
        symbol="EURUSD",
        side=Side.BUY,
        volume=0.10,
        open_price=1.1000,
        stop_loss=1.0950,
        take_profit=1.1100,
        opened_at=1_800_000_000.0,
        entry_confidence=0.8,
        status="open",
        close_price=None,
        closed_at=None,
        pnl=0.0,
    )
    try:
        worker.position_book.upsert(position)
        worker.store.upsert_trade(
            position,
            worker.name,
            worker.strategy_name,
            worker.mode.value,
            strategy_entry="momentum",
        )
        worker._finalize_position_close(
            position,
            close_price=1.1050,
            reason="take_profit",
            broker_sync={
                "source": "ig_history_transactions",
                "close_price": 1.1046,
                "closed_at": 1_800_000_120.0,
                "realized_pnl": 46.0,
            },
            use_local_close_price=False,
        )

        performance = worker.store.load_trade_performance(position.position_id)
        assert performance is not None
        assert float(performance["exit_reference_price"]) == pytest.approx(1.1050)
        assert float(performance["exit_fill_price"]) == pytest.approx(1.1046)
        assert float(performance["exit_slippage_pips"]) == pytest.approx(4.0)
        assert float(performance["exit_adverse_slippage_pips"]) == pytest.approx(4.0)
    finally:
        worker.store.close()


def test_worker_state_persistence_is_throttled_for_unchanged_state(tmp_path):
    worker = _make_worker(tmp_path)
    saved_states = []

    try:
        worker.worker_state_flush_interval_sec = 5.0
        worker.store.save_worker_state = lambda state: saved_states.append(state)  # type: ignore[method-assign]

        with patch("xtb_bot.worker.time.time", side_effect=[100.0, 100.0, 101.0, 106.0, 106.0]), patch(
            "xtb_bot.worker.time.monotonic",
            side_effect=[100.0, 100.0, 101.0, 101.0, 106.0, 106.0],
        ):
            worker._save_state(last_price=1.1000, last_error=None)
            worker._save_state(last_price=1.1001, last_error=None)
            worker._save_state(last_price=1.1002, last_error=None)

        assert len(saved_states) == 2
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_state_force_persistence_bypasses_throttle(tmp_path):
    worker = _make_worker(tmp_path)
    saved_states = []

    try:
        worker.worker_state_flush_interval_sec = 60.0
        worker.store.save_worker_state = lambda state: saved_states.append(state)  # type: ignore[method-assign]

        with patch("xtb_bot.worker.time.time", side_effect=[100.0, 100.0, 101.0, 101.0]), patch(
            "xtb_bot.worker.time.monotonic",
            side_effect=[100.0, 100.0, 101.0, 101.0],
        ):
            worker._save_state(last_price=1.1000, last_error=None)
            worker._save_state(last_price=1.1000, last_error=None, force=True)

        assert len(saved_states) == 2
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_state_persistence_separates_wall_clock_heartbeat_from_monotonic_flush_ts(tmp_path):
    worker = _make_worker(tmp_path)
    saved_states = []

    try:
        worker.worker_state_flush_interval_sec = 60.0
        worker.store.save_worker_state = lambda state: saved_states.append(state)  # type: ignore[method-assign]

        with patch("xtb_bot.worker.time.time", return_value=1_777_777_777.0), patch(
            "xtb_bot.worker.time.monotonic",
            return_value=123.456,
        ):
            worker._save_state(last_price=1.1000, last_error=None)

        assert len(saved_states) == 1
        assert worker._last_saved_worker_state_ts == pytest.approx(1_777_777_777.0)
        assert worker._last_saved_worker_state_monotonic == pytest.approx(123.456)
    finally:
        worker.broker.close()
        worker.store.close()


def test_account_snapshot_persistence_is_throttled_between_intervals(tmp_path):
    worker = _make_worker(tmp_path)
    persisted = []
    snapshot = AccountSnapshot(balance=10_000, equity=10_010, margin_free=9_000, timestamp=1.0)

    try:
        worker.account_snapshot_persist_interval_sec = 10.0
        worker.store.record_account_snapshot = lambda **kwargs: persisted.append(kwargs)  # type: ignore[method-assign]

        with patch("xtb_bot.worker.time.time", side_effect=[100.0, 101.0, 111.0]):
            worker._persist_account_snapshot_if_due(
                snapshot,
                open_positions=1,
                daily_pnl=10.0,
                drawdown_pct=0.5,
            )
            worker._persist_account_snapshot_if_due(
                snapshot,
                open_positions=1,
                daily_pnl=10.0,
                drawdown_pct=0.5,
            )
            worker._persist_account_snapshot_if_due(
                snapshot,
                open_positions=1,
                daily_pnl=10.0,
                drawdown_pct=0.5,
            )

        assert len(persisted) == 2
    finally:
        worker.broker.close()
        worker.store.close()


def test_account_snapshot_cached_avoids_broker_call_during_allowance_backoff(tmp_path, monkeypatch):
    worker = _make_worker(tmp_path)
    snapshot = AccountSnapshot(balance=10_000, equity=10_020, margin_free=9_100, timestamp=1.0)
    try:
        worker._cached_account_snapshot = snapshot
        worker._cached_account_snapshot_ts = 100.0
        worker.account_snapshot_cache_ttl_sec = 0.1
        worker._allowance_backoff_until_ts = 230.0

        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: 200.0)

        def _fail_snapshot():
            raise AssertionError("broker.get_account_snapshot should not be called during allowance backoff")

        worker.broker.get_account_snapshot = _fail_snapshot  # type: ignore[assignment]

        result = worker._get_account_snapshot_cached()
        assert result.balance == pytest.approx(snapshot.balance)
        assert result.equity == pytest.approx(snapshot.equity)
        assert result.margin_free == pytest.approx(snapshot.margin_free)
        assert result.timestamp == pytest.approx(1.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_account_snapshot_cached_db_first_uses_direct_fallback_when_cache_is_stale(tmp_path, monkeypatch):
    worker = _make_worker(tmp_path, db_first_reads_enabled=True)
    stale_snapshot = AccountSnapshot(balance=10_000, equity=10_010, margin_free=9_000, timestamp=100.0)
    fresh_snapshot = AccountSnapshot(balance=10_200, equity=10_250, margin_free=9_400, timestamp=200.0)
    try:
        worker.db_first_account_snapshot_max_age_sec = 10.0
        worker.store.upsert_broker_account_snapshot(stale_snapshot, source="test_stale")
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: 200.0)
        worker.broker.get_account_snapshot = lambda: fresh_snapshot  # type: ignore[assignment]

        result = worker._get_account_snapshot_cached()

        assert result.balance == pytest.approx(10_200)
        assert result.equity == pytest.approx(10_250)
        assert result.margin_free == pytest.approx(9_400)
        assert worker._cached_account_snapshot is not None
        assert worker._cached_account_snapshot.balance == pytest.approx(10_200)
        events = [
            event
            for event in worker.store.load_events()
            if event["message"] == "DB-first account snapshot stale, direct broker fallback used"
        ]
        assert len(events) == 1
        assert events[0]["payload"]["stale_age_sec"] == pytest.approx(100.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_account_snapshot_cached_db_first_can_use_stale_snapshot_for_active_position(tmp_path, monkeypatch):
    worker = _make_worker(tmp_path, db_first_reads_enabled=True)
    stale_snapshot = AccountSnapshot(balance=10_000, equity=10_010, margin_free=9_000, timestamp=100.0)
    try:
        worker.db_first_account_snapshot_max_age_sec = 10.0
        worker.store.upsert_broker_account_snapshot(stale_snapshot, source="test_stale")
        worker.position_book.upsert(
            Position(
                position_id="pos-1",
                symbol=worker.symbol,
                side=Side.BUY,
                volume=1.0,
                open_price=1.1,
                stop_loss=1.0,
                take_profit=1.2,
                opened_at=100.0,
            )
        )
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: 200.0)

        def _fail_snapshot():
            raise RuntimeError("broker unavailable")

        worker.broker.get_account_snapshot = _fail_snapshot  # type: ignore[assignment]

        result = worker._get_account_snapshot_cached()

        assert result.balance == pytest.approx(10_000)
        assert result.equity == pytest.approx(10_010)
        assert result.margin_free == pytest.approx(9_000)
        assert result.timestamp == pytest.approx(100.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_parses_allowance_cooldown_remaining(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        assert worker._is_allowance_backoff_error(
            "IG public API allowance cooldown is active (12.4s remaining)"
        )
        assert worker._is_allowance_backoff_error(
            "IG non-critical REST request deferred: critical_trade_operation_active method=GET path=/markets (1.0s remaining)"
        )
        assert worker._is_allowance_backoff_error(
            'IG API POST /positions/otc failed: 403 Forbidden {"errorCode":"error.public-api.exceeded-account-trading-allowance"}'
        )
        assert worker._extract_allowance_backoff_remaining_sec(
            "IG public API allowance cooldown is active (12.4s remaining)"
        ) == pytest.approx(12.4)
        assert worker._allowance_backoff_kind(
            "IG non-critical REST request deferred: critical_trade_operation_active method=GET path=/markets (1.0s remaining)"
        ) == "critical_trade_deferred"
        assert worker._allowance_backoff_kind(
            'IG API GET /markets/X failed: 403 Forbidden {"errorCode":"error.public-api.exceeded-api-key-allowance"}'
        ) == "api_key_allowance_exceeded"
        assert worker._allowance_backoff_kind(
            'IG API POST /positions/otc failed: 403 Forbidden {"errorCode":"error.public-api.exceeded-account-trading-allowance"}'
        ) == "account_trading_allowance_exceeded"
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_records_allowance_backoff_as_warn_not_broker_error(tmp_path):
    worker = _make_worker(tmp_path, stream_event_cooldown_sec=60.0)
    try:
        remaining = worker._handle_allowance_backoff_error(
            "IG public API allowance cooldown is active (12.4s remaining)"
        )
        assert remaining == pytest.approx(12.4)
        events = worker.store.load_events(limit=10)
        allowance_events = [event for event in events if event["message"] == "Broker allowance backoff active"]
        broker_errors = [event for event in events if event["message"] == "Broker error"]
        assert allowance_events
        assert allowance_events[0]["level"] == "WARN"
        assert broker_errors == []
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_detects_db_first_symbol_spec_cache_miss_error(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        assert worker._is_db_first_symbol_spec_cache_miss_error(
            "DB-first symbol spec cache is empty or stale for BTC"
        )
        assert not worker._is_db_first_symbol_spec_cache_miss_error(
            "DB-first tick cache is empty or stale for BTC"
        )
    finally:
        worker.broker.close()
        worker.store.close()


def test_db_first_tick_cache_miss_uses_exponential_backoff(tmp_path, monkeypatch: pytest.MonkeyPatch):
    worker = _make_worker(tmp_path, poll_interval_sec=1.0, stream_event_cooldown_sec=5.0)
    clock = {"now": 1_000.0}
    monkeypatch.setattr("xtb_bot.worker.time.time", lambda: clock["now"])
    try:
        first_wait = worker._handle_db_first_tick_cache_miss()
        first_retry_after = worker._db_first_tick_cache_retry_after_ts
        events = worker.store.load_events(limit=5)
        assert any(event.get("message") == "DB-first tick cache warming up" for event in events)
        assert first_wait == pytest.approx(5.0)
        assert first_retry_after == pytest.approx(1_005.0)

        clock["now"] = 1_006.0
        second_wait = worker._handle_db_first_tick_cache_miss()
        assert second_wait == pytest.approx(10.0)
        assert worker._db_first_tick_cache_retry_after_ts == pytest.approx(1_016.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_wait_with_state_heartbeat_refreshes_worker_state_during_long_backoff(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
):
    worker = _make_worker(tmp_path, poll_interval_sec=1.0)
    mono_clock = {"now": 0.0}
    waits: list[float] = []
    saved: list[tuple[float | None, str | None]] = []

    class _FakeEvent:
        @staticmethod
        def is_set() -> bool:
            return False

        def wait(self, timeout: float | None = None) -> bool:
            wait_for = max(0.0, float(timeout or 0.0))
            waits.append(wait_for)
            mono_clock["now"] += wait_for
            return False

    monkeypatch.setattr("xtb_bot.worker.time.monotonic", lambda: mono_clock["now"])
    monkeypatch.setattr(worker, "stop_event", _FakeEvent())
    monkeypatch.setattr(
        worker,
        "_save_state",
        lambda last_price, last_error=None: saved.append((last_price, last_error)),
    )

    try:
        worker.worker_state_flush_interval_sec = 5.0
        worker._wait_with_state_heartbeat(
            17.0,
            last_price=123.45,
            last_error="DB-first tick cache is empty or stale for GOLD",
        )
        assert waits == pytest.approx([5.0, 5.0, 5.0, 2.0])
        assert saved == [
            (123.45, "DB-first tick cache is empty or stale for GOLD"),
            (123.45, "DB-first tick cache is empty or stale for GOLD"),
            (123.45, "DB-first tick cache is empty or stale for GOLD"),
        ]
    finally:
        worker.broker.close()
        worker.store.close()


def test_db_first_symbol_spec_cache_miss_uses_symbol_spec_retry_backoff(tmp_path, monkeypatch: pytest.MonkeyPatch):
    worker = _make_worker(tmp_path, poll_interval_sec=1.0, stream_event_cooldown_sec=5.0)
    monkeypatch.setattr("xtb_bot.worker.time.time", lambda: 2_000.0)
    try:
        worker._symbol_spec_retry_backoff_sec = 40.0
        wait_sec = worker._handle_db_first_symbol_spec_cache_miss()
        assert wait_sec == pytest.approx(40.0)
        events = worker.store.load_events(limit=5)
        warming_events = [event for event in events if event.get("message") == "DB-first symbol spec cache warming up"]
        assert warming_events
        assert float(warming_events[0]["payload"]["retry_after_sec"]) == pytest.approx(40.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_load_symbol_spec_uses_stale_db_first_fallback(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.db_first_reads_enabled = True
        worker.symbol_spec = None
        worker.db_first_symbol_spec_max_age_sec = 5.0
        stale_ts = time.time() - 3_600.0
        worker.store.upsert_broker_symbol_spec(
            symbol=worker.symbol,
            spec=SymbolSpec(
                symbol=worker.symbol,
                tick_size=0.0001,
                tick_value=10.0,
                contract_size=100000.0,
                lot_min=0.01,
                lot_max=100.0,
                lot_step=0.01,
            ),
            ts=stale_ts,
            source="test",
        )

        worker._load_symbol_spec()
        assert worker.symbol_spec is not None
        assert worker.symbol_spec.tick_size == pytest.approx(0.0001)

        events = worker.store.load_events(limit=10)
        assert any(event.get("message") == "Loaded stale DB-first symbol spec fallback" for event in events)
    finally:
        worker.broker.close()
        worker.store.close()


def test_load_symbol_spec_uses_cached_index_variant_without_live_refresh(tmp_path):
    worker = _make_worker(tmp_path, symbol="UK100")
    try:
        worker.db_first_reads_enabled = True
        worker.symbol_spec = None
        worker.db_first_symbol_spec_max_age_sec = 3600.0
        worker.store.upsert_broker_symbol_spec(
            symbol=worker.symbol,
            spec=SymbolSpec(
                symbol=worker.symbol,
                tick_size=1.0,
                tick_value=10.0,
                contract_size=10.0,
                lot_min=1.0,
                lot_max=100.0,
                lot_step=1.0,
                metadata={"broker": "ig", "epic": "IX.D.FTSE.DAILY.IP", "epic_variant": "daily"},
            ),
            ts=time.time(),
            source="test",
        )
        def _unexpected_refresh(symbol: str):
            raise AssertionError(f"unexpected live symbol-spec refresh for {symbol}")

        worker.broker.get_symbol_spec = _unexpected_refresh  # type: ignore[assignment]

        worker._load_symbol_spec()

        assert worker.symbol_spec is not None
        assert worker.symbol_spec.metadata.get("epic") == "IX.D.FTSE.DAILY.IP"
        events = worker.store.load_events(limit=20)
        assert not any(event.get("message") == "Resolving live index symbol variant from broker" for event in events)
    finally:
        worker.broker.close()
        worker.store.close()


def test_load_symbol_spec_uses_cached_active_position_epic_variant_without_live_refresh(tmp_path):
    worker = _make_worker(tmp_path, symbol="UK100")
    try:
        worker.db_first_reads_enabled = True
        worker.symbol_spec = None
        worker.db_first_symbol_spec_max_age_sec = 3600.0
        worker.store.upsert_broker_symbol_spec(
            symbol=worker.symbol,
            spec=SymbolSpec(
                symbol=worker.symbol,
                tick_size=1.0,
                tick_value=1.0,
                contract_size=1.0,
                lot_min=1.0,
                lot_max=100.0,
                lot_step=1.0,
                metadata={"broker": "ig", "epic": "IX.D.FTSE.CASH.IP", "epic_variant": "cash"},
            ),
            ts=time.time(),
            source="cash_test",
        )
        worker.store.upsert_broker_symbol_spec(
            symbol=worker.symbol,
            spec=SymbolSpec(
                symbol=worker.symbol,
                tick_size=1.0,
                tick_value=10.0,
                contract_size=10.0,
                lot_min=0.5,
                lot_max=100.0,
                lot_step=0.5,
                metadata={"broker": "ig", "epic": "IX.D.FTSE.DAILY.IP", "epic_variant": "daily"},
            ),
            ts=time.time() + 1.0,
            source="daily_test",
        )
        worker.position_book.upsert(
            Position(
                position_id="deal-1",
                symbol="UK100",
                side=Side.BUY,
                volume=1.0,
                open_price=8000.0,
                stop_loss=7960.0,
                take_profit=8100.0,
                opened_at=time.time(),
                status="open",
                epic="IX.D.FTSE.DAILY.IP",
                epic_variant="daily",
            )
        )

        def _unexpected_refresh(symbol: str, epic: str):
            raise AssertionError(f"unexpected live symbol-spec refresh for {symbol}/{epic}")

        worker.broker.get_symbol_spec_for_epic = _unexpected_refresh  # type: ignore[assignment]

        worker._load_symbol_spec()

        assert worker.symbol_spec is not None
        assert worker.symbol_spec.metadata.get("epic") == "IX.D.FTSE.DAILY.IP"
        assert worker.symbol_spec.lot_min == pytest.approx(0.5)
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_affordability_retry_refreshes_live_current_epic_before_reject(tmp_path):
    worker = _make_worker(tmp_path, symbol="UK100", mode=RunMode.EXECUTION)
    try:
        stale_spec = SymbolSpec(
            symbol="UK100",
            tick_size=1.0,
            tick_value=10.0,
            contract_size=10.0,
            lot_min=1.0,
            lot_max=100.0,
            lot_step=1.0,
            min_stop_distance_price=4.0,
            price_precision=1,
            lot_precision=1,
            metadata={
                "broker": "ig",
                "epic": "IX.D.FTSE.DAILY.IP",
                "epic_variant": "daily",
                "lot_min_source": "stale_cached",
            },
        )
        live_spec = SymbolSpec(
            symbol="UK100",
            tick_size=1.0,
            tick_value=10.0,
            contract_size=10.0,
            lot_min=0.1,
            lot_max=100.0,
            lot_step=0.1,
            min_stop_distance_price=4.0,
            price_precision=1,
            lot_precision=1,
            metadata={
                "broker": "ig",
                "epic": "IX.D.FTSE.DAILY.IP",
                "epic_variant": "daily",
                "lot_min_source": "minDealSize",
            },
        )
        worker.symbol_spec = stale_spec
        worker.broker.get_symbol_spec_for_epic = lambda symbol, epic, force_refresh=False: live_spec  # type: ignore[assignment]
        worker.broker.get_symbol_spec_candidates_for_entry = lambda symbol, force_refresh=False: [live_spec]  # type: ignore[assignment]

        snapshot = AccountSnapshot(
            balance=10_856.0,
            equity=10_856.0,
            margin_free=10_856.0,
            timestamp=time.time(),
        )
        signal = Signal(
            side=Side.SELL,
            confidence=1.0,
            stop_loss_pips=31.25,
            take_profit_pips=62.5,
            metadata={"indicator": "test"},
        )
        entry = 8200.0
        initial_stop_loss, _ = worker._build_sl_tp_for_spec(
            stale_spec,
            signal.side,
            entry,
            signal.stop_loss_pips,
            signal.take_profit_pips,
        )
        initial_decision = worker.risk.can_open_trade(
            snapshot=snapshot,
            symbol="UK100",
            open_positions_count=0,
            entry=entry,
            stop_loss=initial_stop_loss,
            symbol_spec=stale_spec,
            current_spread_pips=1.0,
        )

        assert initial_decision.allowed is False

        retried_decision, stop_loss, take_profit, _ = worker._maybe_retry_with_more_affordable_entry_variant(
            snapshot=snapshot,
            signal=signal,
            entry=entry,
            bid=8199.5,
            ask=8200.5,
            current_spread_pips=1.0,
            decision=initial_decision,
        )

        assert retried_decision.allowed is True
        assert worker.symbol_spec is not None
        assert worker.symbol_spec.lot_min == pytest.approx(0.1)
        assert worker.symbol_spec.lot_step == pytest.approx(0.1)
        assert worker.symbol_spec.metadata.get("epic") == "IX.D.FTSE.DAILY.IP"
        assert stop_loss == pytest.approx(8231.25)
        assert take_profit == pytest.approx(8137.5)
    finally:
        worker.broker.close()
        worker.store.close()


def test_load_symbol_spec_rebinds_to_active_position_epic(tmp_path):
    worker = _make_worker(tmp_path, symbol="UK100")
    try:
        worker.db_first_reads_enabled = True
        worker.symbol_spec = None
        worker.db_first_symbol_spec_max_age_sec = 3600.0
        worker.store.upsert_broker_symbol_spec(
            symbol=worker.symbol,
            spec=SymbolSpec(
                symbol=worker.symbol,
                tick_size=1.0,
                tick_value=1.0,
                contract_size=1.0,
                lot_min=1.0,
                lot_max=100.0,
                lot_step=1.0,
                metadata={"broker": "ig", "epic": "IX.D.FTSE.CASH.IP", "epic_variant": "cash"},
            ),
            ts=time.time(),
            source="test",
        )
        worker.position_book.upsert(
            Position(
                position_id="deal-1",
                symbol="UK100",
                side=Side.BUY,
                volume=1.0,
                open_price=8000.0,
                stop_loss=7960.0,
                take_profit=8100.0,
                opened_at=time.time(),
                status="open",
                epic="IX.D.FTSE.DAILY.IP",
                epic_variant="daily",
            )
        )
        rebound_spec = SymbolSpec(
            symbol=worker.symbol,
            tick_size=1.0,
            tick_value=10.0,
            contract_size=10.0,
            lot_min=1.0,
            lot_max=100.0,
            lot_step=1.0,
            metadata={"broker": "ig", "epic": "IX.D.FTSE.DAILY.IP", "epic_variant": "daily"},
        )
        worker.broker.get_symbol_spec_for_epic = lambda symbol, epic: rebound_spec  # type: ignore[attr-defined]

        worker._load_symbol_spec()

        assert worker.symbol_spec is not None
        assert worker.symbol_spec.metadata.get("epic") == "IX.D.FTSE.DAILY.IP"
        events = worker.store.load_events(limit=20)
        assert any(event.get("message") == "Rebinding symbol specification to active position epic" for event in events)
    finally:
        worker.broker.close()
        worker.store.close()


def test_db_first_tick_load_uses_shared_memory_cache_without_store_read(tmp_path):
    shared_tick = PriceTick(
        symbol="EURUSD",
        bid=1.2150,
        ask=1.2152,
        timestamp=time.time(),
        volume=77.0,
    )
    worker = _make_worker(
        tmp_path,
        db_first_reads_enabled=True,
        latest_tick_getter=lambda symbol, max_age_sec: shared_tick,
    )
    try:
        worker.store.load_latest_broker_tick = lambda symbol, max_age_sec: (_ for _ in ()).throw(  # type: ignore[assignment]
            AssertionError("store.load_latest_broker_tick should not be called when shared cache getter is configured")
        )
        loaded = worker._load_db_first_tick()
        assert loaded is not None
        assert loaded.bid == pytest.approx(shared_tick.bid)
        assert loaded.ask == pytest.approx(shared_tick.ask)
        assert loaded.timestamp == pytest.approx(shared_tick.timestamp)
    finally:
        worker.broker.close()
        worker.store.close()


def test_db_first_tick_load_skips_store_even_when_shared_cache_misses(tmp_path):
    worker = _make_worker(
        tmp_path,
        db_first_reads_enabled=True,
        latest_tick_getter=lambda symbol, max_age_sec: None,
    )
    try:
        fallback_tick = PriceTick(
            symbol="EURUSD",
            bid=1.2050,
            ask=1.2052,
            timestamp=time.time(),
            volume=55.0,
        )
        worker.store.load_latest_broker_tick = lambda symbol, max_age_sec: fallback_tick  # type: ignore[assignment]
        loaded = worker._load_db_first_tick()
        assert loaded is not None
        assert loaded.bid == pytest.approx(fallback_tick.bid)
        assert loaded.ask == pytest.approx(fallback_tick.ask)
    finally:
        worker.broker.close()
        worker.store.close()


def test_db_first_tick_load_falls_back_to_store_when_shared_cache_errors(tmp_path):
    worker = _make_worker(
        tmp_path,
        db_first_reads_enabled=True,
        latest_tick_getter=lambda symbol, max_age_sec: (_ for _ in ()).throw(RuntimeError("cache miss")),
    )
    try:
        fallback_tick = PriceTick(
            symbol="EURUSD",
            bid=1.2250,
            ask=1.2253,
            timestamp=time.time(),
            volume=42.0,
        )
        worker.store.load_latest_broker_tick = lambda symbol, max_age_sec: fallback_tick  # type: ignore[assignment]
        loaded = worker._load_db_first_tick()
        assert loaded is not None
        assert loaded.bid == pytest.approx(fallback_tick.bid)
        assert loaded.ask == pytest.approx(fallback_tick.ask)
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_lease_check_stops_stale_worker(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        lease_key = f"worker.lease.{worker.symbol}"
        worker._worker_lease_key = lease_key
        worker._worker_lease_id = "lease-a"
        worker.store.set_kv(lease_key, "lease-a")

        assert worker._worker_lease_allows_trading(force_check=True) is True

        worker.store.set_kv(lease_key, "lease-b")
        assert worker._worker_lease_allows_trading(force_check=True) is False
        assert worker.stop_event.is_set()
    finally:
        worker.broker.close()
        worker.store.close()


def test_open_position_is_blocked_when_worker_lease_revoked(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        lease_key = f"worker.lease.{worker.symbol}"
        worker._worker_lease_key = lease_key
        worker._worker_lease_id = "lease-a"
        worker.store.set_kv(lease_key, "lease-b")

        worker._open_position(
            Side.BUY,
            entry=1.1000,
            stop_loss=1.0950,
            take_profit=1.1100,
            volume=0.1,
            confidence=0.8,
        )

        assert worker.position_book.get(worker.symbol) is None
        events = worker.store.load_events(limit=10)
        assert any(event.get("message") == "Open skipped: worker lease is no longer active" for event in events)
    finally:
        worker.broker.close()
        worker.store.close()


def test_close_position_proceeds_when_worker_lease_revoked(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION, stream_event_cooldown_sec=0.0)
    try:
        lease_key = f"worker.lease.{worker.symbol}"
        worker._worker_lease_key = lease_key
        worker._worker_lease_id = "lease-a"
        worker.store.set_kv(lease_key, "lease-b")

        position = Position(
            position_id="deal-lease-close-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)

        calls = {"count": 0}

        def _fake_close(_position):
            calls["count"] += 1

        worker.broker.close_position = _fake_close  # type: ignore[assignment]
        worker.broker.get_position_close_sync = lambda _position_id, **kwargs: {  # type: ignore[attr-defined]
            "source": "ig_confirm",
            "close_price": 1.0990,
            "realized_pnl": -10.0,
            "closed_at": 2.0,
        }

        worker._close_position(position, close_price=1.0990, reason="stop_loss")

        assert calls["count"] == 1
        assert worker.stop_event.is_set()
        assert worker.position_book.get("EURUSD") is None
        events = worker.store.load_events(limit=20)
        assert any(
            event.get("message")
            == "Worker lease revoked during close, proceeding with risk-reducing broker close"
            for event in events
        )
    finally:
        worker.broker.close()
        worker.store.close()


def test_db_first_active_position_tick_fallback_uses_direct_broker_price(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.db_first_reads_enabled = True
        active = Position(
            position_id="pos-1",
            symbol=worker.symbol,
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0950,
            take_profit=1.1100,
            opened_at=time.time(),
            entry_confidence=0.8,
        )
        worker.position_book.upsert(active)

        direct_tick = PriceTick(
            symbol=worker.symbol,
            bid=1.1010,
            ask=1.1012,
            timestamp=time.time(),
            volume=123.0,
        )
        worker.broker.get_price = lambda symbol: direct_tick  # type: ignore[assignment]

        tick = worker._load_direct_tick_for_active_position_fallback(active)
        assert tick is not None
        assert tick.bid == pytest.approx(direct_tick.bid)
        assert tick.ask == pytest.approx(direct_tick.ask)

        cached_tick = worker.store.load_latest_broker_tick(worker.symbol, max_age_sec=60.0)
        assert cached_tick is not None
        assert cached_tick.bid == pytest.approx(direct_tick.bid)
        assert cached_tick.ask == pytest.approx(direct_tick.ask)

        events = worker.store.load_events(limit=20)
        assert any(
            event.get("message")
            == "DB-first tick cache stale, direct broker fallback used for active position"
            for event in events
        )
    finally:
        worker.broker.close()
        worker.store.close()


def test_db_first_active_position_tick_fallback_returns_none_when_broker_has_no_quote(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.db_first_reads_enabled = True
        active = Position(
            position_id="pos-1",
            symbol=worker.symbol,
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0950,
            take_profit=1.1100,
            opened_at=time.time(),
            entry_confidence=0.8,
        )

        worker.broker.get_price = lambda symbol: None  # type: ignore[assignment]

        assert worker._load_direct_tick_for_active_position_fallback(active) is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_caps_market_data_allowance_backoff_to_five_seconds(tmp_path):
    worker = _make_worker(tmp_path, stream_event_cooldown_sec=60.0)
    try:
        worker.broker.get_public_api_backoff_remaining_sec = lambda: 30.0  # type: ignore[assignment]
        remaining = worker._handle_allowance_backoff_error(
            'IG API GET /markets/IX.D.SPTRD.DAILY.IP failed: 403 Forbidden '
            '{"errorCode":"error.public-api.exceeded-api-key-allowance"}'
        )
        assert remaining == pytest.approx(5.0)
        assert worker._runtime_remaining_sec(worker._allowance_backoff_until_ts) == pytest.approx(
            5.0,
            abs=1.0,
        )
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_syncs_local_lot_min_from_broker_minimum_error(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        assert worker.symbol_spec is not None
        worker.symbol_spec.lot_min = 0.5
        updated = worker._sync_local_symbol_lot_min_from_broker_error(
            "Requested size below broker minimum for EURUSD "
            "(requested=0.76, min=1, epic=CS.D.EURUSD.CFD.IP, "
            "lot_min_source=adaptive_reject_minimum_order_size_error)"
        )
        assert updated is True
        assert worker.symbol_spec.lot_min == pytest.approx(1.0)
        events = worker.store.load_events(limit=20)
        assert any(event["message"] == "Local symbol lot minimum updated" for event in events)
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_refreshes_symbol_spec_after_minimum_order_reject(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        assert worker.symbol_spec is not None
        worker.symbol_spec.lot_min = 0.1
        updated_spec = SymbolSpec(
            symbol=worker.symbol_spec.symbol,
            tick_size=worker.symbol_spec.tick_size,
            tick_value=worker.symbol_spec.tick_value,
            contract_size=worker.symbol_spec.contract_size,
            lot_min=0.2,
            lot_max=max(worker.symbol_spec.lot_max, 0.2),
            lot_step=worker.symbol_spec.lot_step,
            price_precision=worker.symbol_spec.price_precision,
            lot_precision=worker.symbol_spec.lot_precision,
            metadata=dict(worker.symbol_spec.metadata or {}),
        )
        worker.broker.get_symbol_spec = lambda symbol: updated_spec  # type: ignore[assignment]
        refreshed = worker._refresh_symbol_spec_after_min_order_reject(
            "IG deal rejected: MINIMUM_ORDER_SIZE_ERROR | epic=IX.D.DAX.IFS.IP direction=BUY size=0.1"
        )
        assert refreshed is True
        assert worker.symbol_spec.lot_min == pytest.approx(0.2)
        events = worker.store.load_events(limit=20)
        assert any(event["message"] == "Local symbol specification refreshed" for event in events)
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_refreshes_symbol_spec_after_order_size_increment_reject(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        assert worker.symbol_spec is not None
        worker.symbol_spec.lot_min = 0.1
        worker.symbol_spec.lot_step = 0.01
        updated_spec = SymbolSpec(
            symbol=worker.symbol_spec.symbol,
            tick_size=worker.symbol_spec.tick_size,
            tick_value=worker.symbol_spec.tick_value,
            contract_size=worker.symbol_spec.contract_size,
            lot_min=0.1,
            lot_max=max(worker.symbol_spec.lot_max, 0.1),
            lot_step=0.1,
            price_precision=worker.symbol_spec.price_precision,
            lot_precision=max(worker.symbol_spec.lot_precision, 2),
            metadata=dict(worker.symbol_spec.metadata or {}),
        )
        worker.broker.get_symbol_spec = lambda symbol: updated_spec  # type: ignore[assignment]
        refreshed = worker._refresh_symbol_spec_after_min_order_reject(
            "IG deal rejected: ORDER_SIZE_INCREMENT_ERROR | "
            "statusReason=Order size must be traded in set increments."
        )
        assert refreshed is True
        assert worker.symbol_spec.lot_min == pytest.approx(0.1)
        assert worker.symbol_spec.lot_step == pytest.approx(0.1)
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_refreshes_symbol_spec_after_size_increment_reject_code(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        assert worker.symbol_spec is not None
        worker.symbol_spec.lot_min = 0.1
        worker.symbol_spec.lot_step = 0.01
        updated_spec = SymbolSpec(
            symbol=worker.symbol_spec.symbol,
            tick_size=worker.symbol_spec.tick_size,
            tick_value=worker.symbol_spec.tick_value,
            contract_size=worker.symbol_spec.contract_size,
            lot_min=0.1,
            lot_max=max(worker.symbol_spec.lot_max, 0.1),
            lot_step=0.1,
            price_precision=worker.symbol_spec.price_precision,
            lot_precision=max(worker.symbol_spec.lot_precision, 2),
            metadata=dict(worker.symbol_spec.metadata or {}),
        )
        worker.broker.get_symbol_spec = lambda symbol: updated_spec  # type: ignore[assignment]
        refreshed = worker._refresh_symbol_spec_after_min_order_reject(
            "IG deal rejected: SIZE_INCREMENT | epic=CC.D.CL.UNC.IP direction=BUY size=0.309"
        )
        assert refreshed is True
        assert worker.symbol_spec.lot_step == pytest.approx(0.1)
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_refreshes_symbol_spec_after_invalid_size_error_text(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        assert worker.symbol_spec is not None
        worker.symbol_spec.lot_min = 0.1
        worker.symbol_spec.lot_step = 0.01
        updated_spec = SymbolSpec(
            symbol=worker.symbol_spec.symbol,
            tick_size=worker.symbol_spec.tick_size,
            tick_value=worker.symbol_spec.tick_value,
            contract_size=worker.symbol_spec.contract_size,
            lot_min=0.1,
            lot_max=max(worker.symbol_spec.lot_max, 0.1),
            lot_step=0.1,
            price_precision=worker.symbol_spec.price_precision,
            lot_precision=max(worker.symbol_spec.lot_precision, 2),
            metadata=dict(worker.symbol_spec.metadata or {}),
        )
        worker.broker.get_symbol_spec = lambda symbol: updated_spec  # type: ignore[assignment]
        refreshed = worker._refresh_symbol_spec_after_min_order_reject(
            "IG API POST /positions/otc failed: 400 Bad Request "
            '{"errorCode":"error.service.otc.invalid.size"}'
        )
        assert refreshed is True
        assert worker.symbol_spec.lot_step == pytest.approx(0.1)
    finally:
        worker.broker.close()
        worker.store.close()


def test_g1_worker_history_buffer_scales_for_closed_candles(tmp_path):
    store = StateStore(tmp_path / "g1-buffer.db")
    risk = RiskManager(RiskConfig(), store)
    broker = MockBrokerClient(start_balance=10_000.0)
    broker.connect()
    try:
        worker = SymbolWorker(
            symbol="EURUSD",
            mode=RunMode.PAPER,
            strategy_name="g1",
            strategy_params={"g1_candle_timeframe_sec": 60},
            broker=broker,
            store=store,
            risk=risk,
            position_book=PositionBook(),
            stop_event=threading.Event(),
            poll_interval_sec=15.0,
            poll_jitter_sec=0.0,
            default_volume=0.0,
            bot_magic_prefix="XTBBOT",
            bot_magic_instance="TEST01",
        )
        ticks_per_candle = 4  # ceil(60 / 15)
        required_min_buffer = (worker.strategy.min_history + 2) * ticks_per_candle
        assert worker.prices.maxlen >= required_min_buffer
        assert worker.price_timestamps.maxlen >= required_min_buffer
    finally:
        broker.close()
        store.close()


def test_reverse_signal_exit_reason(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.strategy = _FakeStrategy(Side.SELL)
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )

        reason = worker._reverse_signal_exit_reason(position)
        assert reason == "reverse_signal:sell"
    finally:
        worker.broker.close()
        worker.store.close()


def test_reverse_signal_exit_reason_triggers_on_hold_signal_invalidation(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={"momentum_protective_exit_on_signal_invalidation": True},
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "ma_cross",
                    "reason": "cross_not_confirmed",
                    "direction": "down",
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-1b",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )

        reason = worker._reverse_signal_exit_reason(position)
        assert reason == "signal_invalidation:sell"
    finally:
        worker.broker.close()
        worker.store.close()


def test_reverse_signal_exit_reason_triggers_on_hold_signal_invalidation_for_g1_too(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="g1",
        strategy_param_overrides={"g1_protective_exit_on_signal_invalidation": True},
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "g1",
                    "reason": "no_ema_cross",
                    "fast_ema": 1.0990,
                    "slow_ema": 1.1000,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-1b-g1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )

        reason = worker._reverse_signal_exit_reason(position)
        assert reason == "signal_invalidation:sell"
    finally:
        worker.broker.close()
        worker.store.close()


def test_reverse_signal_exit_reason_ignores_hold_without_directional_metadata(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={"momentum_protective_exit_on_signal_invalidation": True},
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "ma_cross",
                    "reason": "spread_too_wide",
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-1c",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )

        reason = worker._reverse_signal_exit_reason(position)
        assert reason is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_reverse_signal_exit_reason_ignores_multi_strategy_hold_flat_signal(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={"momentum_protective_exit_on_signal_invalidation": True},
    )
    try:
        position = Position(
            position_id="paper-1c-multi-flat",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        signal = Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "indicator": "multi_strategy_netting",
                "reason": "multi_net_flat",
                "fast_ema": 1.0990,
                "slow_ema": 1.1000,
            },
        )

        reason = worker._reverse_signal_exit_reason_from_signal(position, signal, mark_price=1.0995)
        assert reason is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_reverse_signal_exit_reason_from_signal_blocks_until_profit_is_armed(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "momentum_protective_exit_require_armed_profit": True,
            "momentum_protective_exit_arm_tp_progress": 0.25,
            "momentum_protective_exit_arm_min_profit_pips": 5.0,
        },
    )
    try:
        position = Position(
            position_id="paper-1d",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        opposite_signal = Signal(
            side=Side.SELL,
            confidence=0.9,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"indicator": "ma_cross"},
        )

        reason = worker._reverse_signal_exit_reason_from_signal(
            position, opposite_signal, mark_price=1.1003
        )
        assert reason is None

        reason = worker._reverse_signal_exit_reason_from_signal(
            position, opposite_signal, mark_price=1.1006
        )
        assert reason == "reverse_signal:sell"
    finally:
        worker.broker.close()
        worker.store.close()


def test_reverse_signal_exit_reason_from_signal_respects_fresh_reversal_grace(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "momentum_protective_exit_require_armed_profit": False,
            "protective_fresh_reversal_grace_sec": 12.0,
            "protective_fresh_reversal_grace_max_adverse_spread_ratio": 2.5,
            "protective_fresh_reversal_grace_max_adverse_stop_ratio": 0.12,
        },
    )
    try:
        now = {"ts": 1_700_200_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])
        worker.spreads_pips.append(0.8)
        position = Position(
            position_id="paper-fresh-reverse-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=now["ts"] - 1.0,
            status="open",
        )
        opposite_signal = Signal(
            side=Side.SELL,
            confidence=0.9,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"indicator": "ma_cross"},
        )

        reason = worker._reverse_signal_exit_reason_from_signal(
            position,
            opposite_signal,
            mark_price=1.0999,
        )
        assert reason is None

        now["ts"] += 20.0
        reason = worker._reverse_signal_exit_reason_from_signal(
            position,
            opposite_signal,
            mark_price=1.0999,
        )
        assert reason == "reverse_signal:sell"
    finally:
        worker.broker.close()
        worker.store.close()


def test_reverse_signal_exit_reason_from_signal_profit_lock_on_reversal(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "momentum_protective_exit_require_armed_profit": True,
            "momentum_protective_exit_arm_tp_progress": 0.9,
            "momentum_protective_exit_arm_min_profit_pips": 50.0,
            "momentum_protective_profit_lock_on_reversal": True,
            "momentum_protective_profit_lock_min_profit_pips": 1.0,
        },
    )
    try:
        position = Position(
            position_id="paper-1d-profit-lock",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        opposite_signal = Signal(
            side=Side.SELL,
            confidence=0.9,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"indicator": "ma_cross"},
        )

        reason = worker._reverse_signal_exit_reason_from_signal(
            position, opposite_signal, mark_price=1.1002
        )
        assert reason == "profit_lock:reverse_signal:sell"
    finally:
        worker.broker.close()
        worker.store.close()


def test_reverse_signal_exit_reason_from_signal_peak_drawdown_profit_lock(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "momentum_protective_peak_drawdown_exit_enabled": True,
            "momentum_protective_peak_drawdown_ratio": 0.5,
            "momentum_protective_peak_drawdown_min_peak_pips": 5.0,
            "momentum_protective_exit_require_armed_profit": True,
            "momentum_protective_exit_arm_tp_progress": 0.95,
            "momentum_protective_exit_arm_min_profit_pips": 50.0,
            "momentum_protective_profit_lock_on_reversal": False,
        },
    )
    try:
        position = Position(
            position_id="paper-1d-peak-drawdown",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker._position_peak_favorable_pips[position.position_id] = 20.0
        opposite_signal = Signal(
            side=Side.SELL,
            confidence=0.9,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"indicator": "ma_cross"},
        )

        reason = worker._reverse_signal_exit_reason_from_signal(
            position, opposite_signal, mark_price=1.1006
        )
        assert reason == "profit_lock_peak_drawdown:reverse_signal:sell"
    finally:
        worker.broker.close()
        worker.store.close()


def test_should_close_position_on_peak_drawdown_price_retrace(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "momentum_protective_peak_drawdown_hard_exit_enabled": True,
            "momentum_protective_peak_drawdown_exit_enabled": True,
            "momentum_protective_peak_drawdown_ratio": 0.4,
            "momentum_protective_peak_drawdown_min_peak_pips": 8.0,
            "protective_runner_preservation_enabled": False,
        },
    )
    try:
        position = Position(
            position_id="paper-peak-drawdown-hard-close",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0980,
            take_profit=1.1060,
            opened_at=1.0,
            status="open",
        )
        worker._position_peak_favorable_pips[position.position_id] = 20.0

        should_close, mark, reason = worker._should_close_position(position, bid=1.1008, ask=1.1010)
        assert should_close is True
        assert mark == pytest.approx(1.1008)
        assert reason == "profit_lock_peak_drawdown:price_retrace"
    finally:
        worker.broker.close()
        worker.store.close()


def test_peak_drawdown_hard_exit_waits_for_minimum_tp_progress(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "momentum_protective_peak_drawdown_hard_exit_enabled": True,
            "momentum_protective_peak_drawdown_exit_enabled": True,
            "momentum_protective_peak_drawdown_ratio": 0.35,
            "momentum_protective_peak_drawdown_min_peak_pips": 4.0,
            "momentum_protective_exit_arm_tp_progress": 0.25,
            "protective_runner_preservation_enabled": False,
        },
    )
    try:
        position = Position(
            position_id="paper-peak-drawdown-progress-gate",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1040,  # 40 pips TP -> progress gate requires >= 10 pips peak.
            opened_at=1.0,
            status="open",
        )
        worker._position_peak_favorable_pips[position.position_id] = 8.0

        should_close, _, reason = worker._should_close_position(position, bid=1.1004, ask=1.1006)
        assert should_close is False
        assert reason == ""

        worker._position_peak_favorable_pips[position.position_id] = 12.0
        should_close, _, reason = worker._should_close_position(position, bid=1.1004, ask=1.1006)
        assert should_close is True
        assert reason == "profit_lock_peak_drawdown:price_retrace"
    finally:
        worker.broker.close()
        worker.store.close()


def test_index_trend_peak_drawdown_hard_exit_arms_from_initial_stop_before_tp_progress(tmp_path):
    worker = _make_worker(
        tmp_path,
        symbol="US100",
        strategy_name="momentum",
        strategy_param_overrides={
            "momentum_protective_peak_drawdown_hard_exit_enabled": True,
            "momentum_protective_peak_drawdown_exit_enabled": True,
            "momentum_protective_peak_drawdown_ratio": 0.35,
            "momentum_protective_peak_drawdown_min_peak_pips": 4.0,
            "momentum_protective_exit_arm_tp_progress": 0.25,
            "protective_runner_preservation_enabled": False,
        },
    )
    try:
        position = Position(
            position_id="paper-index-trend-peak-drawdown-progress-gate",
            symbol="US100",
            side=Side.BUY,
            volume=0.5,
            open_price=25000.0,
            stop_loss=24990.0,
            take_profit=25040.0,
            opened_at=1.0,
            status="open",
        )
        worker._position_entry_component_by_id[position.position_id] = "index_hybrid"
        worker._position_entry_signal_by_id[position.position_id] = "index_hybrid:trend_following"
        worker._position_peak_favorable_pips[position.position_id] = 8.0

        should_close, _, reason = worker._should_close_position(position, bid=25005.0, ask=25006.0)
        assert should_close is True
        assert reason == "profit_lock_peak_drawdown:price_retrace"
    finally:
        worker.broker.close()
        worker.store.close()


def test_should_close_position_on_peak_stagnation(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "momentum_protective_peak_stagnation_exit_enabled": True,
            "momentum_protective_peak_stagnation_timeout_sec": 60.0,
            "momentum_protective_peak_stagnation_min_peak_pips": 8.0,
            "momentum_protective_peak_stagnation_min_retain_ratio": 0.5,
            "momentum_protective_peak_drawdown_hard_exit_enabled": False,
            "protective_runner_preservation_enabled": False,
        },
    )
    try:
        position = Position(
            position_id="paper-peak-stagnation-close",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0980,
            take_profit=1.1060,
            opened_at=1.0,
            status="open",
        )
        worker._position_peak_favorable_pips[position.position_id] = 20.0
        worker._position_peak_favorable_ts[position.position_id] = time.time() - 90.0

        should_close, mark, reason = worker._should_close_position(position, bid=1.1007, ask=1.1009)
        assert should_close is True
        assert mark == pytest.approx(1.1007)
        assert reason == "profit_lock_peak_stagnation"
    finally:
        worker.broker.close()
        worker.store.close()


def test_peak_drawdown_hard_exit_runner_preservation_requires_deeper_retrace(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "momentum_protective_peak_drawdown_hard_exit_enabled": True,
            "momentum_protective_peak_drawdown_exit_enabled": True,
            "momentum_protective_peak_drawdown_ratio": 0.35,
            "momentum_protective_peak_drawdown_min_peak_pips": 4.0,
            "protective_runner_preservation_enabled": True,
            "protective_runner_min_tp_progress": 0.35,
            "protective_runner_min_peak_pips": 8.0,
            "protective_runner_min_retain_ratio": 0.45,
            "protective_runner_peak_drawdown_ratio_multiplier": 1.5,
        },
    )
    try:
        position = Position(
            position_id="paper-peak-drawdown-runner",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker._set_position_trailing_override(
            position.position_id,
            {
                "adaptive_trailing_family": "trend",
                "adaptive_trailing_initial_stop_pips": 10.0,
            },
        )
        worker._position_peak_favorable_pips[position.position_id] = 12.0

        should_close, _, reason = worker._should_close_position(position, bid=1.1007, ask=1.1009)
        assert should_close is False
        assert reason == ""

        should_close, _, reason = worker._should_close_position(position, bid=1.10055, ask=1.10075)
        assert should_close is True
        assert reason == "profit_lock_peak_drawdown:price_retrace"
    finally:
        worker.broker.close()
        worker.store.close()


def test_peak_drawdown_hard_exit_arms_before_runner_maturity(tmp_path):
    worker = _make_worker(
        tmp_path,
        symbol="WTI",
        strategy_name="momentum",
        strategy_param_overrides={
            "momentum_protective_peak_drawdown_hard_exit_enabled": True,
            "momentum_protective_peak_drawdown_exit_enabled": True,
            "momentum_protective_peak_drawdown_ratio": 0.30,
            "momentum_protective_peak_drawdown_min_peak_pips": 3.0,
            "momentum_protective_exit_arm_tp_progress": 0.25,
            "protective_runner_preservation_enabled": True,
            "protective_runner_min_tp_progress": 0.35,
            "protective_runner_min_peak_pips": 8.0,
            "protective_runner_min_retain_ratio": 0.45,
            "protective_runner_peak_drawdown_ratio_multiplier": 1.5,
        },
    )
    try:
        assert worker.symbol_spec is not None
        worker.symbol_spec.tick_size = 1.0
        worker.symbol_spec.tick_value = 10.0
        position = Position(
            position_id="paper-wti-peak-drawdown-pre-runner",
            symbol="WTI",
            side=Side.BUY,
            volume=0.23,
            open_price=10083.7,
            stop_loss=10053.0,
            take_profit=10144.0,
            opened_at=1.0,
            status="open",
        )
        worker._set_position_trailing_override(
            position.position_id,
            {
                "adaptive_trailing_family": "trend",
                "adaptive_trailing_initial_stop_pips": 30.7,
            },
        )
        worker._position_peak_favorable_pips[position.position_id] = 17.3

        should_close, mark, reason = worker._should_close_position(position, bid=10094.0, ask=10098.8)
        assert should_close is True
        assert mark == pytest.approx(10094.0)
        assert reason == "profit_lock_peak_drawdown:price_retrace"
    finally:
        worker.broker.close()
        worker.store.close()


def test_peak_stagnation_runner_preservation_extends_timeout(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "momentum_protective_peak_stagnation_exit_enabled": True,
            "momentum_protective_peak_stagnation_timeout_sec": 60.0,
            "momentum_protective_peak_stagnation_min_peak_pips": 8.0,
            "momentum_protective_peak_stagnation_min_retain_ratio": 0.5,
            "momentum_protective_peak_drawdown_hard_exit_enabled": False,
            "protective_runner_preservation_enabled": True,
            "protective_runner_min_tp_progress": 0.35,
            "protective_runner_min_peak_pips": 8.0,
            "protective_runner_min_retain_ratio": 0.45,
            "protective_runner_peak_stagnation_timeout_multiplier": 2.0,
        },
    )
    try:
        position = Position(
            position_id="paper-peak-stagnation-runner",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker._set_position_trailing_override(
            position.position_id,
            {
                "adaptive_trailing_family": "trend",
                "adaptive_trailing_initial_stop_pips": 10.0,
            },
        )
        worker._position_peak_favorable_pips[position.position_id] = 20.0
        worker._position_peak_favorable_ts[position.position_id] = time.time() - 90.0

        should_close, _, reason = worker._should_close_position(position, bid=1.1010, ask=1.1012)
        assert should_close is False
        assert reason == ""

        worker._position_peak_favorable_ts[position.position_id] = time.time() - 130.0
        should_close, _, reason = worker._should_close_position(position, bid=1.1010, ask=1.1012)
        assert should_close is True
        assert reason == "profit_lock_peak_stagnation"
    finally:
        worker.broker.close()
        worker.store.close()


def test_reverse_signal_exit_reason_from_signal_profit_lock_on_hold_invalidation(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "momentum_protective_profit_lock_on_hold_invalidation": True,
            "momentum_protective_profit_lock_min_profit_pips": 1.0,
        },
    )
    try:
        position = Position(
            position_id="paper-1d-profit-lock-hold",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        hold_signal = Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"indicator": "ma_cross", "reason": "no_signal"},
        )

        reason = worker._reverse_signal_exit_reason_from_signal(
            position, hold_signal, mark_price=1.1002
        )
        assert reason == "profit_lock:hold_invalidation:no_signal"
    finally:
        worker.broker.close()
        worker.store.close()


def test_reverse_signal_exit_reason_from_signal_triggers_early_loss_cut_when_never_profitable(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "momentum_protective_exit_require_armed_profit": True,
            "momentum_protective_exit_arm_tp_progress": 0.25,
            "momentum_protective_early_loss_cut_enabled": True,
            "momentum_protective_early_loss_cut_loss_ratio": 0.4,
            "momentum_protective_early_loss_cut_never_profitable_only": True,
            "momentum_protective_early_loss_cut_profit_tolerance_pips": 0.0,
        },
    )
    try:
        position = Position(
            position_id="paper-1e",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        opposite_signal = Signal(
            side=Side.SELL,
            confidence=0.9,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"indicator": "ma_cross"},
        )

        reason = worker._reverse_signal_exit_reason_from_signal(
            position, opposite_signal, mark_price=1.0996
        )
        assert reason == "early_loss_cut:reverse_signal:sell"
    finally:
        worker.broker.close()
        worker.store.close()


def test_reverse_signal_exit_reason_from_signal_triggers_early_loss_cut_on_hold_invalidation(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "momentum_protective_exit_require_armed_profit": True,
            "momentum_protective_exit_arm_tp_progress": 0.25,
            "momentum_protective_early_loss_cut_enabled": True,
            "momentum_protective_early_loss_cut_loss_ratio": 0.4,
            "momentum_protective_early_loss_cut_never_profitable_only": True,
            "momentum_protective_early_loss_cut_on_hold_invalidation": True,
        },
    )
    try:
        position = Position(
            position_id="paper-1e-hold",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        hold_signal = Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"indicator": "ma_cross", "reason": "no_signal"},
        )

        reason = worker._reverse_signal_exit_reason_from_signal(
            position, hold_signal, mark_price=1.0996
        )
        assert reason == "early_loss_cut:hold_invalidation:no_signal"
    finally:
        worker.broker.close()
        worker.store.close()


def test_reverse_signal_exit_reason_from_signal_early_loss_cut_respects_never_profitable_only(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "momentum_protective_exit_require_armed_profit": True,
            "momentum_protective_exit_arm_tp_progress": 0.25,
            "momentum_protective_early_loss_cut_enabled": True,
            "momentum_protective_early_loss_cut_loss_ratio": 0.4,
            "momentum_protective_early_loss_cut_never_profitable_only": True,
            "momentum_protective_early_loss_cut_profit_tolerance_pips": 0.0,
        },
    )
    try:
        position = Position(
            position_id="paper-1f",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        # Was profitable before: disallow early-loss-cut by this mode.
        worker._position_peak_favorable_pips[position.position_id] = 1.0
        opposite_signal = Signal(
            side=Side.SELL,
            confidence=0.9,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"indicator": "ma_cross"},
        )

        reason = worker._reverse_signal_exit_reason_from_signal(
            position, opposite_signal, mark_price=1.0996
        )
        assert reason is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_index_hybrid_dynamic_exit_reason_trend_mid_reentry(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.strategy_name = "index_hybrid"
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "index_hybrid",
                    "trend_regime": True,
                    "mean_reversion_regime": False,
                    "channel_mid": 100.0,
                },
            )
        )
        worker.prices.append(100.0)

        position = Position(
            position_id="paper-ix-1",
            symbol="US100",
            side=Side.BUY,
            volume=0.1,
            open_price=101.0,
            stop_loss=99.0,
            take_profit=103.0,
            opened_at=1.0,
            status="open",
        )

        signal = worker._strategy_exit_signal()
        reason = worker._strategy_dynamic_exit_reason(position, bid=99.9, ask=100.0, signal=signal)
        assert reason == "strategy_exit:index_hybrid:trend_mid_reentry"
    finally:
        worker.broker.close()
        worker.store.close()


def test_index_hybrid_dynamic_exit_reason_mean_reversion_mid_target(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.strategy_name = "index_hybrid"
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "index_hybrid",
                    "trend_regime": False,
                    "mean_reversion_regime": True,
                    "channel_mid": 100.0,
                    "channel_lower": 98.0,
                    "channel_upper": 102.0,
                },
            )
        )
        worker.prices.append(100.0)

        position = Position(
            position_id="paper-ix-2",
            symbol="US100",
            side=Side.SELL,
            volume=0.1,
            open_price=101.0,
            stop_loss=102.0,
            take_profit=99.0,
            opened_at=1.0,
            status="open",
        )

        signal = worker._strategy_exit_signal()
        reason = worker._strategy_dynamic_exit_reason(position, bid=99.7, ask=99.8, signal=signal)
        assert reason == "strategy_exit:index_hybrid:mean_mid_target"
    finally:
        worker.broker.close()
        worker.store.close()


def test_index_hybrid_dynamic_exit_reason_respects_trend_exit_hint_flags(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.strategy_name = "index_hybrid"
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "index_hybrid",
                    "exit_hint": "close_if_price_reenters_channel_mid",
                    "trend_exit_buy_reentry_mid": True,
                    "trend_exit_sell_reentry_mid": False,
                },
            )
        )
        worker.prices.append(100.0)

        position = Position(
            position_id="paper-ix-hint-1",
            symbol="US100",
            side=Side.BUY,
            volume=0.1,
            open_price=101.0,
            stop_loss=99.0,
            take_profit=103.0,
            opened_at=1.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        reason = worker._strategy_dynamic_exit_reason(position, bid=101.2, ask=101.3, signal=signal)
        assert reason == "strategy_exit:index_hybrid:trend_mid_reentry"
    finally:
        worker.broker.close()
        worker.store.close()


def test_effective_entry_signal_uses_index_hybrid_regime_token_for_multi_strategy_signal(tmp_path):
    worker = _make_worker(tmp_path, strategy_name="momentum")
    try:
        worker._multi_strategy_enabled = True
        signal = Signal(
            side=Side.BUY,
            confidence=0.55,
            stop_loss_pips=20.0,
            take_profit_pips=40.0,
            metadata={
                "indicator": "multi_strategy_netting",
                "multi_entry_strategy": "momentum",
                "multi_entry_strategy_component": "index_hybrid",
                "regime": "mean_reversion",
            },
        )

        assert worker._effective_entry_signal_from_signal(signal) == "index_hybrid:mean_reversion"
    finally:
        worker.broker.close()
        worker.store.close()


def test_position_management_family_uses_index_hybrid_entry_regime_token(tmp_path):
    worker = _make_worker(tmp_path, strategy_name="momentum")
    position = Position(
        position_id="DIAAAAIDXMEAN1",
        symbol="US30",
        side=Side.BUY,
        volume=1.0,
        open_price=46000.0,
        stop_loss=45970.0,
        take_profit=46060.0,
        opened_at=time.time(),
        entry_confidence=0.5,
        status="open",
        pnl=0.0,
    )
    try:
        worker._position_entry_component_by_id[position.position_id] = "index_hybrid"
        worker._position_entry_signal_by_id[position.position_id] = "index_hybrid:mean_reversion"
        assert worker._position_management_family(position) == "mean_reversion"

        worker._position_entry_signal_by_id[position.position_id] = "index_hybrid:trend_following"
        assert worker._position_management_family(position) == "trend"
    finally:
        worker.broker.close()
        worker.store.close()


def test_position_management_family_treats_g2_entries_as_trend(tmp_path):
    worker = _make_worker(tmp_path, strategy_name="g2")
    position = Position(
        position_id="DIAAAAG2TREND1",
        symbol="US30",
        side=Side.BUY,
        volume=1.0,
        open_price=46000.0,
        stop_loss=45970.0,
        take_profit=46090.0,
        opened_at=time.time(),
        entry_confidence=0.5,
        status="open",
        pnl=0.0,
    )
    try:
        worker._position_entry_component_by_id[position.position_id] = "g2"
        assert worker._position_management_family(position) == "trend"
    finally:
        worker.broker.close()
        worker.store.close()


def test_index_hybrid_dynamic_exit_reason_respects_mean_exit_hint_flags(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.strategy_name = "index_hybrid"
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "index_hybrid",
                    "exit_hint": "close_if_price_reaches_channel_mid_or_opposite_band",
                    "mean_exit_buy_mid_target": False,
                    "mean_exit_sell_mid_target": False,
                    "mean_exit_buy_opposite_band": True,
                    "mean_exit_sell_opposite_band": False,
                },
            )
        )
        worker.prices.append(100.0)

        position = Position(
            position_id="paper-ix-hint-2",
            symbol="US100",
            side=Side.BUY,
            volume=0.1,
            open_price=101.0,
            stop_loss=99.0,
            take_profit=103.0,
            opened_at=1.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        reason = worker._strategy_dynamic_exit_reason(position, bid=101.2, ask=101.3, signal=signal)
        assert reason == "strategy_exit:index_hybrid:mean_opposite_band"
    finally:
        worker.broker.close()
        worker.store.close()


def test_index_hybrid_dynamic_exit_reason_closes_on_regime_flip_against_position(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.strategy_name = "index_hybrid"
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "index_hybrid",
                    "regime_flip_exit_buy": True,
                    "regime_flip_exit_sell": False,
                },
            )
        )
        worker.prices.append(100.0)

        position = Position(
            position_id="paper-ix-flip-1",
            symbol="US100",
            side=Side.BUY,
            volume=0.1,
            open_price=101.0,
            stop_loss=99.0,
            take_profit=103.0,
            opened_at=1.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        reason = worker._strategy_dynamic_exit_reason(position, bid=100.4, ask=100.5, signal=signal)
        assert reason == "strategy_exit:index_hybrid:regime_flip_against_position"
    finally:
        worker.broker.close()
        worker.store.close()


def test_dynamic_exit_reason_ignored_for_non_index_hybrid_strategy(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_param_overrides={"protective_stale_loser_exit_enabled": False},
    )
    try:
        worker.strategy_name = "momentum"
        worker.strategy = _FakeStrategy(Side.HOLD)
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-m-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1,
            stop_loss=1.09,
            take_profit=1.12,
            opened_at=1.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        reason = worker._strategy_dynamic_exit_reason(position, bid=1.0, ask=1.01, signal=signal)
        assert reason is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_mean_breakout_v2_dynamic_exit_reason_on_breakout_invalidation(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.strategy_name = "mean_breakout_v2"
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "mean_breakout_v2",
                    "reason": "breakout_invalidated",
                    "exit_hint": "close_on_breakout_invalidation",
                    "breakout_invalidation_up": True,
                    "breakout_invalidation_down": False,
                },
            )
        )
        worker.prices.append(100.0)

        position = Position(
            position_id="paper-mb2-1",
            symbol="US100",
            side=Side.BUY,
            volume=0.1,
            open_price=101.0,
            stop_loss=99.0,
            take_profit=103.0,
            opened_at=1.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        reason = worker._strategy_dynamic_exit_reason(position, bid=100.0, ask=100.1, signal=signal)
        assert reason == "strategy_exit:mean_breakout_v2:breakout_invalidation"
    finally:
        worker.broker.close()
        worker.store.close()


def test_mean_reversion_bb_dynamic_exit_reason_on_exit_hint(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.strategy_name = "mean_reversion_bb"
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "bollinger_bands",
                    "reason": "mean_reversion_target_reached",
                    "exit_hint": "close_on_bb_midline",
                    "bb_midline": 1.1,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-bb-1",
            symbol="EURUSD",
            side=Side.SELL,
            volume=0.1,
            open_price=1.1010,
            stop_loss=1.1020,
            take_profit=1.0990,
            opened_at=1.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        reason = worker._strategy_dynamic_exit_reason(position, bid=1.1, ask=1.1002, signal=signal)
        assert reason == "strategy_exit:mean_reversion_bb:midline_reverted"
    finally:
        worker.broker.close()
        worker.store.close()


def test_mean_reversion_bb_dynamic_exit_reason_respects_execution_tp_floor(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.strategy_name = "mean_reversion_bb"
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "bollinger_bands",
                    "reason": "mean_reversion_target_reached",
                    "exit_hint": "close_on_bb_midline",
                    "execution_tp_floor_applied": True,
                    "execution_take_profit_pips": 20.0,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-bb-floor-1",
            symbol="EURUSD",
            side=Side.SELL,
            volume=0.1,
            open_price=1.1010,
            stop_loss=1.1020,
            take_profit=1.0990,
            opened_at=1.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        reason = worker._strategy_dynamic_exit_reason(position, bid=1.1, ask=1.1002, signal=signal)
        assert reason is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_mean_reversion_bb_dynamic_exit_reason_ignores_entry_signal_exit_hint(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.strategy_name = "mean_reversion_bb"
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.BUY,
                confidence=0.8,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "bollinger_bands",
                    "reason": "entry_signal",
                    "exit_hint": "close_on_bb_midline",
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-bb-2",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        reason = worker._strategy_dynamic_exit_reason(position, bid=1.1001, ask=1.1003, signal=signal)
        assert reason is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_momentum_dynamic_exit_reason_triggers_on_trend_reversal_in_profit_globally(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "momentum_protective_exit_enabled": True,
            "momentum_protective_exit_on_trend_reversal": True,
            "momentum_protective_exit_require_armed_profit": True,
            "momentum_protective_exit_arm_tp_progress": 0.25,
        },
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "ma_cross",
                    "reason": "no_ma_cross",
                    "fast_ma": 1.0990,
                    "slow_ma": 1.1000,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-mo-generic-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1030,
            opened_at=1.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        reason = worker._strategy_dynamic_exit_reason(position, bid=1.1009, ask=1.1011, signal=signal)
        assert reason == "strategy_exit:protective:trend_reversal"
    finally:
        worker.broker.close()
        worker.store.close()


def test_momentum_dynamic_exit_reason_triggers_loss_guard_on_trend_invalidation_globally(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "momentum_protective_exit_enabled": True,
            "momentum_protective_exit_on_trend_reversal": False,
            "momentum_protective_exit_loss_ratio": 0.8,
            "protective_stale_loser_exit_enabled": False,
        },
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "ma_cross",
                    "reason": "no_ma_cross",
                    "fast_ma": 1.0990,
                    "slow_ma": 1.1000,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-mo-generic-2",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1030,
            opened_at=1.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        reason = worker._strategy_dynamic_exit_reason(position, bid=1.09918, ask=1.09920, signal=signal)
        assert reason == "strategy_exit:protective:loss_guard_trend_invalidated"
    finally:
        worker.broker.close()
        worker.store.close()


def test_momentum_dynamic_exit_reason_triggers_stale_loser_timeout_globally(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "protective_stale_loser_exit_enabled": True,
            "protective_stale_loser_timeout_sec": 1800.0,
            "protective_stale_loser_loss_ratio": 0.55,
            "protective_stale_loser_profit_tolerance_pips": 0.0,
            "momentum_protective_exit_on_trend_reversal": False,
        },
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "ma_cross",
                    "reason": "trend_alive",
                    "fast_ma": 1.1010,
                    "slow_ma": 1.1000,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-mo-generic-stale-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1030,
            opened_at=1_000.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        with patch("xtb_bot.worker.time.time", return_value=2_801.0):
            reason = worker._strategy_dynamic_exit_reason(position, bid=1.09945, ask=1.09955, signal=signal)
        assert reason == "strategy_exit:protective:stale_loser_timeout"
    finally:
        worker.broker.close()
        worker.store.close()


def test_momentum_dynamic_exit_reason_stale_loser_timeout_respects_peak_profit_tolerance(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "protective_stale_loser_exit_enabled": True,
            "protective_stale_loser_timeout_sec": 1800.0,
            "protective_stale_loser_loss_ratio": 0.55,
            "protective_stale_loser_profit_tolerance_pips": 0.0,
            "momentum_protective_exit_on_trend_reversal": False,
        },
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "ma_cross",
                    "reason": "trend_alive",
                    "fast_ma": 1.1010,
                    "slow_ma": 1.1000,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-mo-generic-stale-2",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1030,
            opened_at=1_000.0,
            status="open",
        )
        worker._position_peak_favorable_pips[position.position_id] = 1.0
        worker._position_peak_favorable_ts[position.position_id] = 1_100.0

        signal = worker._strategy_exit_signal()
        with patch("xtb_bot.worker.time.time", return_value=2_801.0):
            reason = worker._strategy_dynamic_exit_reason(position, bid=1.09945, ask=1.09955, signal=signal)
        assert reason is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_momentum_dynamic_exit_reason_triggers_fast_fail_for_quick_bad_entry(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "protective_fast_fail_exit_enabled": True,
            "protective_fast_fail_timeout_sec": 300.0,
            "protective_fast_fail_loss_ratio": 0.4,
            "protective_fast_fail_profit_tolerance_pips": 6.0,
            "protective_stale_loser_exit_enabled": False,
            "momentum_protective_exit_on_trend_reversal": False,
        },
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "ma_cross",
                    "reason": "trend_alive",
                    "fast_ma": 1.1010,
                    "slow_ma": 1.1000,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-mo-fast-fail-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1030,
            opened_at=1_000.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        with patch("xtb_bot.worker.time.time", return_value=1_180.0):
            reason = worker._strategy_dynamic_exit_reason(position, bid=1.09955, ask=1.09965, signal=signal)
        assert reason == "strategy_exit:protective:fast_fail"
    finally:
        worker.broker.close()
        worker.store.close()


def test_momentum_dynamic_exit_reason_fast_fail_respects_peak_profit_tolerance(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "protective_fast_fail_exit_enabled": True,
            "protective_fast_fail_timeout_sec": 300.0,
            "protective_fast_fail_loss_ratio": 0.4,
            "protective_fast_fail_profit_tolerance_pips": 6.0,
            "protective_stale_loser_exit_enabled": False,
            "momentum_protective_exit_on_trend_reversal": False,
        },
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "ma_cross",
                    "reason": "trend_alive",
                    "fast_ma": 1.1010,
                    "slow_ma": 1.1000,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-mo-fast-fail-2",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1030,
            opened_at=1_000.0,
            status="open",
        )
        worker._position_peak_favorable_pips[position.position_id] = 8.0
        signal = worker._strategy_exit_signal()
        with patch("xtb_bot.worker.time.time", return_value=1_180.0):
            reason = worker._strategy_dynamic_exit_reason(position, bid=1.09955, ask=1.09965, signal=signal)
        assert reason is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_momentum_dynamic_exit_reason_fast_fail_uses_tp_progress_tolerance(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "protective_fast_fail_exit_enabled": True,
            "protective_fast_fail_timeout_sec": 300.0,
            "protective_fast_fail_loss_ratio": 0.4,
            "protective_fast_fail_profit_tolerance_pips": 6.0,
            "protective_fast_fail_peak_tp_progress_tolerance": 0.15,
            "protective_stale_loser_exit_enabled": False,
            "momentum_protective_exit_on_trend_reversal": False,
        },
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=100.0,
                metadata={
                    "indicator": "ma_cross",
                    "reason": "trend_alive",
                    "fast_ma": 1.1010,
                    "slow_ma": 1.1000,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-mo-fast-fail-progress-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1100,
            opened_at=1_000.0,
            status="open",
        )
        worker._position_peak_favorable_pips[position.position_id] = 7.0
        signal = worker._strategy_exit_signal()
        with patch("xtb_bot.worker.time.time", return_value=1_180.0):
            reason = worker._strategy_dynamic_exit_reason(position, bid=1.09955, ask=1.09965, signal=signal)
        assert reason == "strategy_exit:protective:fast_fail"
    finally:
        worker.broker.close()
        worker.store.close()


def test_momentum_dynamic_exit_reason_fast_fail_uses_zero_followthrough_threshold(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "protective_fast_fail_exit_enabled": True,
            "protective_fast_fail_timeout_sec": 300.0,
            "protective_fast_fail_loss_ratio": 0.4,
            "protective_fast_fail_profit_tolerance_pips": 6.0,
            "protective_fast_fail_zero_followthrough_timeout_sec": 240.0,
            "protective_fast_fail_zero_followthrough_loss_ratio": 0.2,
            "protective_stale_loser_exit_enabled": False,
            "momentum_protective_exit_on_trend_reversal": False,
        },
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "ma_cross",
                    "reason": "trend_alive",
                    "fast_ma": 1.1010,
                    "slow_ma": 1.1000,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-mo-fast-fail-zero-followthrough",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1030,
            opened_at=1_000.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        with patch("xtb_bot.worker.time.time", return_value=1_250.0):
            reason = worker._strategy_dynamic_exit_reason(position, bid=1.09978, ask=1.09988, signal=signal)
        assert reason == "strategy_exit:protective:fast_fail"
    finally:
        worker.broker.close()
        worker.store.close()


def test_momentum_dynamic_exit_reason_fast_fail_uses_symbol_zero_followthrough_override(tmp_path):
    worker = _make_worker(
        tmp_path,
        symbol="WTI",
        strategy_name="momentum",
        strategy_param_overrides={
            "protective_fast_fail_exit_enabled": True,
            "protective_fast_fail_timeout_sec": 300.0,
            "protective_fast_fail_loss_ratio": 0.4,
            "protective_fast_fail_profit_tolerance_pips": 6.0,
            "protective_fast_fail_zero_followthrough_timeout_sec": 240.0,
            "protective_fast_fail_zero_followthrough_timeout_sec_by_symbol": {"WTI": 60.0},
            "protective_fast_fail_zero_followthrough_loss_ratio": 0.2,
            "protective_fast_fail_zero_followthrough_loss_ratio_by_symbol": {"WTI": 0.1},
            "protective_stale_loser_exit_enabled": False,
            "momentum_protective_exit_on_trend_reversal": False,
        },
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "ma_cross",
                    "reason": "trend_alive",
                    "fast_ma": 80.2,
                    "slow_ma": 80.0,
                },
            )
        )
        worker.prices.append(80.0)

        position = Position(
            position_id="paper-mo-fast-fail-zero-followthrough-wti",
            symbol="WTI",
            side=Side.BUY,
            volume=0.1,
            open_price=80.0,
            stop_loss=79.0,
            take_profit=83.0,
            opened_at=1_000.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        with patch("xtb_bot.worker.time.time", return_value=1_090.0):
            reason = worker._strategy_dynamic_exit_reason(position, bid=79.88, ask=79.98, signal=signal)
        assert reason == "strategy_exit:protective:fast_fail"
    finally:
        worker.broker.close()
        worker.store.close()


def test_momentum_dynamic_exit_reason_fast_fail_uses_tp_progress_to_detect_fake_followthrough(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "protective_fast_fail_exit_enabled": True,
            "protective_fast_fail_timeout_sec": 300.0,
            "protective_fast_fail_loss_ratio": 0.4,
            "protective_fast_fail_profit_tolerance_pips": 6.0,
            "protective_fast_fail_peak_tp_progress_tolerance": 0.15,
            "protective_fast_fail_zero_followthrough_timeout_sec": 240.0,
            "protective_fast_fail_zero_followthrough_loss_ratio": 0.2,
            "protective_stale_loser_exit_enabled": False,
            "momentum_protective_exit_on_trend_reversal": False,
        },
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=100.0,
                metadata={
                    "indicator": "ma_cross",
                    "reason": "trend_alive",
                    "fast_ma": 1.1010,
                    "slow_ma": 1.1000,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-mo-fast-fail-fake-followthrough",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1100,
            opened_at=1_000.0,
            status="open",
        )
        worker._position_peak_favorable_pips[position.position_id] = 8.0
        signal = worker._strategy_exit_signal()
        with patch("xtb_bot.worker.time.time", return_value=1_250.0):
            reason = worker._strategy_dynamic_exit_reason(position, bid=1.09978, ask=1.09988, signal=signal)
        assert reason == "strategy_exit:protective:fast_fail"
    finally:
        worker.broker.close()
        worker.store.close()


def test_momentum_dynamic_exit_reason_fast_fail_continuation_entries_cut_earlier(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "protective_fast_fail_exit_enabled": True,
            "protective_fast_fail_timeout_sec": 300.0,
            "protective_fast_fail_loss_ratio": 0.4,
            "protective_fast_fail_profit_tolerance_pips": 6.0,
            "protective_fast_fail_zero_followthrough_timeout_sec": 240.0,
            "protective_fast_fail_zero_followthrough_loss_ratio": 0.2,
            "protective_stale_loser_exit_enabled": False,
            "momentum_protective_exit_on_trend_reversal": False,
        },
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "ma_cross",
                    "reason": "trend_alive",
                    "fast_ma": 1.1010,
                    "slow_ma": 1.1000,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-mo-fast-fail-continuation-early",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1030,
            opened_at=1_000.0,
            status="open",
        )
        worker._position_entry_signal_by_id[position.position_id] = "ma_trend_up"
        signal = worker._strategy_exit_signal()
        with patch("xtb_bot.worker.time.time", return_value=1_130.0):
            reason = worker._strategy_dynamic_exit_reason(position, bid=1.09978, ask=1.09988, signal=signal)
        assert reason == "strategy_exit:protective:fast_fail"
    finally:
        worker.broker.close()
        worker.store.close()


def test_momentum_dynamic_exit_reason_fast_fail_skips_when_tp_progress_is_meaningful(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "protective_fast_fail_exit_enabled": True,
            "protective_fast_fail_timeout_sec": 300.0,
            "protective_fast_fail_loss_ratio": 0.4,
            "protective_fast_fail_profit_tolerance_pips": 6.0,
            "protective_fast_fail_peak_tp_progress_tolerance": 0.15,
            "protective_stale_loser_exit_enabled": False,
            "momentum_protective_exit_on_trend_reversal": False,
        },
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=100.0,
                metadata={
                    "indicator": "ma_cross",
                    "reason": "trend_alive",
                    "fast_ma": 1.1010,
                    "slow_ma": 1.1000,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-mo-fast-fail-progress-2",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1100,
            opened_at=1_000.0,
            status="open",
        )
        worker._position_peak_favorable_pips[position.position_id] = 18.0
        signal = worker._strategy_exit_signal()
        with patch("xtb_bot.worker.time.time", return_value=1_180.0):
            reason = worker._strategy_dynamic_exit_reason(position, bid=1.09955, ask=1.09965, signal=signal)
        assert reason is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_momentum_dynamic_exit_reason_uses_aus200_stale_loser_symbol_override(tmp_path):
    worker = _make_worker(
        tmp_path,
        symbol="AUS200",
        strategy_name="momentum",
        strategy_param_overrides={
            "protective_stale_loser_exit_enabled": True,
            "protective_stale_loser_timeout_sec": 1800.0,
            "protective_stale_loser_timeout_sec_by_symbol": {"AUS200": 1200.0},
            "protective_stale_loser_loss_ratio": 0.55,
            "protective_stale_loser_loss_ratio_by_symbol": {"AUS200": 0.35},
            "protective_stale_loser_profit_tolerance_pips": 0.0,
            "momentum_protective_exit_on_trend_reversal": False,
        },
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=30.0,
                take_profit_pips=75.0,
                metadata={
                    "indicator": "ma_cross",
                    "reason": "trend_alive",
                    "fast_ma": 8440.0,
                    "slow_ma": 8420.0,
                },
            )
        )
        worker.prices.append(8429.4)

        position = Position(
            position_id="paper-au-stale-1",
            symbol="AUS200",
            side=Side.BUY,
            volume=3.0,
            open_price=8429.4,
            stop_loss=8399.0,
            take_profit=8505.0,
            opened_at=1_000.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        with patch("xtb_bot.worker.time.time", return_value=2_201.0):
            reason = worker._strategy_dynamic_exit_reason(position, bid=8418.5, ask=8419.5, signal=signal)
        assert reason == "strategy_exit:protective:stale_loser_timeout"
    finally:
        worker.broker.close()
        worker.store.close()


def test_g1_dynamic_exit_reason_triggers_on_ema_invalidation_near_stop(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="g1",
        strategy_param_overrides={
            "g1_protective_exit_enabled": True,
            "g1_protective_exit_loss_ratio": 0.8,
            "g1_protective_exit_on_trend_reversal": False,
            "protective_stale_loser_exit_enabled": False,
        },
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "g1",
                    "reason": "no_ema_cross",
                    "fast_ema": 1.0990,
                    "slow_ema": 1.1000,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-g1-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1030,
            opened_at=1.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        reason = worker._strategy_dynamic_exit_reason(position, bid=1.09918, ask=1.09920, signal=signal)
        assert reason == "strategy_exit:g1:loss_guard_ema_invalidated"
    finally:
        worker.broker.close()
        worker.store.close()


def test_g1_dynamic_exit_reason_does_not_trigger_when_loss_is_not_large_enough(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="g1",
        strategy_param_overrides={
            "g1_protective_exit_enabled": True,
            "g1_protective_exit_loss_ratio": 0.8,
            "g1_protective_exit_on_trend_reversal": False,
            "protective_stale_loser_exit_enabled": False,
        },
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "g1",
                    "reason": "no_ema_cross",
                    "fast_ema": 1.0990,
                    "slow_ema": 1.1000,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-g1-2",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1030,
            opened_at=1.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        reason = worker._strategy_dynamic_exit_reason(position, bid=1.09925, ask=1.09927, signal=signal)
        assert reason is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_g1_dynamic_exit_reason_triggers_on_trend_reversal_even_while_position_is_in_profit(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="g1",
        strategy_param_overrides={
            "g1_protective_exit_enabled": True,
            "g1_protective_exit_loss_ratio": 0.8,
        },
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "g1",
                    "reason": "no_ema_cross",
                    "fast_ema": 1.0990,
                    "slow_ema": 1.1000,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-g1-3",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1030,
            opened_at=1.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        reason = worker._strategy_dynamic_exit_reason(position, bid=1.1009, ask=1.1011, signal=signal)
        assert reason == "strategy_exit:g1:trend_reversal"
    finally:
        worker.broker.close()
        worker.store.close()


def test_g1_dynamic_exit_reason_can_disable_trend_reversal_exit(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="g1",
        strategy_param_overrides={
            "g1_protective_exit_enabled": True,
            "g1_protective_exit_loss_ratio": 0.8,
            "g1_protective_exit_on_trend_reversal": False,
        },
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "g1",
                    "reason": "no_ema_cross",
                    "fast_ema": 1.0990,
                    "slow_ema": 1.1000,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-g1-3b",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1030,
            opened_at=1.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        reason = worker._strategy_dynamic_exit_reason(position, bid=1.1002, ask=1.1004, signal=signal)
        assert reason is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_g1_dynamic_exit_reason_trend_reversal_waits_for_armed_profit(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="g1",
        strategy_param_overrides={
            "g1_protective_exit_enabled": True,
            "g1_protective_exit_on_trend_reversal": True,
            "g1_protective_exit_require_armed_profit": True,
            "g1_protective_exit_arm_tp_progress": 0.25,
        },
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "g1",
                    "reason": "no_ema_cross",
                    "fast_ema": 1.0990,
                    "slow_ema": 1.1000,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-g1-3c",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1030,
            opened_at=1.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        reason = worker._strategy_dynamic_exit_reason(position, bid=1.1002, ask=1.1004, signal=signal)
        assert reason is None
        reason = worker._strategy_dynamic_exit_reason(position, bid=1.1009, ask=1.1011, signal=signal)
        assert reason == "strategy_exit:g1:trend_reversal"
    finally:
        worker.broker.close()
        worker.store.close()


def test_g1_dynamic_exit_reason_profit_lock_closes_profitable_reversal_before_armed_progress(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="g1",
        strategy_param_overrides={
            "g1_protective_exit_enabled": True,
            "g1_protective_exit_on_trend_reversal": True,
            "g1_protective_exit_require_armed_profit": True,
            "g1_protective_exit_arm_tp_progress": 0.6,
            "g1_protective_profit_lock_on_reversal": True,
            "g1_protective_profit_lock_min_profit_pips": 1.0,
        },
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "g1",
                    "reason": "no_ema_cross",
                    "fast_ema": 1.0990,
                    "slow_ema": 1.1000,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-g1-3d",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1030,
            opened_at=1.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        reason = worker._strategy_dynamic_exit_reason(position, bid=1.1002, ask=1.1004, signal=signal)
        assert reason == "profit_lock:g1:trend_reversal"
    finally:
        worker.broker.close()
        worker.store.close()


def test_g1_dynamic_exit_reason_triggers_early_loss_cut_before_sl_when_never_profitable(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="g1",
        strategy_param_overrides={
            "g1_protective_exit_enabled": True,
            "g1_protective_exit_on_trend_reversal": True,
            "g1_protective_exit_require_armed_profit": True,
            "g1_protective_exit_arm_tp_progress": 0.25,
            "g1_protective_early_loss_cut_enabled": True,
            "g1_protective_early_loss_cut_loss_ratio": 0.4,
            "g1_protective_early_loss_cut_never_profitable_only": True,
            "g1_protective_early_loss_cut_profit_tolerance_pips": 0.0,
        },
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "g1",
                    "reason": "no_ema_cross",
                    "fast_ema": 1.0990,
                    "slow_ema": 1.1000,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-g1-3d",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1030,
            opened_at=1.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        reason = worker._strategy_dynamic_exit_reason(position, bid=1.0996, ask=1.0998, signal=signal)
        assert reason == "early_loss_cut:g1:trend_reversal"
    finally:
        worker.broker.close()
        worker.store.close()


def test_g1_dynamic_exit_reason_triggers_on_adx_regime_loss_near_stop(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="g1",
        strategy_param_overrides={
            "g1_protective_exit_enabled": True,
            "g1_protective_exit_loss_ratio": 0.8,
            "g1_protective_exit_allow_adx_regime_loss": True,
            "protective_stale_loser_exit_enabled": False,
        },
    )
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "g1",
                    "reason": "adx_below_threshold",
                    "adx_regime_active_after": False,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-g1-4",
            symbol="EURUSD",
            side=Side.SELL,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.1010,
            take_profit=1.0970,
            opened_at=1.0,
            status="open",
        )
        signal = worker._strategy_exit_signal()
        reason = worker._strategy_dynamic_exit_reason(position, bid=1.10080, ask=1.10082, signal=signal)
        assert reason == "strategy_exit:g1:loss_guard_adx_regime_lost"
    finally:
        worker.broker.close()
        worker.store.close()


def test_entry_cooldown_blocks_reentry_for_configured_interval(tmp_path, monkeypatch):
    worker = _make_worker(tmp_path, momentum_trade_cooldown_sec=30.0, hold_reason_log_interval_sec=1.0)
    try:
        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])

        position = Position(
            position_id="paper-cd-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=now["ts"] - 60.0,
            status="open",
        )

        worker.position_book.upsert(position)
        worker._close_position(position, close_price=1.0990, reason="stop_loss")

        assert worker._next_entry_allowed_ts == pytest.approx(now["ts"] + 30.0)
        assert worker._cooldown_allows_open(Side.BUY) is False

        events = [event for event in worker.store.load_events() if event["message"] == "Trade blocked by entry cooldown"]
        assert events
        assert events[0]["payload"]["remaining_sec"] == pytest.approx(30.0)
        assert events[0]["payload"]["cooldown_outcome"] == "loss"

        now["ts"] += 31.0
        assert worker._cooldown_allows_open(Side.BUY) is True
    finally:
        worker.broker.close()
        worker.store.close()


def test_entry_cooldown_can_be_shorter_after_win_than_after_loss(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        momentum_trade_cooldown_sec=0.0,
        momentum_trade_cooldown_win_sec=60.0,
        momentum_trade_cooldown_loss_sec=300.0,
    )
    try:
        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])

        win_position = Position(
            position_id="paper-win-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=now["ts"] - 60.0,
            status="open",
        )
        worker.position_book.upsert(win_position)
        worker._close_position(win_position, close_price=1.1010, reason="take_profit")

        assert worker._next_entry_allowed_ts == pytest.approx(now["ts"] + 60.0)
        assert worker._active_entry_cooldown_outcome == "win"

        now["ts"] += 61.0
        assert worker._cooldown_allows_open(Side.BUY) is True

        loss_position = Position(
            position_id="paper-loss-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=now["ts"] - 60.0,
            status="open",
        )
        worker.position_book.upsert(loss_position)
        worker._close_position(loss_position, close_price=1.0990, reason="stop_loss")

        assert worker._next_entry_allowed_ts == pytest.approx(now["ts"] + 300.0)
        assert worker._active_entry_cooldown_outcome == "loss"
    finally:
        worker.broker.close()
        worker.store.close()


def test_broker_reject_activates_entry_cooldown(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        momentum_trade_cooldown_sec=0.0,
        strategy_param_overrides={"momentum_open_reject_cooldown_sec": 45.0},
    )
    try:
        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])

        activated = worker._activate_reject_entry_cooldown(
            "IG deal rejected: UNKNOWN | epic=CC.D.LCO.USS.IP direction=BUY size=0.32"
        )
        assert activated is True
        assert worker._next_entry_allowed_ts == pytest.approx(now["ts"] + 45.0)
        assert worker._active_entry_cooldown_outcome == "broker_reject"
        assert worker._cooldown_allows_open(Side.BUY) is False

        now["ts"] += 46.0
        assert worker._cooldown_allows_open(Side.BUY) is True
    finally:
        worker.broker.close()
        worker.store.close()


def test_close_position_marks_pending_sync_when_broker_already_closed_position(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-404",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)

        def _fake_close(_position):
            raise BrokerError(
                'IG API POST /positions/otc failed: 404 Not Found {"errorCode":"error.service.marketdata.position.notional.details.null.error"}'
            )

        worker.broker.close_position = _fake_close  # type: ignore[assignment]
        worker._close_position(position, close_price=1.1010, reason="take_profit")

        assert worker.position_book.get("EURUSD") is None
        open_positions = worker.store.load_open_positions(mode="execution")
        assert "EURUSD" not in open_positions

        events = worker.store.load_events(limit=20)
        messages = [str(event.get("message")) for event in events]
        assert "Broker position missing on close" in messages
        closed = [event for event in events if event.get("message") == "Position closed"]
        assert closed
        payload = closed[0].get("payload") or {}
        reason = str(payload.get("reason") or "")
        assert reason == "broker_position_missing_on_close"
        assert payload.get("close_price") is None
        assert bool(payload.get("close_price_pending_broker_sync")) is True
    finally:
        worker.broker.close()
        worker.store.close()


def test_execution_finalize_close_never_persists_local_close_price(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-no-local-close",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)

        worker._finalize_position_close(
            position,
            close_price=1.0990,
            reason="stop_loss",
            broker_sync={"source": "ig_history_activity", "realized_pnl": -12.34},
            use_local_close_price=True,
        )

        record = worker.store.get_trade_record(position.position_id)
        assert record is not None
        assert record.get("close_price") is None
        assert float(record.get("pnl") or 0.0) == pytest.approx(-12.34)
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_applies_broker_tick_value_calibration_to_ambiguous_spec(tmp_path):
    worker = _make_worker(tmp_path, symbol="US100", mode=RunMode.EXECUTION, db_first_reads_enabled=True)
    try:
        raw_spec = SymbolSpec(
            symbol="US100",
            tick_size=1.0,
            tick_value=100.0,
            contract_size=100.0,
            lot_min=0.2,
            lot_max=100.0,
            lot_step=0.2,
            price_precision=1,
            lot_precision=1,
            metadata={
                "broker": "ig",
                "epic": "IX.D.NASDAQ.CASH.IP",
                "tick_value_source": "valueOfOnePip",
                "tick_value_kind": "pip",
                "one_pip_means": 0.0,
                "value_of_one_point": 0.0,
                "value_per_point": 0.0,
            },
        )
        worker.store.upsert_broker_symbol_spec(
            symbol="US100",
            spec=raw_spec,
            ts=time.time(),
            source="test_seed",
        )
        worker.store.save_broker_tick_value_calibration(
            symbol="US100",
            tick_size=1.0,
            tick_value=1.13,
            source="test_calibration",
            samples=3,
        )

        calibrated = worker._apply_broker_tick_value_calibration(raw_spec)

        assert calibrated is not None
        assert calibrated.tick_value == pytest.approx(1.13)
        metadata = calibrated.metadata or {}
        assert float(metadata.get("raw_tick_value_broker") or 0.0) == pytest.approx(100.0)
        assert str(metadata.get("tick_value_source") or "").endswith("|broker_close_calibration")
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_retries_index_entry_with_more_affordable_variant(tmp_path):
    worker = _make_worker(tmp_path, symbol="US500", mode=RunMode.EXECUTION)
    try:
        worker.risk.cfg.max_risk_per_trade_pct = 0.5
        expensive_spec = SymbolSpec(
            symbol="US500",
            tick_size=1.0,
            tick_value=250.0,
            contract_size=250.0,
            lot_min=0.2,
            lot_max=100.0,
            lot_step=0.2,
            min_stop_distance_price=4.0,
            price_precision=1,
            lot_precision=1,
            metadata={
                "broker": "ig",
                "epic": "IX.D.SPTRD.IFS.IP",
                "epic_variant": "ifs",
                "value_of_one_point": 250.0,
                "margin_factor": 5.0,
                "margin_factor_unit": "PERCENTAGE",
                "min_stop_distance_price": 4.0,
            },
        )
        cash_spec = SymbolSpec(
            symbol="US500",
            tick_size=1.0,
            tick_value=1.0,
            contract_size=1.0,
            lot_min=1.0,
            lot_max=100.0,
            lot_step=1.0,
            min_stop_distance_price=4.0,
            price_precision=1,
            lot_precision=1,
            metadata={
                "broker": "ig",
                "epic": "IX.D.SPTRD.CASH.IP",
                "epic_variant": "cash",
                "value_of_one_point": 1.0,
                "margin_factor": 5.0,
                "margin_factor_unit": "PERCENTAGE",
                "min_stop_distance_price": 4.0,
            },
        )
        daily_spec = SymbolSpec(
            symbol="US500",
            tick_size=1.0,
            tick_value=50.0,
            contract_size=50.0,
            lot_min=0.04,
            lot_max=100.0,
            lot_step=0.04,
            min_stop_distance_price=4.0,
            price_precision=1,
            lot_precision=2,
            metadata={
                "broker": "ig",
                "epic": "IX.D.SPTRD.DAILY.IP",
                "epic_variant": "daily",
                "value_of_one_point": 50.0,
                "margin_factor": 5.0,
                "margin_factor_unit": "PERCENTAGE",
                "min_stop_distance_price": 4.0,
            },
        )
        worker.symbol_spec = expensive_spec
        worker.broker.get_symbol_spec_candidates_for_entry = lambda symbol: [expensive_spec, daily_spec, cash_spec]  # type: ignore[attr-defined]
        worker.broker.get_symbol_spec_for_epic = lambda symbol, epic: {  # type: ignore[attr-defined]
            "IX.D.SPTRD.CASH.IP": cash_spec,
            "IX.D.SPTRD.DAILY.IP": daily_spec,
            "IX.D.SPTRD.IFS.IP": expensive_spec,
        }[epic]

        snapshot = AccountSnapshot(
            balance=10_856.0,
            equity=10_856.0,
            margin_free=10_856.0,
            timestamp=time.time(),
        )
        signal = Signal(
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=20.0,
            take_profit_pips=40.0,
            metadata={"indicator": "test"},
        )
        entry = 8000.0
        initial_stop_loss, _ = worker._build_sl_tp_for_spec(expensive_spec, signal.side, entry, signal.stop_loss_pips, signal.take_profit_pips)
        initial_decision = worker.risk.can_open_trade(
            snapshot=snapshot,
            symbol="US500",
            open_positions_count=0,
            entry=entry,
            stop_loss=initial_stop_loss,
            symbol_spec=expensive_spec,
            current_spread_pips=2.0,
        )

        assert initial_decision.allowed is False
        assert "below instrument minimum" in initial_decision.reason.lower()

        retried_decision, stop_loss, take_profit, _ = worker._maybe_retry_with_more_affordable_entry_variant(
            snapshot=snapshot,
            signal=signal,
            entry=entry,
            bid=7999.0,
            ask=8001.0,
            current_spread_pips=2.0,
            decision=initial_decision,
        )

        assert retried_decision.allowed is True
        assert worker.symbol_spec is not None
        assert worker.symbol_spec.metadata.get("epic") == "IX.D.SPTRD.CASH.IP"
        assert stop_loss == pytest.approx(7980.0)
        assert take_profit == pytest.approx(8040.0)
        events = worker.store.load_events(limit=20)
        assert any(event.get("message") == "Switched to more affordable broker variant" for event in events)
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_retries_brent_entry_with_more_affordable_variant(tmp_path):
    worker = _make_worker(tmp_path, symbol="BRENT", mode=RunMode.EXECUTION)
    try:
        worker.risk.cfg.max_risk_per_trade_pct = 0.5
        expensive_spec = SymbolSpec(
            symbol="BRENT",
            tick_size=1.0,
            tick_value=10.0,
            contract_size=10.0,
            lot_min=1.0,
            lot_max=100.0,
            lot_step=1.0,
            min_stop_distance_price=6.0,
            one_pip_means=1.0,
            price_precision=1,
            lot_precision=1,
            metadata={
                "broker": "ig",
                "epic": "CC.D.LCO.USS.IP",
                "epic_variant": "other",
                "value_of_one_pip": 10.0,
                "margin_factor": 10.0,
                "margin_factor_unit": "PERCENTAGE",
                "min_stop_distance_price": 6.0,
            },
        )
        cfd_spec = SymbolSpec(
            symbol="BRENT",
            tick_size=1.0,
            tick_value=10.0,
            contract_size=10.0,
            lot_min=0.1,
            lot_max=100.0,
            lot_step=0.1,
            min_stop_distance_price=6.0,
            one_pip_means=1.0,
            price_precision=1,
            lot_precision=1,
            metadata={
                "broker": "ig",
                "epic": "CC.D.LCO.CFD.IP",
                "epic_variant": "cfd",
                "value_of_one_pip": 10.0,
                "margin_factor": 10.0,
                "margin_factor_unit": "PERCENTAGE",
                "min_stop_distance_price": 6.0,
            },
        )
        worker.symbol_spec = expensive_spec
        worker.broker.get_symbol_spec_candidates_for_entry = lambda symbol: [expensive_spec, cfd_spec]  # type: ignore[attr-defined]
        worker.broker.get_symbol_spec_for_epic = lambda symbol, epic: {  # type: ignore[attr-defined]
            "CC.D.LCO.USS.IP": expensive_spec,
            "CC.D.LCO.CFD.IP": cfd_spec,
        }[epic]

        snapshot = AccountSnapshot(
            balance=10_856.0,
            equity=10_856.0,
            margin_free=10_856.0,
            timestamp=time.time(),
        )
        signal = Signal(
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=34.0,
            take_profit_pips=68.0,
            metadata={"indicator": "test"},
        )
        entry = 10061.0
        initial_stop_loss, _ = worker._build_sl_tp_for_spec(
            expensive_spec,
            signal.side,
            entry,
            signal.stop_loss_pips,
            signal.take_profit_pips,
        )
        initial_decision = worker.risk.can_open_trade(
            snapshot=snapshot,
            symbol="BRENT",
            open_positions_count=0,
            entry=entry,
            stop_loss=initial_stop_loss,
            symbol_spec=expensive_spec,
            current_spread_pips=4.8,
        )

        assert initial_decision.allowed is False
        assert "below instrument minimum" in initial_decision.reason.lower()

        retried_decision, stop_loss, take_profit, _ = worker._maybe_retry_with_more_affordable_entry_variant(
            snapshot=snapshot,
            signal=signal,
            entry=entry,
            bid=10058.6,
            ask=10063.4,
            current_spread_pips=4.8,
            decision=initial_decision,
        )

        assert retried_decision.allowed is True
        assert worker.symbol_spec is not None
        assert worker.symbol_spec.metadata.get("epic") == "CC.D.LCO.CFD.IP"
        assert worker.symbol_spec.lot_min == pytest.approx(0.1)
        assert stop_loss == pytest.approx(10027.0)
        assert take_profit == pytest.approx(10129.0)
        events = worker.store.load_events(limit=20)
        assert any(event.get("message") == "Switched to more affordable broker variant" for event in events)
    finally:
        worker.broker.close()
        worker.store.close()


def test_close_position_defers_when_allowance_backoff_active(tmp_path, monkeypatch):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION, stream_event_cooldown_sec=0.0)
    try:
        position = Position(
            position_id="deal-backoff-close",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)

        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])
        calls = {"count": 0}

        def _close_rejected(_position):
            calls["count"] += 1
            raise BrokerError(
                'IG API POST /positions/otc failed: 403 Forbidden {"errorCode":"error.public-api.exceeded-account-trading-allowance"}'
            )

        worker.broker.close_position = _close_rejected  # type: ignore[assignment]

        worker._close_position(position, close_price=1.0990, reason="emergency:drawdown")
        assert calls["count"] == 1
        assert worker.position_book.get("EURUSD") is not None
        assert worker._pending_close_retry_position_id == position.position_id
        assert worker._pending_close_retry_reason == "emergency:drawdown"
        first_retry_at = worker._pending_close_retry_until_ts
        assert first_retry_at > now["ts"]

        now["ts"] += 1.0
        worker._close_position(position, close_price=1.0990, reason="emergency:drawdown")
        assert calls["count"] == 1
        assert worker._pending_close_retry_until_ts == pytest.approx(first_retry_at)

        events = worker.store.load_events(limit=50)
        messages = [str(event.get("message") or "") for event in events]
        assert "Broker allowance backoff active" in messages
        assert "Position close deferred by broker allowance backoff" in messages
    finally:
        worker.broker.close()
        worker.store.close()


def test_close_position_waits_for_factual_broker_close_before_finalizing(tmp_path, monkeypatch):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION, stream_event_cooldown_sec=0.0)
    try:
        position = Position(
            position_id="deal-close-pending-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)

        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])
        worker.broker.close_position = lambda _position: None  # type: ignore[assignment]
        worker.broker.get_position_close_sync = lambda _position_id, **kwargs: None  # type: ignore[attr-defined]

        worker._close_position(position, close_price=1.0990, reason="stop_loss")

        assert worker.position_book.get("EURUSD") is not None
        assert worker._pending_close_verification_position_id == position.position_id
        assert worker._pending_close_verification_reason == "stop_loss"
        assert worker._pending_close_retry_position_id == position.position_id
        assert worker._pending_close_retry_until_ts > now["ts"]
        open_positions = worker.store.load_open_positions(mode="execution")
        assert position.position_id in open_positions

        events = worker.store.load_events(limit=20)
        messages = [str(event.get("message") or "") for event in events]
        assert "Broker close awaiting factual verification" in messages
        assert "Position closed" not in messages
    finally:
        worker.broker.close()
        worker.store.close()


def test_pending_close_verification_retries_and_finalizes_after_factual_sync(tmp_path, monkeypatch):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION, stream_event_cooldown_sec=0.0)
    try:
        position = Position(
            position_id="deal-close-pending-2",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)

        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])
        calls = {"close": 0, "sync": 0}

        def _fake_close(_position):
            calls["close"] += 1

        def _fake_sync(_position_id, **kwargs):
            _ = kwargs
            calls["sync"] += 1
            if calls["sync"] == 1:
                return None
            return {
                "source": "ig_history_activity",
                "close_price": 1.0987,
                "realized_pnl": -13.0,
                "closed_at": now["ts"] - 5.0,
            }

        worker.broker.close_position = _fake_close  # type: ignore[assignment]
        worker.broker.get_position_close_sync = _fake_sync  # type: ignore[attr-defined]

        worker._close_position(position, close_price=1.0990, reason="stop_loss")
        assert calls["close"] == 1
        assert worker.position_book.get("EURUSD") is not None

        retry_at = worker._pending_close_retry_until_ts
        now["ts"] = retry_at + 0.1
        tick = PriceTick(symbol="EURUSD", bid=1.0987, ask=1.0989, timestamp=10.0)
        handled = worker._maybe_execute_pending_close_retry(position, tick)

        assert handled is True
        assert calls["close"] == 2
        assert worker.position_book.get("EURUSD") is None
        assert worker._pending_close_verification_position_id is None

        events = [event for event in worker.store.load_events(limit=30) if event.get("message") == "Position closed"]
        assert events
        payload = events[0].get("payload") or {}
        assert float(payload.get("close_price") or 0.0) == pytest.approx(1.0987)
        assert float(payload.get("pnl") or 0.0) == pytest.approx(-13.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_close_position_defers_for_retry_on_retryable_broker_error(tmp_path, monkeypatch):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION, stream_event_cooldown_sec=0.0)
    try:
        position = Position(
            position_id="deal-retry-close",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)

        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])
        calls = {"count": 0}

        def _close_rejected(_position):
            calls["count"] += 1
            raise BrokerError(
                'IG API DELETE /positions/otc failed: 400 Bad Request {"errorCode":"validation.null-not-allowed.request"}'
            )

        worker.broker.close_position = _close_rejected  # type: ignore[assignment]

        worker._close_position(position, close_price=1.0990, reason="emergency:drawdown")
        assert calls["count"] == 1
        assert worker.position_book.get("EURUSD") is not None
        assert worker._pending_close_retry_position_id == position.position_id
        assert worker._pending_close_retry_reason == "emergency:drawdown"
        retry_at = worker._pending_close_retry_until_ts
        assert retry_at > now["ts"]

        events = worker.store.load_events(limit=50)
        deferred_events = [
            event for event in events if event.get("message") == "Position close deferred for retry"
        ]
        assert deferred_events
    finally:
        worker.broker.close()
        worker.store.close()


def test_close_position_defers_for_retry_on_market_closed_error(tmp_path, monkeypatch):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION, stream_event_cooldown_sec=0.0)
    try:
        position = Position(
            position_id="deal-market-closed-retry",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)

        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])
        calls = {"count": 0}

        def _close_rejected(_position):
            calls["count"] += 1
            raise BrokerError(
                'IG close deal rejected: MARKET_CLOSED_WITH_EDITS {"errorCode":"MARKET_CLOSED_WITH_EDITS"}'
            )

        worker.broker.close_position = _close_rejected  # type: ignore[assignment]

        worker._close_position(position, close_price=1.0990, reason="session_end_buffer:55s")
        assert calls["count"] == 1
        assert worker.position_book.get("EURUSD") is not None
        assert worker._pending_close_retry_position_id == position.position_id
        assert worker._pending_close_retry_reason == "session_end_buffer:55s"
        assert worker._pending_close_retry_until_ts > now["ts"] + 100.0

        events = worker.store.load_events(limit=50)
        deferred_events = [
            event for event in events if event.get("message") == "Position close deferred for retry"
        ]
        assert deferred_events
    finally:
        worker.broker.close()
        worker.store.close()


def test_pending_close_retry_executes_with_stored_reason_when_due(tmp_path, monkeypatch):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-pending-retry",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)

        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])

        worker._pending_close_retry_position_id = position.position_id
        worker._pending_close_retry_until_ts = now["ts"] - 0.01
        worker._pending_close_retry_reason = "early_loss_cut:hold_invalidation:no_ma_cross"

        captured: dict[str, object] = {}

        def _close_stub(pos, close_price, reason):
            captured["position_id"] = pos.position_id
            captured["close_price"] = close_price
            captured["reason"] = reason

        worker._close_position = _close_stub  # type: ignore[assignment]
        tick = PriceTick(symbol="EURUSD", bid=1.0992, ask=1.0994, timestamp=now["ts"])
        executed = worker._maybe_execute_pending_close_retry(position, tick)

        assert executed is True
        assert captured["position_id"] == position.position_id
        assert float(captured["close_price"]) == pytest.approx(1.0992)
        assert captured["reason"] == "early_loss_cut:hold_invalidation:no_ma_cross"
    finally:
        worker.broker.close()
        worker.store.close()


def test_handle_missing_broker_position_error_schedules_reconciliation_without_factual_sync(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-active-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)

        tick = PriceTick(symbol="EURUSD", bid=1.0988, ask=1.0990, timestamp=10.0)
        handled = worker._handle_missing_broker_position_error(
            'IG API PUT /positions/otc/deal-active-1 failed: 404 Not Found {"errorCode":"error.service.marketdata.position.details.null.error"}',
            tick,
        )

        assert handled is True
        assert worker.position_book.get("EURUSD") is not None
        assert worker._pending_missing_position_id == "deal-active-1"

        open_positions = worker.store.load_open_positions(mode="execution")
        assert "deal-active-1" in open_positions

        closed_events = [
            event for event in worker.store.load_events(limit=20) if event.get("message") == "Position closed"
        ]
        assert not closed_events
    finally:
        worker.broker.close()
        worker.store.close()


def test_close_position_uses_broker_close_sync_price_and_pnl(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-sync-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)

        worker.broker.close_position = lambda _position: None  # type: ignore[assignment]
        broker_closed_at = 950.0
        worker.broker.get_position_close_sync = lambda _position_id: {  # type: ignore[attr-defined]
            "source": "ig_confirm",
            "close_price": 1.1012,
            "realized_pnl": -62.05,
            "pnl_currency": "EUR",
            "closed_at": broker_closed_at,
        }

        with patch("xtb_bot.worker.time.time", return_value=1000.0):
            worker._close_position(position, close_price=1.1007, reason="take_profit")
        assert worker.position_book.get("EURUSD") is None

        events = [event for event in worker.store.load_events(limit=20) if event["message"] == "Position closed"]
        assert events
        payload = events[0].get("payload") or {}
        assert float(payload.get("close_price") or 0.0) == pytest.approx(1.1012)
        assert float(payload.get("pnl") or 0.0) == pytest.approx(-62.05)
        assert float(payload.get("local_pnl_estimate") or 0.0) != pytest.approx(-62.05)
        assert float(payload.get("broker_closed_at") or 0.0) == pytest.approx(broker_closed_at)
        broker_sync = payload.get("broker_close_sync")
        assert isinstance(broker_sync, dict)
        assert str(broker_sync.get("source") or "") == "ig_confirm"
        trade_record = worker.store.get_trade_record(position.position_id)
        assert trade_record is not None
        assert float(trade_record.get("closed_at") or 0.0) == pytest.approx(broker_closed_at)
    finally:
        worker.broker.close()
        worker.store.close()


def test_manual_broker_close_reconciliation_closes_active_execution_position(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-manual-close-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)
        worker.manual_close_sync_interval_sec = 5.0
        worker._manual_close_sync_next_check_ts = 0.0
        worker.broker.get_position_close_sync = lambda _position_id: {  # type: ignore[attr-defined]
            "source": "ig_history_transactions",
            "close_price": 1.0987,
            "realized_pnl": -71.41,
            "pnl_currency": "EUR",
            "history_reference": "UKUM6MA9",
        }

        tick = PriceTick(symbol="EURUSD", bid=1.1015, ask=1.1017, timestamp=10.0)
        with patch("xtb_bot.worker.time.time", return_value=1013.0):
            handled = worker._maybe_reconcile_execution_manual_close(position, tick)

        assert handled is True
        assert worker.position_book.get("EURUSD") is None
        events = [event for event in worker.store.load_events(limit=20) if event.get("message") == "Position closed"]
        assert events
        payload = events[0].get("payload") or {}
        assert float(payload.get("close_price") or 0.0) == pytest.approx(1.0987)
        assert float(payload.get("pnl") or 0.0) == pytest.approx(-71.41)
        assert str(payload.get("reason") or "").startswith("broker_manual_close:")
    finally:
        worker.broker.close()
        worker.store.close()


def test_manual_broker_close_reconciliation_persists_estimated_pnl_without_broker_amount(tmp_path):
    worker = _make_worker(tmp_path, symbol="UK100", mode=RunMode.EXECUTION)
    try:
        worker.symbol_spec = SymbolSpec(
            symbol="UK100",
            tick_size=1.0,
            tick_value=10.0,
            contract_size=10.0,
            lot_min=0.5,
            lot_max=100.0,
            lot_step=0.5,
            price_precision=1,
            lot_precision=1,
        )
        position = Position(
            position_id="deal-manual-close-gbp-1",
            symbol="UK100",
            side=Side.SELL,
            volume=0.5,
            open_price=10363.7,
            stop_loss=10395.0,
            take_profit=10297.0,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(
            position,
            worker.name,
            worker.strategy_name,
            worker.mode.value,
            strategy_entry="momentum",
        )
        worker.manual_close_sync_interval_sec = 5.0
        worker._manual_close_sync_next_check_ts = 0.0
        worker.broker.get_account_currency_code = lambda: "EUR"  # type: ignore[assignment]
        def _fx_price(symbol: str) -> PriceTick:
            if symbol == "EURGBP":
                return PriceTick(symbol=symbol, bid=0.8690, ask=0.8710, timestamp=10.0)
            raise BrokerError(f"unexpected conversion symbol {symbol}")

        worker.broker.get_price = _fx_price  # type: ignore[assignment]
        worker.broker.get_position_close_sync = lambda _position_id, **kwargs: {  # type: ignore[attr-defined]
            "source": "ig_history_activity",
            "close_price": 10395.0,
            "pnl_currency": "£",
            "history_reference": "UKMANUALGBP1",
        }

        tick = PriceTick(symbol="UK100", bid=10362.0, ask=10362.2, timestamp=10.0)
        with patch("xtb_bot.worker.time.time", return_value=1013.0):
            handled = worker._maybe_reconcile_execution_manual_close(position, tick)

        assert handled is True
        assert worker.position_book.get("UK100") is None

        trade_record = worker.store.get_trade_record(position.position_id)
        assert trade_record is not None
        assert float(trade_record.get("pnl") or 0.0) == pytest.approx(-156.5 / 0.87)
        assert bool(trade_record.get("pnl_is_estimated")) is True
        assert str(trade_record.get("pnl_estimate_source") or "") == "broker_close_price"
        assert str(trade_record.get("strategy_exit") or "") == "momentum"

        events = [event for event in worker.store.load_events(limit=20) if event.get("message") == "Position closed"]
        assert events
        payload = events[0].get("payload") or {}
        assert float(payload.get("pnl") or 0.0) == pytest.approx(-156.5 / 0.87)
        assert payload.get("pnl_pending_broker_sync") is True
        assert payload.get("pnl_is_estimated") is True
        assert str(payload.get("pnl_estimate_source") or "") == "broker_close_price"
        assert str(payload.get("pnl_currency") or "") == "GBP"
        normalization = payload.get("pnl_normalization")
        assert isinstance(normalization, dict)
        assert normalization.get("pnl_conversion_applied") is True
        assert float(normalization.get("pnl_native_amount") or 0.0) == pytest.approx(-156.5)
        assert float(payload.get("local_pnl_estimate") or 0.0) == pytest.approx(-156.5 / 0.87)
    finally:
        worker.broker.close()
        worker.store.close()


def test_finalize_close_does_not_persist_unconverted_foreign_currency_pnl(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-close-conversion-missing-1",
            symbol="UK100",
            side=Side.SELL,
            volume=0.5,
            open_price=10363.7,
            stop_loss=10395.0,
            take_profit=10297.0,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(
            position,
            worker.name,
            worker.strategy_name,
            worker.mode.value,
            strategy_entry="momentum",
        )
        worker.broker.get_account_currency_code = lambda: "EUR"  # type: ignore[assignment]

        def _missing_fx_price(symbol: str) -> PriceTick:
            raise BrokerError(f"unexpected conversion miss for {symbol}")

        worker.broker.get_price = _missing_fx_price  # type: ignore[assignment]

        with patch("xtb_bot.worker.time.time", return_value=1010.0):
            worker._finalize_position_close(
                position,
                close_price=10395.0,
                reason="take_profit",
                broker_sync={
                    "source": "ig_history_activity",
                    "close_price": 10395.0,
                    "realized_pnl": -156.5,
                    "pnl_currency": "GBP",
                    "closed_at": 1009.0,
                },
            )

        trade_record = worker.store.get_trade_record(position.position_id)
        assert trade_record is not None
        assert float(trade_record.get("pnl") or 0.0) == pytest.approx(0.0)
        assert bool(trade_record.get("pnl_is_estimated")) is True
        assert str(trade_record.get("pnl_estimate_source") or "") == "currency_conversion_missing"

        events = [event for event in worker.store.load_events(limit=20) if event.get("message") == "Position closed"]
        assert events
        payload = events[0].get("payload") or {}
        assert float(payload.get("pnl") or 0.0) == pytest.approx(0.0)
        assert payload.get("pnl_pending_currency_conversion") is True
        assert payload.get("pnl_is_estimated") is True
        assert str(payload.get("pnl_estimate_source") or "") == "currency_conversion_missing"
        assert str(payload.get("pnl_currency") or "") == "GBP"
        normalization = payload.get("pnl_normalization")
        assert isinstance(normalization, dict)
        assert normalization.get("pnl_conversion_applied") is False
        assert normalization.get("pnl_conversion_missing") is True
        assert float(normalization.get("pnl_native_amount") or 0.0) == pytest.approx(-156.5)
    finally:
        worker.broker.close()
        worker.store.close()


def test_manual_broker_close_reconciliation_ignores_partial_close_sync(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-manual-partial-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.5,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)
        worker.manual_close_sync_interval_sec = 5.0
        worker._manual_close_sync_next_check_ts = 0.0
        worker.broker.get_position_close_sync = lambda _position_id, **kwargs: {  # type: ignore[attr-defined]
            "source": "ig_confirm",
            "close_price": 1.1008,
            "realized_pnl": 4.2,
            "closed_volume": 0.2,
            "close_complete": False,
        }

        tick = PriceTick(symbol="EURUSD", bid=1.1015, ask=1.1017, timestamp=10.0)
        with patch("xtb_bot.worker.time.time", return_value=1013.0):
            handled = worker._maybe_reconcile_execution_manual_close(position, tick)

        assert handled is False
        assert worker.position_book.get("EURUSD") is not None
        closed_events = [
            event for event in worker.store.load_events(limit=30) if event.get("message") == "Position closed"
        ]
        assert not closed_events
        ignored_events = [
            event
            for event in worker.store.load_events(limit=30)
            if event.get("message") == "Ignoring partial broker close sync for active position"
        ]
        assert ignored_events
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_get_broker_close_sync_filters_kwargs_by_supported_signature(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-close-sync-filter-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)
        worker.store.bind_trade_deal_reference(position.position_id, "EXPECTEDREF123")
        captured: list[tuple[str, str | None, bool]] = []

        def _get_position_close_sync(
            position_id: str,
            *,
            deal_reference: str | None = None,
            include_history: bool = True,
        ) -> dict[str, object]:
            captured.append((position_id, deal_reference, include_history))
            return {
                "source": "ig_history_transactions",
                "close_price": 1.0987,
                "realized_pnl": -71.41,
                "pnl_currency": "EUR",
                "deal_reference": deal_reference,
                "history_match_mode": "deal_reference",
            }

        worker.broker.get_position_close_sync = _get_position_close_sync  # type: ignore[attr-defined]

        payload = worker._get_broker_close_sync(position, include_history=False)

        assert isinstance(payload, dict)
        assert captured == [(position.position_id, "EXPECTEDREF123", False)]
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_get_broker_close_sync_does_not_mask_internal_type_error(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-close-sync-typeerror-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)
        call_count = 0

        def _get_position_close_sync(
            position_id: str,
            *,
            deal_reference: str | None = None,
            include_history: bool = True,
        ) -> dict[str, object]:
            nonlocal call_count
            _ = include_history
            call_count += 1
            raise TypeError(f"internal close sync failure for {position_id} / {deal_reference}")

        worker.broker.get_position_close_sync = _get_position_close_sync  # type: ignore[attr-defined]

        payload = worker._get_broker_close_sync(position, include_history=False)

        assert payload is None
        assert call_count == 1
        warning_events = [
            event
            for event in worker.store.load_events(limit=20)
            if event.get("message") == "Broker close sync fetch failed"
        ]
        assert warning_events
        warning_payload = warning_events[0].get("payload") or {}
        assert "internal close sync failure" in str(warning_payload.get("error") or "")
    finally:
        worker.broker.close()
        worker.store.close()


def test_handle_active_position_without_fresh_tick_runs_manual_reconcile(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-stale-tick-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)

        worker._attempt_pending_missing_position_reconciliation = lambda _position, _tick: False  # type: ignore[method-assign]

        manual_calls: list[str] = []

        def _manual_reconcile(_position, tick):
            assert tick is None
            manual_calls.append(_position.position_id)
            worker.position_book.remove_by_id(_position.position_id)
            return True

        worker._maybe_reconcile_execution_manual_close = _manual_reconcile  # type: ignore[method-assign]
        worker._handle_active_position_without_fresh_tick(
            position,
            "DB-first tick cache is empty or stale for EURUSD",
        )

        assert manual_calls == [position.position_id]
        assert worker.position_book.get("EURUSD") is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_manual_broker_close_reconciliation_closes_when_position_missing_without_details(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-manual-close-pending-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)
        worker.manual_close_sync_interval_sec = 5.0
        worker._manual_close_sync_next_check_ts = 0.0
        worker.broker.get_position_close_sync = lambda _position_id, **kwargs: {  # type: ignore[attr-defined]
            "source": "ig_positions_deal_id",
            "position_found": False,
        }

        tick = PriceTick(symbol="EURUSD", bid=1.1015, ask=1.1017, timestamp=10.0)
        with patch("xtb_bot.worker.time.time", return_value=1013.0):
            handled = worker._maybe_reconcile_execution_manual_close(position, tick)

        assert handled is True
        assert worker.position_book.get("EURUSD") is None
        events = [event for event in worker.store.load_events(limit=20) if event.get("message") == "Position closed"]
        assert events
        payload = events[0].get("payload") or {}
        assert payload.get("close_price") is None
        assert bool(payload.get("close_price_pending_broker_sync")) is True
        assert float(payload.get("pnl") or 0.0) != pytest.approx(0.0)
        assert payload.get("pnl_is_estimated") is True
        assert str(payload.get("pnl_estimate_source") or "") == "manual_reconcile_inferred_close_price"
        assert str(payload.get("reason") or "").startswith("broker_manual_close:")
        assert "pending_broker_sync" in str(payload.get("reason") or "")
        trade_record = worker.store.get_trade_record(position.position_id)
        assert trade_record is not None
        assert bool(trade_record.get("pnl_is_estimated")) is True
        assert str(trade_record.get("pnl_estimate_source") or "") == "manual_reconcile_inferred_close_price"
    finally:
        worker.broker.close()
        worker.store.close()


def test_manual_broker_close_reconciliation_escalates_to_history_before_pending(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-manual-close-history-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)
        worker.manual_close_sync_interval_sec = 5.0
        worker._manual_close_sync_next_check_ts = 0.0

        calls: list[bool] = []

        def _get_position_close_sync(_position_id, **kwargs):
            include_history = bool(kwargs.get("include_history"))
            calls.append(include_history)
            if include_history:
                return {
                    "source": "ig_history_transactions",
                    "close_price": 1.0986,
                    "realized_pnl": -73.0,
                    "closed_at": 1010.0,
                    "history_reference": "HIST-REF-1",
                }
            return {
                "source": "ig_positions_deal_id",
                "position_found": False,
            }

        worker.broker.get_position_close_sync = _get_position_close_sync  # type: ignore[attr-defined]

        tick = PriceTick(symbol="EURUSD", bid=1.1015, ask=1.1017, timestamp=10.0)
        with patch("xtb_bot.worker.time.time", return_value=1013.0):
            handled = worker._maybe_reconcile_execution_manual_close(position, tick)

        assert handled is True
        assert calls == [False, True]
        trade_record = worker.store.get_trade_record(position.position_id)
        assert trade_record is not None
        assert float(trade_record.get("close_price") or 0.0) == pytest.approx(1.0986)
        assert float(trade_record.get("pnl") or 0.0) == pytest.approx(-73.0)
        assert float(trade_record.get("closed_at") or 0.0) == pytest.approx(1010.0)
        closed_events = [event for event in worker.store.load_events(limit=20) if event.get("message") == "Position closed"]
        assert closed_events
        payload = closed_events[0].get("payload") or {}
        assert payload.get("close_price_pending_broker_sync") in {False, None}
    finally:
        worker.broker.close()
        worker.store.close()


def test_manual_broker_close_reconciliation_ignores_stale_history_position_hint_sync(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-manual-close-stale-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1000.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)
        worker.manual_close_sync_interval_sec = 5.0
        worker._manual_close_sync_next_check_ts = 0.0
        worker.broker.get_position_close_sync = lambda _position_id: {  # type: ignore[attr-defined]
            "source": "ig_history_transactions",
            "history_match_mode": "position_hint",
            "close_price": 1.0987,
            "realized_pnl": -71.41,
            "closed_at": 900.0,
            "history_reference": "STALE-REF-1",
        }

        tick = PriceTick(symbol="EURUSD", bid=1.1015, ask=1.1017, timestamp=10.0)
        with patch("xtb_bot.worker.time.time", return_value=1013.0):
            handled = worker._maybe_reconcile_execution_manual_close(position, tick)

        assert handled is False
        assert worker.position_book.get("EURUSD") is not None
        closed_events = [event for event in worker.store.load_events(limit=20) if event.get("message") == "Position closed"]
        assert not closed_events
        stale_events = [
            event
            for event in worker.store.load_events(limit=20)
            if event.get("message") == "Broker close sync rejected as stale"
        ]
        assert stale_events
    finally:
        worker.broker.close()
        worker.store.close()


def test_manual_broker_close_reconciliation_rejects_reference_mismatch(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-manual-close-ref-mismatch-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1000.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)
        worker.store.bind_trade_deal_reference(position.position_id, "EXPECTEDREF123")
        worker.manual_close_sync_interval_sec = 5.0
        worker._manual_close_sync_next_check_ts = 0.0
        worker.broker.get_position_close_sync = lambda _position_id: {  # type: ignore[attr-defined]
            "source": "ig_history_transactions",
            "history_match_mode": "deal_reference",
            "close_price": 1.0987,
            "realized_pnl": -71.41,
            "closed_at": 1005.0,
            "history_reference": "OTHERREF999",
        }

        tick = PriceTick(symbol="EURUSD", bid=1.1015, ask=1.1017, timestamp=10.0)
        with patch("xtb_bot.worker.time.time", return_value=1013.0):
            handled = worker._maybe_reconcile_execution_manual_close(position, tick)

        assert handled is False
        assert worker.position_book.get("EURUSD") is not None
        closed_events = [event for event in worker.store.load_events(limit=20) if event.get("message") == "Position closed"]
        assert not closed_events
        mismatch_events = [
            event
            for event in worker.store.load_events(limit=20)
            if event.get("message") == "Broker close sync rejected by deal reference mismatch"
        ]
        assert mismatch_events
    finally:
        worker.broker.close()
        worker.store.close()


def test_manual_broker_close_reconciliation_accepts_position_id_history_match_without_reference(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-manual-close-position-id-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1000.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)
        worker.store.bind_trade_deal_reference(position.position_id, "EXPECTEDREF123")
        worker.manual_close_sync_interval_sec = 5.0
        worker._manual_close_sync_next_check_ts = 0.0
        worker.broker.get_position_close_sync = lambda _position_id: {  # type: ignore[attr-defined]
            "source": "ig_history_transactions",
            "history_match_mode": "position_id",
            "close_price": 1.0987,
            "realized_pnl": -71.41,
            "closed_at": 1005.0,
            "history_reference": "OTHERREF999",
        }

        tick = PriceTick(symbol="EURUSD", bid=1.1015, ask=1.1017, timestamp=10.0)
        with patch("xtb_bot.worker.time.time", return_value=1013.0):
            handled = worker._maybe_reconcile_execution_manual_close(position, tick)

        assert handled is True
        assert worker.position_book.get("EURUSD") is None
        closed_events = [event for event in worker.store.load_events(limit=20) if event.get("message") == "Position closed"]
        assert closed_events
        mismatch_events = [
            event
            for event in worker.store.load_events(limit=20)
            if event.get("message") == "Broker close sync rejected by deal reference mismatch"
        ]
        assert not mismatch_events
    finally:
        worker.broker.close()
        worker.store.close()


def test_manual_broker_close_reconciliation_accepts_ig_confirm_with_different_close_reference(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-manual-close-confirm-ref-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1000.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)
        worker.store.bind_trade_deal_reference(position.position_id, "OPENREF123")
        worker.manual_close_sync_interval_sec = 5.0
        worker._manual_close_sync_next_check_ts = 0.0
        worker.broker.get_position_close_sync = lambda _position_id: {  # type: ignore[attr-defined]
            "source": "ig_confirm",
            "deal_reference": "CLOSEREF999",
            "close_price": 1.0987,
            "realized_pnl": -71.41,
            "closed_at": 1005.0,
        }

        tick = PriceTick(symbol="EURUSD", bid=1.1015, ask=1.1017, timestamp=10.0)
        with patch("xtb_bot.worker.time.time", return_value=1013.0):
            handled = worker._maybe_reconcile_execution_manual_close(position, tick)

        assert handled is True
        assert worker.position_book.get("EURUSD") is None
        closed_events = [event for event in worker.store.load_events(limit=20) if event.get("message") == "Position closed"]
        assert closed_events
        payload = closed_events[0].get("payload") or {}
        assert float(payload.get("pnl") or 0.0) == pytest.approx(-71.41)
        mismatch_events = [
            event
            for event in worker.store.load_events(limit=20)
            if event.get("message") == "Broker close sync rejected by deal reference mismatch"
        ]
        assert not mismatch_events
    finally:
        worker.broker.close()
        worker.store.close()


def test_manual_broker_close_reconciliation_rejects_conflicting_close_price_sign(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-manual-close-sign-mismatch-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1000.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)
        worker.manual_close_sync_interval_sec = 5.0
        worker._manual_close_sync_next_check_ts = 0.0
        worker.broker.get_position_close_sync = lambda _position_id: {  # type: ignore[attr-defined]
            "source": "ig_history_activity",
            "close_price": 1.0980,  # local pnl would be negative for BUY
            "realized_pnl": 50.0,  # broker pnl is positive -> mismatch
            "closed_at": 1005.0,
            "deal_reference": "TESTREF123",
        }

        tick = PriceTick(symbol="EURUSD", bid=1.1010, ask=1.1012, timestamp=10.0)
        with patch("xtb_bot.worker.time.time", return_value=1013.0):
            handled = worker._maybe_reconcile_execution_manual_close(position, tick)

        assert handled is True
        assert worker.position_book.get("EURUSD") is None
        trade_record = worker.store.get_trade_record(position.position_id)
        assert trade_record is not None
        assert trade_record.get("close_price") is None
        assert float(trade_record.get("pnl") or 0.0) == pytest.approx(50.0)

        mismatch_events = [
            event
            for event in worker.store.load_events(limit=30)
            if event.get("message") == "Broker close sync close_price rejected by pnl sign mismatch"
        ]
        assert mismatch_events
    finally:
        worker.broker.close()
        worker.store.close()


def test_missing_broker_position_reconcile_prefers_broker_sync_payload(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-sync-missing-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)
        worker.broker.get_position_close_sync = lambda _position_id: {  # type: ignore[attr-defined]
            "source": "ig_history_transactions",
            "close_price": 1.0987,
            "realized_pnl": -71.41,
            "pnl_currency": "EUR",
            "history_reference": "UKUM6MA9",
        }

        tick = PriceTick(symbol="EURUSD", bid=1.1015, ask=1.1017, timestamp=10.0)
        handled = worker._handle_missing_broker_position_error(
            'IG API PUT /positions/otc/deal-sync-missing-1 failed: 404 Not Found {"errorCode":"error.service.marketdata.position.details.null.error"}',
            tick,
        )

        assert handled is True
        events = [event for event in worker.store.load_events(limit=20) if event.get("message") == "Position closed"]
        assert events
        payload = events[0].get("payload") or {}
        assert float(payload.get("close_price") or 0.0) == pytest.approx(1.0987)
        assert float(payload.get("pnl") or 0.0) == pytest.approx(-71.41)
        assert "ig_history_transactions" in str(payload.get("reason") or "")
    finally:
        worker.broker.close()
        worker.store.close()


def test_missing_broker_position_reconciliation_is_delayed_during_allowance_backoff(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-sync-delay-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)
        worker.broker.get_position_close_sync = lambda _position_id: None  # type: ignore[attr-defined]
        worker.broker.get_public_api_backoff_remaining_sec = lambda: 12.5  # type: ignore[attr-defined]

        tick = PriceTick(symbol="EURUSD", bid=1.1005, ask=1.1007, timestamp=10.0)
        with patch("xtb_bot.worker.time.time", return_value=1000.0):
            handled = worker._handle_missing_broker_position_error(
                'IG API PUT /positions/otc/deal-sync-delay-1 failed: 404 Not Found {"errorCode":"error.service.marketdata.position.details.null.error"}',
                tick,
            )

        assert handled is True
        assert worker.position_book.get("EURUSD") is not None
        assert worker._pending_missing_position_id == "deal-sync-delay-1"
        assert worker._pending_missing_position_until_ts == pytest.approx(1012.5)
        assert worker._pending_missing_position_deadline_ts == pytest.approx(
            1000.0 + worker.missing_position_reconcile_timeout_sec
        )

        closed_events = [
            event for event in worker.store.load_events(limit=20) if event.get("message") == "Position closed"
        ]
        assert not closed_events
        delayed_events = [
            event
            for event in worker.store.load_events(limit=20)
            if event.get("message") == "Broker position missing reconciliation delayed"
        ]
        assert delayed_events
    finally:
        worker.broker.close()
        worker.store.close()


def test_missing_broker_position_reconciliation_is_delayed_on_mark_even_without_backoff(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-sync-delay-mark-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)
        worker.broker.get_position_close_sync = lambda _position_id: None  # type: ignore[attr-defined]
        worker.broker.get_public_api_backoff_remaining_sec = lambda: 0.0  # type: ignore[attr-defined]
        worker.manual_close_sync_interval_sec = 20.0

        # Mark is between SL/TP, so close reason is ambiguous until broker sync appears.
        tick = PriceTick(symbol="EURUSD", bid=1.1004, ask=1.1006, timestamp=10.0)
        with patch("xtb_bot.worker.time.time", return_value=1000.0):
            handled = worker._handle_missing_broker_position_error(
                'IG API PUT /positions/otc/deal-sync-delay-mark-1 failed: 404 Not Found {"errorCode":"error.service.marketdata.position.details.null.error"}',
                tick,
            )

        assert handled is True
        assert worker.position_book.get("EURUSD") is not None
        assert worker._pending_missing_position_id == "deal-sync-delay-mark-1"
        assert worker._pending_missing_position_until_ts == pytest.approx(1005.0)
        assert worker._pending_missing_position_deadline_ts == pytest.approx(
            1000.0 + worker.missing_position_reconcile_timeout_sec
        )
        closed_events = [
            event for event in worker.store.load_events(limit=20) if event.get("message") == "Position closed"
        ]
        assert not closed_events
    finally:
        worker.broker.close()
        worker.store.close()


def test_pending_missing_position_reconciliation_waits_out_extended_allowance_backoff(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-sync-delay-2",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)
        worker._pending_missing_position_id = position.position_id
        worker._pending_missing_position_until_ts = 1012.5
        worker._pending_missing_position_deadline_ts = 1100.0
        worker.broker.get_position_close_sync = lambda _position_id: None  # type: ignore[attr-defined]
        worker.broker.get_public_api_backoff_remaining_sec = lambda: 10.0  # type: ignore[attr-defined]

        tick = PriceTick(symbol="EURUSD", bid=1.1005, ask=1.1007, timestamp=10.0)
        with patch("xtb_bot.worker.time.time", return_value=1013.0):
            handled = worker._attempt_pending_missing_position_reconciliation(position, tick)

        assert handled is True
        assert worker.position_book.get("EURUSD") is not None
        assert worker._pending_missing_position_id == position.position_id
        assert worker._pending_missing_position_until_ts == pytest.approx(1023.0)

        closed_events = [
            event for event in worker.store.load_events(limit=20) if event.get("message") == "Position closed"
        ]
        assert not closed_events
    finally:
        worker.broker.close()
        worker.store.close()


def test_pending_missing_position_reconciliation_uses_broker_sync_after_backoff(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-sync-delay-3",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.23,
            open_price=10051.9,
            stop_loss=10021.9,
            take_profit=10126.9,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)
        worker._pending_missing_position_id = position.position_id
        worker._pending_missing_position_until_ts = 1012.5
        worker._pending_missing_position_deadline_ts = 1100.0
        worker.broker.get_position_close_sync = lambda _position_id: {  # type: ignore[attr-defined]
            "source": "ig_history_transactions",
            "close_price": 10021.9,
            "realized_pnl": -47.47,
            "pnl_currency": "EUR",
            "history_reference": "VPK3DMA9",
        }
        worker.broker.get_public_api_backoff_remaining_sec = lambda: 0.0  # type: ignore[attr-defined]

        tick = PriceTick(symbol="EURUSD", bid=10059.0, ask=10059.2, timestamp=10.0)
        with patch("xtb_bot.worker.time.time", return_value=1013.0):
            handled = worker._attempt_pending_missing_position_reconciliation(position, tick)

        assert handled is True
        assert worker.position_book.get("EURUSD") is None
        closed_events = [
            event for event in worker.store.load_events(limit=20) if event.get("message") == "Position closed"
        ]
        assert closed_events
        payload = closed_events[0].get("payload") or {}
        assert float(payload.get("close_price") or 0.0) == pytest.approx(10021.9)
        assert float(payload.get("pnl") or 0.0) == pytest.approx(-47.47)
        assert "ig_history_transactions" in str(payload.get("reason") or "")
    finally:
        worker.broker.close()
        worker.store.close()


def test_pending_missing_position_reconciliation_times_out_without_broker_sync(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-sync-delay-4",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)
        worker._pending_missing_position_id = position.position_id
        worker._pending_missing_position_until_ts = 1012.5
        worker._pending_missing_position_deadline_ts = 1015.0
        worker.broker.get_position_close_sync = lambda _position_id: None  # type: ignore[attr-defined]
        worker.broker.get_public_api_backoff_remaining_sec = lambda: 0.0  # type: ignore[attr-defined]

        tick = PriceTick(symbol="EURUSD", bid=1.1005, ask=1.1007, timestamp=10.0)
        with patch("xtb_bot.worker.time.time", return_value=1016.0):
            handled = worker._attempt_pending_missing_position_reconciliation(position, tick)

        assert handled is True
        assert worker.position_book.get("EURUSD") is None
        closed_events = [
            event for event in worker.store.load_events(limit=20) if event.get("message") == "Position closed"
        ]
        assert closed_events
        payload = closed_events[0].get("payload") or {}
        assert str(payload.get("reason") or "") == "broker_position_missing:timeout:pending_broker_sync"
        assert payload.get("close_price") is None
        assert bool(payload.get("close_price_pending_broker_sync")) is True
    finally:
        worker.broker.close()
        worker.store.close()


def test_entry_tick_age_blocks_open_on_stale_tick(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        strategy_param_overrides={"momentum_entry_tick_max_age_sec": 5.0},
        hold_reason_log_interval_sec=1.0,
    )
    try:
        now = {"ts": 200.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])

        assert worker._entry_tick_age_allows_open(197.0) is True
        assert worker._entry_tick_age_allows_open(190.0) is False

        stale_events = [
            event
            for event in worker.store.load_events(limit=20)
            if event.get("message") == "Trade blocked by stale tick age"
        ]
        assert stale_events
    finally:
        worker.broker.close()
        worker.store.close()


def test_entry_tick_age_uses_received_at_when_available(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        strategy_param_overrides={"momentum_entry_tick_max_age_sec": 5.0},
        hold_reason_log_interval_sec=1.0,
    )
    try:
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: 200.0)
        assert worker._entry_tick_age_allows_open(
            150.0,
            freshness_timestamp_sec=198.0,
        ) is True
    finally:
        worker.broker.close()
        worker.store.close()


def test_refresh_stale_entry_candidate_uses_direct_broker_fallback(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        db_first_reads_enabled=True,
        db_first_tick_max_age_sec=120.0,
        strategy_param_overrides={"entry_tick_max_age_sec": 10.0},
    )
    try:
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: 200.0)
        worker.prices.append(1.1001)
        worker.price_timestamps.append(150.0)
        worker.volumes.append(0.0)
        worker.spreads_pips.append(2.0)

        fallback_tick = PriceTick(
            symbol="EURUSD",
            bid=1.1010,
            ask=1.1012,
            timestamp=150.0,
            received_at=200.0,
        )
        monkeypatch.setattr(worker, "_load_direct_tick_for_entry_fallback", lambda: fallback_tick)

        refreshed = worker._refresh_stale_entry_candidate(
            PriceTick(
                symbol="EURUSD",
                bid=1.1000,
                ask=1.1002,
                timestamp=150.0,
                received_at=150.0,
            ),
            raw_tick_timestamp_sec=150.0,
            current_spread_pips=2.0,
            current_spread_pct=0.01818,
            current_volume=None,
        )

        assert refreshed is not None
        (
            refreshed_tick,
            refreshed_raw_ts,
            refreshed_freshness_ts,
            refreshed_freshness_monotonic_ts,
            refreshed_spread_pips,
            _refreshed_spread_pct,
            _refreshed_volume,
        ) = refreshed
        assert refreshed_tick.bid == pytest.approx(1.1010)
        assert refreshed_raw_ts == pytest.approx(150.0)
        assert refreshed_freshness_ts == pytest.approx(200.0)
        assert refreshed_freshness_monotonic_ts == pytest.approx(200.0)
        assert refreshed_spread_pips > 0
        assert worker.prices[-1] == pytest.approx(refreshed_tick.mid)
        assert worker.price_timestamps[-1] == pytest.approx(200.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_run_uses_stream_only_entry_fallback_when_db_first_tick_cache_is_empty(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        db_first_reads_enabled=True,
        db_first_tick_max_age_sec=120.0,
        poll_interval_sec=0.05,
    )
    thread: threading.Thread | None = None
    try:
        worker.strategy = _FakeStrategy(Side.HOLD)
        monkeypatch.setattr(worker, "_load_symbol_spec", lambda: None)
        monkeypatch.setattr(worker, "_load_db_first_tick", lambda: None)
        fallback_calls: list[dict[str, object]] = []
        fallback_tick = PriceTick(
            symbol="EURUSD",
            bid=1.1010,
            ask=1.1012,
            timestamp=1_700_000_010.0,
            received_at=1_700_000_011.0,
        )

        def _fallback(*, allow_rest_fallback: bool = True):
            fallback_calls.append({"allow_rest_fallback": allow_rest_fallback})
            worker.stop_event.set()
            return fallback_tick

        monkeypatch.setattr(worker, "_load_direct_tick_for_entry_fallback", _fallback)

        thread = threading.Thread(target=worker.run, daemon=True)
        thread.start()
        thread.join(timeout=1.0)

        assert fallback_calls
        assert fallback_calls[0]["allow_rest_fallback"] is False
        assert worker.prices
        assert worker.prices[-1] == pytest.approx(fallback_tick.mid)
    finally:
        worker.stop_event.set()
        if thread is not None and thread.is_alive():
            thread.join(timeout=1.0)
        worker.broker.close()
        worker.store.close()


def test_db_first_entry_tick_fallback_returns_none_when_stream_has_no_quote(tmp_path):
    worker = _make_worker(
        tmp_path,
        db_first_reads_enabled=True,
        db_first_tick_max_age_sec=120.0,
    )
    try:
        worker.broker.get_price_stream_only = lambda symbol, wait_timeout_sec=0.0: None  # type: ignore[assignment]

        assert worker._load_direct_tick_for_entry_fallback(allow_rest_fallback=False) is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_stale_entry_history_blocks_open_until_recent_samples_rebuild(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        db_first_reads_enabled=True,
        strategy_param_overrides={"momentum_entry_tick_max_age_sec": 10.0},
        hold_reason_log_interval_sec=1.0,
    )
    try:
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: 200.0)
        worker.price_timestamps.extend([20.0, 40.0, 200.0])
        signal = Signal(
            side=Side.BUY,
            confidence=0.9,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"indicator": "g1", "timeframe_sec": 60.0},
        )

        assert worker._stale_entry_history_allows_open(signal) is False

        blocked_events = [
            event
            for event in worker.store.load_events(limit=20)
            if event.get("message") == "Trade blocked by stale entry history"
        ]
        assert blocked_events
        payload = blocked_events[0].get("payload") or {}
        assert float(payload.get("last_gap_sec") or 0.0) > 100.0
        assert int(payload.get("recent_samples") or 0) == 1
    finally:
        worker.broker.close()
        worker.store.close()


def test_stale_entry_history_allows_open_after_recent_ticks_rebuild(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        db_first_reads_enabled=True,
        strategy_param_overrides={"momentum_entry_tick_max_age_sec": 10.0},
    )
    try:
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: 200.0)
        worker.price_timestamps.extend([150.0, 170.0, 195.0, 200.0])
        signal = Signal(
            side=Side.BUY,
            confidence=0.9,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"indicator": "g1", "timeframe_sec": 60.0},
        )

        assert worker._stale_entry_history_allows_open(signal) is True
    finally:
        worker.broker.close()
        worker.store.close()


def test_stale_entry_history_uses_atomic_history_snapshot(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        db_first_reads_enabled=True,
        strategy_param_overrides={"momentum_entry_tick_max_age_sec": 10.0},
    )
    try:
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: 200.0)
        monkeypatch.setattr(worker._history_runtime, "timestamp_snapshot", lambda: [150.0, 170.0, 195.0, 200.0])
        worker.price_timestamps = _ExplodingDeque([10.0, 20.0, 30.0])
        signal = Signal(
            side=Side.BUY,
            confidence=0.9,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"indicator": "g1", "timeframe_sec": 60.0},
        )

        assert worker._stale_entry_history_allows_open(signal) is True
    finally:
        worker.broker.close()
        worker.store.close()


def test_entry_tick_age_default_scales_with_db_first_tick_age_limit(tmp_path):
    worker = _make_worker(
        tmp_path,
        db_first_reads_enabled=True,
        db_first_tick_max_age_sec=115.0,
    )
    try:
        assert worker.entry_tick_max_age_sec == pytest.approx(57.5)
    finally:
        worker.broker.close()
        worker.store.close()


def test_entry_tick_age_global_risk_override_applies_to_all_strategies(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="g1",
        db_first_reads_enabled=True,
        db_first_tick_max_age_sec=115.0,
        risk_entry_tick_max_age_sec=20.0,
    )
    try:
        assert worker.entry_tick_max_age_sec == pytest.approx(20.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_moves_stop_to_breakeven_at_halfway_to_tp(tmp_path):
    worker = _make_worker(
        tmp_path,
        trailing_activation_ratio=0.5,
        trailing_distance_pips=10.0,
        strategy_param_overrides={"protective_runner_preservation_enabled": False},
    )
    try:
        position = Position(
            position_id="paper-2",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )

        new_sl, progress = worker._trailing_candidate_stop(position, bid=1.1010, ask=1.1012)
        assert progress == pytest.approx(0.5)
        assert new_sl == pytest.approx(1.1000)
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_follows_price_after_breakeven(tmp_path):
    worker = _make_worker(tmp_path, trailing_activation_ratio=0.5, trailing_distance_pips=10.0)
    try:
        position = Position(
            position_id="paper-3",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.1000,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )

        new_sl, progress = worker._trailing_candidate_stop(position, bid=1.1015, ask=1.1017)
        assert progress > 0.5
        assert new_sl == pytest.approx(1.1005)
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_does_not_activate_while_position_is_in_loss_even_if_ratio_zero(tmp_path):
    worker = _make_worker(tmp_path, trailing_activation_ratio=0.0, trailing_distance_pips=10.0)
    try:
        position = Position(
            position_id="paper-3a",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )

        new_sl, progress = worker._trailing_candidate_stop(position, bid=1.0998, ask=1.1000)
        assert progress < 0.0
        assert new_sl is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_activates_on_first_profit_when_ratio_zero(tmp_path):
    worker = _make_worker(
        tmp_path,
        trailing_activation_ratio=0.0,
        trailing_distance_pips=10.0,
        strategy_param_overrides={"protective_runner_preservation_enabled": False},
    )
    try:
        position = Position(
            position_id="paper-3b",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )

        new_sl, progress = worker._trailing_candidate_stop(position, bid=1.1001, ask=1.1003)
        assert progress > 0.0
        assert new_sl == pytest.approx(1.1000)
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_candidate_stop_runner_trend_waits_for_min_tp_progress(tmp_path):
    worker = _make_worker(
        tmp_path,
        trailing_activation_ratio=0.0,
        trailing_distance_pips=10.0,
        strategy_param_overrides={
            "protective_runner_distance_min_tp_progress": 0.55,
            "protective_breakeven_lock_enabled": False,
        },
    )
    try:
        position = Position(
            position_id="paper-trend-runner-trail",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker._set_position_trailing_override(
            position.position_id,
            {
                "adaptive_trailing_family": "trend",
                "adaptive_trailing_initial_stop_pips": 10.0,
            },
        )

        new_sl_early, progress_early = worker._trailing_candidate_stop(position, bid=1.1010, ask=1.1012)
        assert progress_early == pytest.approx(0.5)
        assert new_sl_early is None

        new_sl_late, progress_late = worker._trailing_candidate_stop(position, bid=1.1011, ask=1.1013)
        assert progress_late == pytest.approx(0.55)
        assert new_sl_late == pytest.approx(1.1001)
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_candidate_stop_adaptive_trend_distance_waits_for_runner_distance_gate(tmp_path):
    worker = _make_worker(
        tmp_path,
        trailing_activation_ratio=0.0,
        trailing_distance_pips=10.0,
        adaptive_trailing_enabled=True,
        adaptive_trailing_heat_trigger=0.30,
        adaptive_trailing_heat_full=0.80,
        adaptive_trailing_distance_factor_at_full=0.50,
        adaptive_trailing_profit_lock_r_at_full=0.0,
        strategy_param_overrides={
            "protective_runner_distance_min_tp_progress": 0.55,
            "protective_breakeven_lock_enabled": False,
        },
    )
    try:
        position = Position(
            position_id="paper-adaptive-trend-runner-gate-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker._set_position_trailing_override(
            position.position_id,
            {
                "adaptive_trailing_family": "trend",
                "adaptive_trailing_initial_stop_pips": 10.0,
            },
        )
        signal = Signal(
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "indicator": "mean_breakout_v2",
                "atr_pips": 12.0,
                "zscore": 3.2,
                "zscore_effective_threshold": 1.6,
            },
        )
        new_sl, progress = worker._trailing_candidate_stop(
            position,
            bid=1.1010,
            ask=1.1012,
            signal=signal,
        )
        assert progress == pytest.approx(0.5)
        assert new_sl is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_candidate_applies_protective_breakeven_lock_before_activation(tmp_path):
    worker = _make_worker(
        tmp_path,
        trailing_activation_ratio=0.9,
        trailing_distance_pips=10.0,
        strategy_name="momentum",
        strategy_param_overrides={
            "momentum_protective_breakeven_lock_enabled": True,
            "momentum_protective_breakeven_min_peak_pips": 6.0,
            "protective_breakeven_min_tp_progress": 0.0,
            "momentum_protective_breakeven_offset_pips": 1.0,
            "protective_runner_preservation_enabled": False,
        },
    )
    try:
        position = Position(
            position_id="paper-breakeven-lock",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1100,
            opened_at=1.0,
            status="open",
        )
        worker._position_peak_favorable_pips[position.position_id] = 12.0

        new_sl, progress = worker._trailing_candidate_stop(position, bid=1.1003, ask=1.1005)
        assert progress < 0.9
        assert new_sl == pytest.approx(1.1001)
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_candidate_breakeven_lock_trend_waits_for_min_tp_progress(tmp_path):
    worker = _make_worker(
        tmp_path,
        trailing_activation_ratio=0.9,
        trailing_distance_pips=10.0,
        strategy_name="momentum",
        strategy_param_overrides={
            "momentum_protective_breakeven_lock_enabled": True,
            "momentum_protective_breakeven_min_peak_pips": 4.0,
            "momentum_protective_breakeven_min_tp_progress": 0.55,
            "momentum_protective_breakeven_offset_pips": 0.0,
            "protective_runner_preservation_enabled": False,
        },
    )
    try:
        position = Position(
            position_id="paper-breakeven-trend-progress",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker._position_peak_favorable_pips[position.position_id] = 10.0

        new_sl_early, progress_early = worker._trailing_candidate_stop(position, bid=1.1003, ask=1.1005)
        assert progress_early < 0.55
        assert new_sl_early is None

        worker._position_peak_favorable_pips[position.position_id] = 12.0
        new_sl_late, progress_late = worker._trailing_candidate_stop(position, bid=1.1003, ask=1.1005)
        assert progress_late < 0.55
        assert new_sl_late == pytest.approx(1.1000)
    finally:
        worker.broker.close()
        worker.store.close()


def test_index_trend_breakeven_lock_arms_from_initial_stop_before_tp_progress(tmp_path):
    worker = _make_worker(
        tmp_path,
        symbol="US100",
        trailing_activation_ratio=0.9,
        trailing_distance_pips=10.0,
        strategy_name="momentum",
        strategy_param_overrides={
            "momentum_protective_breakeven_lock_enabled": True,
            "momentum_protective_breakeven_min_peak_pips": 4.0,
            "momentum_protective_breakeven_min_tp_progress": 0.55,
            "momentum_protective_breakeven_offset_pips": 0.0,
            "protective_runner_preservation_enabled": False,
        },
    )
    try:
        position = Position(
            position_id="paper-index-trend-breakeven-progress",
            symbol="US100",
            side=Side.BUY,
            volume=0.5,
            open_price=25000.0,
            stop_loss=24990.0,
            take_profit=25040.0,
            opened_at=1.0,
            status="open",
        )
        worker._position_entry_component_by_id[position.position_id] = "index_hybrid"
        worker._position_entry_signal_by_id[position.position_id] = "index_hybrid:trend_following"
        worker._position_peak_favorable_pips[position.position_id] = 6.0

        new_sl, progress = worker._trailing_candidate_stop(position, bid=25001.0, ask=25002.0)
        assert progress < 0.55
        assert new_sl == pytest.approx(25000.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_partial_take_profit_skips_mature_trend_runner(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "partial_take_profit_enabled": True,
            "partial_take_profit_r_multiple": 1.4,
            "partial_take_profit_fraction": 0.25,
            "partial_take_profit_skip_trend_positions": False,
            "partial_take_profit_skip_runner_positions": True,
            "protective_runner_preservation_enabled": True,
            "protective_runner_min_tp_progress": 0.35,
            "protective_runner_min_peak_pips": 8.0,
            "protective_runner_min_retain_ratio": 0.45,
        },
    )
    try:
        position = Position(
            position_id="paper-runner-partial-skip",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.40,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker._set_position_trailing_override(
            position.position_id,
            {
                "adaptive_trailing_family": "trend",
                "adaptive_trailing_initial_stop_pips": 10.0,
            },
        )

        applied = worker._maybe_apply_partial_take_profit(position, bid=1.1014, ask=1.1016)
        assert applied is False
        assert position.volume == pytest.approx(0.40)
        assert worker._position_partial_take_profit_done[position.position_id] is True

        events = [
            event
            for event in worker.store.load_events(limit=20)
            if event.get("message") == "Partial take-profit skipped for runner preservation"
        ]
        assert events
    finally:
        worker.broker.close()
        worker.store.close()


def test_partial_take_profit_proceeds_when_worker_lease_revoked(tmp_path):
    worker = _make_worker(
        tmp_path,
        mode=RunMode.EXECUTION,
        strategy_name="momentum",
        stream_event_cooldown_sec=0.0,
        strategy_param_overrides={
            "partial_take_profit_enabled": True,
            "partial_take_profit_r_multiple": 1.0,
            "partial_take_profit_fraction": 0.25,
            "partial_take_profit_skip_trend_positions": False,
            "partial_take_profit_skip_runner_positions": False,
            "protective_runner_preservation_enabled": False,
        },
    )
    try:
        lease_key = f"worker.lease.{worker.symbol}"
        worker._worker_lease_key = lease_key
        worker._worker_lease_id = "lease-a"
        worker.store.set_kv(lease_key, "lease-b")

        position = Position(
            position_id="deal-lease-partial-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.40,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1030,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)

        captured: dict[str, float] = {}

        def _fake_close(_position, close_volume=None):
            if close_volume is not None:
                captured["close_volume"] = float(close_volume)

        worker.broker.close_position = _fake_close  # type: ignore[assignment]

        applied = worker._maybe_apply_partial_take_profit(position, bid=1.1011, ask=1.1013)

        assert applied is True
        assert worker.stop_event.is_set()
        assert float(captured["close_volume"]) == pytest.approx(0.1)
        assert position.volume == pytest.approx(0.3)
        events = worker.store.load_events(limit=20)
        assert any(
            event.get("message")
            == "Worker lease revoked during partial close, proceeding with risk-reducing broker close"
            for event in events
        )
    finally:
        worker.broker.close()
        worker.store.close()


def test_partial_take_profit_does_not_send_second_close_when_remaining_rounds_to_zero(tmp_path):
    worker = _make_worker(
        tmp_path,
        mode=RunMode.EXECUTION,
        strategy_name="momentum",
        stream_event_cooldown_sec=0.0,
        strategy_param_overrides={
            "partial_take_profit_enabled": True,
            "partial_take_profit_r_multiple": 1.0,
            "partial_take_profit_fraction": 0.55,
            "partial_take_profit_skip_trend_positions": False,
            "partial_take_profit_skip_runner_positions": False,
            "protective_runner_preservation_enabled": False,
        },
    )
    try:
        worker.symbol_spec = SymbolSpec(
            symbol=worker.symbol,
            tick_size=0.0001,
            tick_value=10.0,
            contract_size=100000.0,
            lot_min=0.5,
            lot_max=10.0,
            lot_step=0.3,
            price_precision=5,
            lot_precision=2,
        )

        position = Position(
            position_id="deal-partial-rounding-1",
            symbol=worker.symbol,
            side=Side.BUY,
            volume=1.15,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1030,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)

        close_calls: list[float] = []

        def _fake_close(_position, close_volume=None):
            close_calls.append(float(close_volume if close_volume is not None else _position.volume))

        def _unexpected_full_close(*args, **kwargs):
            _ = (args, kwargs)
            raise AssertionError("_close_position should not be called after successful partial close")

        worker.broker.close_position = _fake_close  # type: ignore[assignment]
        worker._close_position = _unexpected_full_close  # type: ignore[assignment]

        applied = worker._maybe_apply_partial_take_profit(position, bid=1.1011, ask=1.1013)

        assert applied is True
        assert len(close_calls) == 1
        assert close_calls[0] == pytest.approx(0.6)
        assert position.volume == pytest.approx(0.55)
        assert worker._position_partial_take_profit_done[position.position_id] is True
        events = worker.store.load_events(limit=20)
        assert any(
            event.get("message") == "Partial take-profit left non-normalized remaining volume"
            for event in events
        )
        assert any(
            event.get("message") == "Partial take-profit executed"
            for event in events
        )
        assert all(event.get("message") != "Position closed" for event in events)
    finally:
        worker.broker.close()
        worker.store.close()


def test_partial_take_profit_skips_trend_positions_before_scale_out(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        strategy_param_overrides={
            "partial_take_profit_enabled": True,
            "partial_take_profit_r_multiple": 1.0,
            "partial_take_profit_fraction": 0.25,
            "partial_take_profit_skip_trend_positions": True,
            "partial_take_profit_skip_runner_positions": False,
            "protective_runner_preservation_enabled": False,
        },
    )
    try:
        position = Position(
            position_id="paper-trend-partial-skip",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.40,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1030,
            opened_at=1.0,
            status="open",
        )

        applied = worker._maybe_apply_partial_take_profit(position, bid=1.1011, ask=1.1013)
        assert applied is False
        assert position.volume == pytest.approx(0.40)
        assert worker._position_partial_take_profit_done[position.position_id] is True

        events = [
            event
            for event in worker.store.load_events(limit=20)
            if event.get("message") == "Partial take-profit skipped for trend preservation"
        ]
        assert events
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_respects_buy_breakeven_offset(tmp_path):
    worker = _make_worker(
        tmp_path,
        trailing_activation_ratio=0.0,
        trailing_distance_pips=10.0,
        trailing_breakeven_offset_pips=2.0,
        strategy_param_overrides={"protective_runner_preservation_enabled": False},
    )
    try:
        position = Position(
            position_id="paper-3c",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )

        new_sl_early, _ = worker._trailing_candidate_stop(position, bid=1.1001, ask=1.1003)
        assert new_sl_early is None

        new_sl_late, _ = worker._trailing_candidate_stop(position, bid=1.1003, ask=1.1005)
        assert new_sl_late == pytest.approx(1.1002)
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_respects_sell_breakeven_offset(tmp_path):
    worker = _make_worker(
        tmp_path,
        trailing_activation_ratio=0.0,
        trailing_distance_pips=10.0,
        trailing_breakeven_offset_pips=2.0,
        strategy_param_overrides={"protective_runner_preservation_enabled": False},
    )
    try:
        position = Position(
            position_id="paper-3d",
            symbol="EURUSD",
            side=Side.SELL,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.1010,
            take_profit=1.0980,
            opened_at=1.0,
            status="open",
        )

        new_sl_early, _ = worker._trailing_candidate_stop(position, bid=1.0997, ask=1.0999)
        assert new_sl_early is None

        new_sl_late, _ = worker._trailing_candidate_stop(position, bid=1.0995, ask=1.0997)
        assert new_sl_late == pytest.approx(1.0998)
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_respects_broker_min_stop_distance(tmp_path):
    worker = _make_worker(
        tmp_path,
        trailing_activation_ratio=0.5,
        trailing_distance_pips=10.0,
        strategy_param_overrides={"protective_runner_preservation_enabled": False},
    )
    try:
        assert worker.symbol_spec is not None
        worker.symbol_spec.metadata["min_stop_distance_price"] = 0.0015  # 15 pips for EURUSD

        position = Position(
            position_id="paper-min-stop",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )

        new_sl, progress = worker._trailing_candidate_stop(position, bid=1.1010, ask=1.1012)
        assert progress == pytest.approx(0.5)
        # Raw trailing target would be 1.1000, but broker min distance clamps it to 1.0995.
        assert new_sl == pytest.approx(1.0995)
    finally:
        worker.broker.close()
        worker.store.close()


def test_open_level_guard_respects_broker_min_stop_distance_in_execution(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        assert worker.symbol_spec is not None
        worker.symbol_spec.metadata["min_stop_distance_price"] = 0.0015  # 15 pips for EURUSD

        stop_loss, take_profit, payload = worker._apply_broker_open_level_guard(
            side=Side.BUY,
            stop_loss=1.1004,
            take_profit=1.1020,
            bid=1.1010,
            ask=1.1012,
        )

        assert stop_loss == pytest.approx(1.0995)
        assert take_profit == pytest.approx(1.1027)
        assert payload is not None
        assert payload["min_stop_distance_price"] == pytest.approx(0.0015)
    finally:
        worker.broker.close()
        worker.store.close()


def test_normalize_volume_handles_float_precision_edge(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        assert worker.symbol_spec is not None
        worker.symbol_spec.lot_step = 0.1
        worker.symbol_spec.lot_min = 0.1
        worker.symbol_spec.lot_precision = 1

        raw_volume = 0.3 - 1e-12
        normalized = worker._normalize_volume(raw_volume)
        assert normalized == pytest.approx(0.3)
    finally:
        worker.broker.close()
        worker.store.close()


def test_normalize_volume_ignores_uninitialized_lot_max(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        assert worker.symbol_spec is not None
        worker.symbol_spec.lot_step = 0.1
        worker.symbol_spec.lot_min = 0.1
        worker.symbol_spec.lot_max = 0.0
        worker.symbol_spec.lot_precision = 1

        normalized = worker._normalize_volume(0.26)
        assert normalized == pytest.approx(0.2)
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_uses_asset_specific_breakeven_offsets(tmp_path):
    worker = _make_worker(
        tmp_path,
        trailing_activation_ratio=0.0,
        trailing_distance_pips=10.0,
        trailing_breakeven_offset_pips=0.0,
        trailing_breakeven_offset_pips_fx=2.0,
        trailing_breakeven_offset_pips_index=3.0,
        trailing_breakeven_offset_pips_commodity=6.0,
        strategy_param_overrides={"protective_runner_preservation_enabled": False},
    )
    try:
        fx_position = Position(
            position_id="paper-off-fx",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        fx_sl, _ = worker._trailing_candidate_stop(fx_position, bid=1.1003, ask=1.1005)
        assert fx_sl == pytest.approx(1.1002)

        worker.symbol = "US100"
        worker.symbol_spec = worker.broker.get_symbol_spec("US100")
        index_position = Position(
            position_id="paper-off-index",
            symbol="US100",
            side=Side.BUY,
            volume=0.1,
            open_price=17750.0,
            stop_loss=17700.0,
            take_profit=17850.0,
            opened_at=1.0,
            status="open",
        )
        index_sl, _ = worker._trailing_candidate_stop(index_position, bid=17754.0, ask=17756.0)
        assert index_sl == pytest.approx(17753.0)

        worker.symbol = "WTI"
        worker.symbol_spec = worker.broker.get_symbol_spec("WTI")
        commodity_position = Position(
            position_id="paper-off-com",
            symbol="WTI",
            side=Side.BUY,
            volume=0.1,
            open_price=75.0,
            stop_loss=74.0,
            take_profit=77.0,
            opened_at=1.0,
            status="open",
        )
        commodity_sl, _ = worker._trailing_candidate_stop(commodity_position, bid=75.7, ask=75.9)
        assert commodity_sl == pytest.approx(75.6)
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_modify_uses_broker_update_in_execution_mode(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="12345",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )

        calls: list[tuple[str, float, float]] = []

        def _fake_modify(pos: Position, stop_loss: float, take_profit: float) -> None:
            calls.append((pos.position_id, stop_loss, take_profit))

        worker.broker.modify_position = _fake_modify  # type: ignore[assignment]
        worker._apply_trailing_stop(position, new_stop_loss=1.1002, progress=0.8)

        assert calls == [("12345", 1.1002, 1.1020)]
        assert position.stop_loss == pytest.approx(1.1002)

        trailing_events = [
            event for event in worker.store.load_events() if event["message"] == "Trailing stop adjusted"
        ]
        assert trailing_events
        assert trailing_events[0]["level"] == "WARN"
        assert trailing_events[0]["payload"]["new_stop_loss"] == pytest.approx(1.1002)
    finally:
        worker.broker.close()
        worker.store.close()


def test_parse_trailing_override_from_signal_converts_activation_pips_to_ratio(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        signal = Signal(
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "trailing_stop": {
                    "trailing_enabled": True,
                    "trailing_distance_pips": 7.5,
                    "trailing_activation_pips": 10.0,
                }
            },
        )
        override = worker._parse_trailing_override_from_signal(
            signal=signal,
            entry_price=1.1000,
            take_profit_price=1.1020,  # 20 pips for EURUSD
        )
        assert override is not None
        assert override["trailing_distance_pips"] == pytest.approx(7.5)
        assert override["trailing_activation_ratio"] == pytest.approx(0.5)
    finally:
        worker.broker.close()
        worker.store.close()


def test_parse_trailing_override_from_signal_uses_entry_component_activation_ratio_fallback(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="momentum",
        trailing_activation_ratio=0.9,
        strategy_params_map={
            "momentum": {
                "momentum_trailing_activation_ratio": 0.8,
            },
            "g1": {
                "g1_trailing_activation_ratio": 0.25,
            },
        },
    )
    try:
        signal = Signal(
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "indicator": "g1",
                "trailing_stop": {
                    "trailing_enabled": True,
                    "trailing_distance_pips": 7.5,
                },
            },
        )
        override = worker._parse_trailing_override_from_signal(
            signal=signal,
            entry_price=1.1000,
            take_profit_price=1.1020,
        )
        assert override is not None
        assert override["trailing_distance_pips"] == pytest.approx(7.5)
        assert override["trailing_activation_ratio"] == pytest.approx(0.25)
    finally:
        worker.broker.close()
        worker.store.close()


def test_parse_trailing_override_from_signal_fast_ma_mode(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        signal = Signal(
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "trailing_stop": {
                    "trailing_enabled": True,
                    "trailing_mode": "fast_ma",
                    "trailing_activation_r_multiple": 0.9,
                    "trailing_activation_min_profit_pips": 3.0,
                    "fast_ma_buffer_atr": 0.2,
                    "fast_ma_buffer_pips": 0.4,
                    "fast_ma_min_step_pips": 0.7,
                    "fast_ma_update_cooldown_sec": 6.0,
                }
            },
        )
        override = worker._parse_trailing_override_from_signal(
            signal=signal,
            entry_price=1.1000,
            take_profit_price=1.1020,
        )
        assert override is not None
        assert override["trailing_mode"] == "fast_ma"
        assert override["trailing_activation_r_multiple"] == pytest.approx(0.9)
        assert override["trailing_activation_min_profit_pips"] == pytest.approx(3.0)
        assert override["fast_ma_buffer_atr"] == pytest.approx(0.2)
        assert override["fast_ma_buffer_pips"] == pytest.approx(0.4)
        assert override["fast_ma_min_step_pips"] == pytest.approx(0.7)
        assert override["fast_ma_update_cooldown_sec"] == pytest.approx(6.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_parse_trailing_override_from_signal_persists_adaptive_context_without_strategy_override(tmp_path):
    worker = _make_worker(tmp_path, adaptive_trailing_enabled=True)
    try:
        signal = Signal(
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "indicator": "bollinger_bands",
                "regime": "mean_reversion",
                "distance_sigma": -2.8,
            },
        )
        override = worker._parse_trailing_override_from_signal(
            signal=signal,
            entry_price=1.1000,
            take_profit_price=1.1020,
            stop_loss_price=1.0990,
        )
        assert override is not None
        assert override["adaptive_trailing_family"] == "mean_reversion"
        assert override["adaptive_trailing_entry_extreme_abs"] == pytest.approx(2.8)
        assert override["adaptive_trailing_initial_stop_pips"] == pytest.approx(10.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_parse_trailing_override_from_signal_persists_adaptive_context_for_g2_signal(tmp_path):
    worker = _make_worker(tmp_path, symbol="US30", strategy_name="g2", adaptive_trailing_enabled=True)
    try:
        signal = Signal(
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=15.0,
            take_profit_pips=45.0,
            metadata={
                "indicator": "g2_index_pullback",
                "atr_pips": 18.0,
            },
        )
        override = worker._parse_trailing_override_from_signal(
            signal=signal,
            entry_price=46000.0,
            take_profit_price=46090.0,
            stop_loss_price=45970.0,
        )
        expected_initial_stop_pips = abs(46000.0 - 45970.0) / max(worker._execution_pip_size(), 1e-9)
        assert override is not None
        assert override["adaptive_trailing_family"] == "trend"
        assert override["adaptive_trailing_initial_stop_pips"] == pytest.approx(expected_initial_stop_pips)
    finally:
        worker.broker.close()
        worker.store.close()


def test_effective_trailing_settings_infer_family_and_initial_stop_without_override(tmp_path):
    worker = _make_worker(tmp_path, strategy_name="momentum")
    position = Position(
        position_id="paper-default-trailing-1",
        symbol="EURUSD",
        side=Side.BUY,
        volume=0.1,
        open_price=1.1000,
        stop_loss=1.0990,
        take_profit=1.1040,
        opened_at=1.0,
        status="open",
    )
    try:
        worker._position_entry_component_by_id[position.position_id] = "momentum"
        settings = worker._effective_trailing_settings(position)
        assert settings["adaptive_trailing_family"] == "trend"
        assert settings["adaptive_trailing_initial_stop_pips"] == pytest.approx(10.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_candidate_stop_uses_strategy_override(tmp_path):
    worker = _make_worker(tmp_path, trailing_activation_ratio=0.0, trailing_distance_pips=10.0)
    try:
        position = Position(
            position_id="paper-override-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker._set_position_trailing_override(
            position.position_id,
            {
                "trailing_activation_ratio": 0.5,
                "trailing_distance_pips": 5.0,
                "trailing_breakeven_offset_pips": 0.0,
            },
        )

        new_sl_early, progress_early = worker._trailing_candidate_stop(position, bid=1.1006, ask=1.1008)
        assert progress_early < 0.5
        assert new_sl_early is None

        new_sl_late, progress_late = worker._trailing_candidate_stop(position, bid=1.1010, ask=1.1012)
        assert progress_late == pytest.approx(0.5)
        assert new_sl_late == pytest.approx(1.1005)
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_candidate_stop_adaptive_trend_heat_tightens_distance_candidate(tmp_path):
    worker = _make_worker(
        tmp_path,
        trailing_activation_ratio=0.0,
        trailing_distance_pips=10.0,
        adaptive_trailing_enabled=True,
        adaptive_trailing_heat_trigger=0.30,
        adaptive_trailing_heat_full=0.80,
        adaptive_trailing_distance_factor_at_full=0.50,
        adaptive_trailing_profit_lock_r_at_full=0.0,
    )
    try:
        position = Position(
            position_id="paper-adaptive-trend-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker._set_position_trailing_override(
            position.position_id,
            {
                "adaptive_trailing_family": "trend",
                "adaptive_trailing_initial_stop_pips": 10.0,
            },
        )
        signal = Signal(
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "indicator": "mean_breakout_v2",
                "atr_pips": 12.0,
                "zscore": 3.2,
                "zscore_effective_threshold": 1.6,
            },
        )
        new_sl, progress = worker._trailing_candidate_stop(
            position,
            bid=1.1014,
            ask=1.1016,
            signal=signal,
        )
        assert progress == pytest.approx(0.7)
        assert new_sl == pytest.approx(1.1008)
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_candidate_stop_adaptive_profit_lock_floor_applies(tmp_path):
    worker = _make_worker(
        tmp_path,
        trailing_activation_ratio=0.0,
        trailing_distance_pips=20.0,
        adaptive_trailing_enabled=True,
        adaptive_trailing_heat_trigger=0.30,
        adaptive_trailing_heat_full=0.80,
        adaptive_trailing_distance_factor_at_full=0.70,
        adaptive_trailing_profit_lock_r_at_full=0.60,
        adaptive_trailing_profit_lock_min_progress=0.25,
    )
    try:
        position = Position(
            position_id="paper-adaptive-lock-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker._set_position_trailing_override(
            position.position_id,
            {
                "adaptive_trailing_family": "trend",
                "adaptive_trailing_initial_stop_pips": 10.0,
            },
        )
        signal = Signal(
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "indicator": "mean_breakout_v2",
                "atr_pips": 20.0,
                "zscore": 3.2,
                "zscore_effective_threshold": 1.6,
            },
        )
        new_sl, progress = worker._trailing_candidate_stop(
            position,
            bid=1.1012,
            ask=1.1014,
            signal=signal,
        )
        assert progress == pytest.approx(0.6)
        assert new_sl == pytest.approx(1.1006)
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_candidate_stop_adaptive_mean_reversion_heat_uses_sigma_capture(tmp_path):
    worker = _make_worker(
        tmp_path,
        trailing_activation_ratio=0.0,
        trailing_distance_pips=10.0,
        adaptive_trailing_enabled=True,
        adaptive_trailing_heat_trigger=0.30,
        adaptive_trailing_heat_full=0.80,
        adaptive_trailing_distance_factor_at_full=0.50,
        adaptive_trailing_profit_lock_r_at_full=0.0,
    )
    try:
        position = Position(
            position_id="paper-adaptive-mr-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker._set_position_trailing_override(
            position.position_id,
            {
                "adaptive_trailing_family": "mean_reversion",
                "adaptive_trailing_initial_stop_pips": 10.0,
                "adaptive_trailing_entry_extreme_abs": 3.5,
            },
        )
        signal = Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "indicator": "bollinger_bands",
                "atr_pips": 10.0,
                "distance_sigma": -0.6,
            },
        )
        new_sl, progress = worker._trailing_candidate_stop(
            position,
            bid=1.1011,
            ask=1.1013,
            signal=signal,
        )
        assert progress == pytest.approx(0.55)
        assert new_sl == pytest.approx(1.1006)
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_candidate_stop_prefers_adaptive_fallback_over_fixed_distance_when_no_explicit_override(
    tmp_path,
):
    worker = _make_worker(
        tmp_path,
        trailing_activation_ratio=0.0,
        trailing_distance_pips=10.0,
        adaptive_trailing_enabled=True,
        adaptive_trailing_heat_trigger=0.30,
        adaptive_trailing_heat_full=0.80,
        adaptive_trailing_distance_factor_at_full=0.50,
        adaptive_trailing_profit_lock_r_at_full=0.0,
    )
    try:
        position = Position(
            position_id="paper-adaptive-default-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1060,
            opened_at=1.0,
            status="open",
            strategy_entry_component="momentum",
        )
        signal = Signal(
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=60.0,
            metadata={
                "indicator": "ma_cross",
                "atr_pips": 40.0,
                "zscore": 3.2,
                "zscore_effective_threshold": 1.6,
            },
        )
        new_sl, progress = worker._trailing_candidate_stop(
            position,
            bid=1.1042,
            ask=1.1044,
            signal=signal,
        )
        assert progress == pytest.approx(0.7)
        assert new_sl == pytest.approx(1.1022)
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_candidate_stop_uses_fast_ma_override(tmp_path):
    worker = _make_worker(
        tmp_path,
        trailing_activation_ratio=1.0,
        trailing_distance_pips=50.0,
        strategy_param_overrides={"protective_runner_preservation_enabled": False},
    )
    try:
        position = Position(
            position_id="paper-fast-ma-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker._set_position_trailing_override(
            position.position_id,
            {
                "trailing_mode": "fast_ma",
                "trailing_activation_r_multiple": 0.5,
                "trailing_activation_min_profit_pips": 0.0,
                "fast_ma_buffer_atr": 0.0,
                "fast_ma_buffer_pips": 0.0,
                "fast_ma_min_step_pips": 0.0,
                "fast_ma_update_cooldown_sec": 0.0,
                "trailing_breakeven_offset_pips": 0.0,
            },
        )
        signal = Signal(
            side=Side.BUY,
            confidence=1.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"fast_ma": 1.1004, "atr_pips": 8.0},
        )
        new_sl, progress = worker._trailing_candidate_stop(
            position,
            bid=1.1007,
            ask=1.1009,
            signal=signal,
        )
        assert progress > 0.0
        assert new_sl == pytest.approx(1.1004)
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_candidate_stop_uses_directional_anchor_from_trailing_metadata(tmp_path):
    worker = _make_worker(
        tmp_path,
        trailing_activation_ratio=1.0,
        trailing_distance_pips=50.0,
        strategy_param_overrides={"protective_runner_preservation_enabled": False},
    )
    try:
        position = Position(
            position_id="paper-fast-ma-2",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker._set_position_trailing_override(
            position.position_id,
            {
                "trailing_mode": "fast_ma",
                "trailing_activation_r_multiple": 0.0,
                "trailing_activation_min_profit_pips": 0.0,
                "fast_ma_buffer_atr": 0.0,
                "fast_ma_buffer_pips": 0.0,
                "fast_ma_min_step_pips": 0.0,
                "fast_ma_update_cooldown_sec": 0.0,
                "trailing_breakeven_offset_pips": 0.0,
            },
        )
        signal = Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "atr_pips": 8.0,
                "trailing_stop": {
                    "long_anchor_value": 1.1005,
                    "short_anchor_value": 1.0995,
                },
            },
        )
        new_sl, progress = worker._trailing_candidate_stop(
            position,
            bid=1.1007,
            ask=1.1009,
            signal=signal,
        )
        assert progress > 0.0
        assert new_sl == pytest.approx(1.1005)
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_override_persists_in_kv_and_can_be_cleared(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        position_id = "paper-override-2"
        worker._set_position_trailing_override(
            position_id,
            {
                "trailing_activation_ratio": 0.4,
                "trailing_distance_pips": 6.0,
                "trailing_breakeven_offset_pips": 1.0,
            },
        )
        worker._strategy_trailing_overrides_by_position.clear()
        loaded = worker._get_position_trailing_override(position_id)
        assert loaded is not None
        assert loaded["trailing_activation_ratio"] == pytest.approx(0.4)
        assert loaded["trailing_distance_pips"] == pytest.approx(6.0)
        assert loaded["trailing_breakeven_offset_pips"] == pytest.approx(1.0)

        worker._set_position_trailing_override(position_id, None)
        worker._strategy_trailing_overrides_by_position.clear()
        loaded_after_clear = worker._get_position_trailing_override(position_id)
        assert loaded_after_clear is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_fast_ma_trailing_override_persists_in_kv(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        position_id = "paper-fast-ma-2"
        worker._set_position_trailing_override(
            position_id,
            {
                "trailing_mode": "fast_ma",
                "trailing_activation_r_multiple": 0.8,
                "trailing_activation_min_profit_pips": 2.0,
                "fast_ma_buffer_atr": 0.1,
                "fast_ma_buffer_pips": 0.2,
                "fast_ma_min_step_pips": 0.3,
                "fast_ma_update_cooldown_sec": 4.0,
                "trailing_breakeven_offset_pips": 0.0,
            },
        )
        worker._strategy_trailing_overrides_by_position.clear()
        loaded = worker._get_position_trailing_override(position_id)
        assert loaded is not None
        assert loaded["trailing_mode"] == "fast_ma"
        assert loaded["trailing_activation_r_multiple"] == pytest.approx(0.8)
        assert loaded["fast_ma_buffer_pips"] == pytest.approx(0.2)
        assert loaded["fast_ma_update_cooldown_sec"] == pytest.approx(4.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_save_state_prunes_stale_position_tracking_state(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        active = Position(
            position_id="active-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(active)
        worker._position_peak_favorable_pips["stale-1"] = 12.0
        worker._position_peak_favorable_ts["stale-1"] = 2.0
        worker._position_peak_adverse_pips["stale-1"] = 4.0
        worker._position_trailing_last_update_ts["stale-1"] = 3.0
        worker._position_partial_take_profit_done["stale-1"] = True
        worker._position_entry_strategy_by_id["stale-1"] = "momentum"
        worker._position_entry_component_by_id["stale-1"] = "g1"
        worker._position_entry_signal_by_id["stale-1"] = "ema_trend_up"
        worker._strategy_trailing_overrides_by_position["stale-1"] = {
            "trailing_activation_ratio": 0.5,
            "trailing_distance_pips": 5.0,
        }
        worker._position_peak_favorable_pips["active-1"] = 1.0
        worker._position_entry_strategy_by_id["active-1"] = "momentum"

        worker._save_state(last_price=1.1005)

        assert "stale-1" not in worker._position_peak_favorable_pips
        assert "stale-1" not in worker._position_peak_favorable_ts
        assert "stale-1" not in worker._position_peak_adverse_pips
        assert "stale-1" not in worker._position_trailing_last_update_ts
        assert "stale-1" not in worker._position_partial_take_profit_done
        assert "stale-1" not in worker._position_entry_strategy_by_id
        assert "stale-1" not in worker._position_entry_component_by_id
        assert "stale-1" not in worker._position_entry_signal_by_id
        assert "stale-1" not in worker._strategy_trailing_overrides_by_position
        assert worker._position_peak_favorable_pips["active-1"] == pytest.approx(1.0)
        assert worker._position_entry_strategy_by_id["active-1"] == "momentum"
    finally:
        worker.broker.close()
        worker.store.close()


def test_save_state_prune_preserves_store_backed_open_position_state(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        store_backed = Position(
            position_id="store-open-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=2.0,
            status="open",
        )
        worker.store.upsert_trade(store_backed, worker.name, worker.strategy_name, worker.mode.value)
        worker._position_peak_favorable_pips["store-open-1"] = 3.0
        worker._position_entry_strategy_by_id["store-open-1"] = "momentum"
        worker._position_peak_favorable_pips["stale-2"] = 12.0
        worker._position_entry_strategy_by_id["stale-2"] = "momentum"

        worker._save_state(last_price=1.1005)

        assert worker._position_peak_favorable_pips["store-open-1"] == pytest.approx(3.0)
        assert worker._position_entry_strategy_by_id["store-open-1"] == "momentum"
        assert "stale-2" not in worker._position_peak_favorable_pips
        assert "stale-2" not in worker._position_entry_strategy_by_id
    finally:
        worker.broker.close()
        worker.store.close()


def test_force_full_sweep_prunes_stale_pending_open_and_persisted_trailing_override(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        worker._order_manager.state.pending_open_created_monotonic_by_id["pending-stale"] = 100.0
        live_pending = PendingOpen(
            pending_id="pending-live",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            entry=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            created_at=10.0,
            thread_name=worker.name,
            strategy=worker.strategy_name,
            mode=worker.mode.value,
        )
        worker.store.upsert_pending_open(live_pending)
        worker._order_manager.state.pending_open_created_monotonic_by_id["pending-live"] = 101.0
        worker._set_position_trailing_override(
            "stale-position-id",
            {
                "trailing_activation_ratio": 0.5,
                "trailing_distance_pips": 6.0,
            },
        )
        worker._strategy_trailing_overrides_by_position.clear()

        worker._prune_position_runtime_state(force_full_sweep=True)

        assert "pending-stale" not in worker._order_manager.state.pending_open_created_monotonic_by_id
        assert worker._order_manager.state.pending_open_created_monotonic_by_id["pending-live"] == pytest.approx(101.0)
        assert worker.store.get_kv(worker._strategy_trailing_store_key) is None

        prune_events = [
            event
            for event in worker.store.load_events(limit=20)
            if event.get("message") == "Worker runtime state pruned"
        ]
        assert prune_events
        payload = prune_events[0].get("payload") or {}
        assert payload.get("cleared_persisted_trailing_override_for") == "stale-position-id"
    finally:
        worker.broker.close()
        worker.store.close()


def test_session_exit_reason_for_index_within_buffer(tmp_path):
    worker = _make_worker(tmp_path, session_close_buffer_min=15)
    try:
        worker.symbol = "US100"
        now_ts = 1_700_000_000.0

        def _fake_session_close(symbol: str, ts: float) -> float | None:
            assert symbol == "US100"
            assert ts == now_ts
            return now_ts + 600  # 10 minutes to close, inside 15-minute buffer.

        worker.broker.get_session_close_utc = _fake_session_close  # type: ignore[assignment]
        reason = worker._session_exit_reason(now_ts)
        assert reason == "session_end_buffer:600s"
    finally:
        worker.broker.close()
        worker.store.close()


def test_active_iteration_uses_wall_clock_tick_timestamp_for_position_management(tmp_path, monkeypatch):
    worker = _make_worker(tmp_path, symbol="US100")
    try:
        active = Position(
            position_id="active-wall-clock-1",
            symbol="US100",
            side=Side.BUY,
            volume=1.0,
            open_price=20000.0,
            stop_loss=19900.0,
            take_profit=20200.0,
            opened_at=1_700_000_000.0,
            status="open",
        )
        worker.position_book.upsert(active)
        tick = PriceTick(symbol="US100", bid=20010.0, ask=20011.0, timestamp=1_700_000_004.0)
        captured: dict[str, object] = {}

        monkeypatch.setattr(worker, "_load_symbol_spec", lambda: None)
        monkeypatch.setattr(worker._orchestrator, "_load_iteration_tick", lambda state: tick)
        monkeypatch.setattr(
            worker._history_runtime,
            "append_tick_sample",
            lambda _tick, current_volume=None, volume_sample=None: 1_700_000_005.0,
        )
        monkeypatch.setattr(worker, "_update_last_price_freshness", lambda **kwargs: None)
        monkeypatch.setattr(worker, "_reset_broker_error_trackers", lambda: None)
        monkeypatch.setattr(worker, "_spread_in_pips", lambda bid, ask: 1.0)
        monkeypatch.setattr(worker, "_spread_in_pct", lambda bid, ask: 0.01)
        monkeypatch.setattr(worker, "_spread_filter_metrics", lambda current_spread_pips: (False, 1.0, 2.0))
        monkeypatch.setattr(worker, "_append_spread_sample", lambda current_spread_pips: None)
        monkeypatch.setattr(worker, "_refresh_stream_health", lambda: None)
        monkeypatch.setattr(
            worker._cycle_runtime,
            "process_active_position",
            lambda **kwargs: captured.update(kwargs) or True,
        )

        state = type(
            "_State",
            (),
            {
                "tick": None,
                "last_error": None,
                "stream_health": None,
                "sleep_override_sec": None,
            },
        )()
        result = worker._orchestrator._process_iteration(state)  # noqa: SLF001

        assert result is True
        assert float(captured["tick_timestamp"]) == pytest.approx(1_700_000_005.0)
        assert captured["tick"] is tick
    finally:
        worker.broker.close()
        worker.store.close()


def test_news_filter_close_returns_close_reason(tmp_path):
    worker = _make_worker(tmp_path, news_event_action="close", news_event_buffer_min=5)
    try:
        now_ts = 1_700_000_000.0
        position = Position(
            position_id="paper-4",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )

        worker.broker.get_upcoming_high_impact_events = (  # type: ignore[assignment]
            lambda current_ts, within_sec: [
                NewsEvent(
                    event_id="nfp-1",
                    name="NFP",
                    timestamp=current_ts + min(within_sec, 60),
                    impact="high",
                    country="US",
                )
            ]
        )
        reason = worker._apply_news_filter(position, now_ts, bid=1.1002, ask=1.1004)
        assert reason == "news_event_close:NFP"
    finally:
        worker.broker.close()
        worker.store.close()


def test_news_filter_breakeven_moves_stop(tmp_path):
    worker = _make_worker(tmp_path, news_event_action="breakeven", news_event_buffer_min=5)
    try:
        now_ts = 1_700_000_000.0
        position = Position(
            position_id="paper-5",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0985,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )

        worker.broker.get_upcoming_high_impact_events = (  # type: ignore[assignment]
            lambda current_ts, within_sec: [
                NewsEvent(
                    event_id="news-1",
                    name="CPI",
                    timestamp=current_ts + min(within_sec, 60),
                    impact="high",
                    country="US",
                )
            ]
        )
        reason = worker._apply_news_filter(position, now_ts, bid=1.1002, ask=1.1004)
        assert reason is None
        assert position.stop_loss == pytest.approx(1.1000)
    finally:
        worker.broker.close()
        worker.store.close()


def test_news_filter_processes_multiple_events_in_single_pass(tmp_path):
    worker = _make_worker(tmp_path, news_event_action="breakeven", news_event_buffer_min=5)
    try:
        now_ts = 1_700_000_000.0
        position = Position(
            position_id="paper-6",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0985,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )

        worker.broker.get_upcoming_high_impact_events = (  # type: ignore[assignment]
            lambda current_ts, within_sec: [
                NewsEvent(
                    event_id="news-a",
                    name="CPI",
                    timestamp=current_ts + min(within_sec, 60),
                    impact="high",
                    country="US",
                ),
                NewsEvent(
                    event_id="news-b",
                    name="FOMC",
                    timestamp=current_ts + min(within_sec, 120),
                    impact="high",
                    country="US",
                ),
            ]
        )

        reason = worker._apply_news_filter(position, now_ts, bid=1.1002, ask=1.1004)
        assert reason is None
        assert "news-a" in worker._handled_news_events
        assert "news-b" in worker._handled_news_events
    finally:
        worker.broker.close()
        worker.store.close()


def test_news_filter_blocks_new_entries_inside_event_buffer(tmp_path):
    worker = _make_worker(tmp_path, news_event_action="breakeven", news_event_buffer_min=5)
    try:
        now_ts = 1_700_000_000.0
        worker.broker.get_upcoming_high_impact_events = (  # type: ignore[assignment]
            lambda current_ts, within_sec: [
                NewsEvent(
                    event_id="news-entry-1",
                    name="CPI",
                    timestamp=current_ts + min(within_sec, 30),
                    impact="high",
                    country="US",
                )
            ]
        )
        assert worker._entry_news_filter_allows_open(now_ts, Side.BUY) is False
    finally:
        worker.broker.close()
        worker.store.close()


def test_price_buffer_debug_fields_uses_wall_clock_age_for_history_timestamps(tmp_path, monkeypatch):
    worker = _make_worker(tmp_path)
    try:
        worker.prices.append(1.1000)
        worker.price_timestamps.append(1_700_000_000.0)
        worker._price_samples_seen = 1
        monkeypatch.setattr(worker, "_monotonic_now", lambda: 100.0)
        monkeypatch.setattr(worker, "_wall_time_now", lambda: 1_700_000_010.0)

        payload = worker._price_buffer_debug_fields(now_ts=100.0)

        assert payload["latest_price_ts"] == pytest.approx(1_700_000_000.0)
        assert payload["latest_price_age_sec"] == pytest.approx(10.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_spread_filter_blocks_anomalous_spread(tmp_path):
    worker = _make_worker(
        tmp_path,
        spread_filter_enabled=True,
        spread_anomaly_multiplier=3.0,
        spread_avg_window=10,
        spread_min_samples=3,
    )
    try:
        worker.spreads_pips.extend([1.0, 1.1, 0.9])
        blocked, avg_spread, threshold = worker._spread_filter_metrics(current_spread_pips=3.6)
        assert blocked is True
        assert avg_spread == pytest.approx(1.0)
        assert threshold == pytest.approx(3.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_spread_filter_uses_atomic_spread_snapshot(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        spread_filter_enabled=True,
        spread_anomaly_multiplier=3.0,
        spread_avg_window=10,
        spread_min_samples=3,
    )
    try:
        monkeypatch.setattr(worker._risk_evaluator, "spread_buffer_snapshot", lambda: [1.0, 1.1, 0.9])
        worker.spreads_pips = _ExplodingDeque([9.0, 9.0, 9.0])

        blocked, avg_spread, threshold = worker._spread_filter_metrics(current_spread_pips=3.6)

        assert blocked is True
        assert avg_spread == pytest.approx(1.0)
        assert threshold == pytest.approx(3.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_spread_filter_ignores_before_min_samples(tmp_path):
    worker = _make_worker(
        tmp_path,
        spread_filter_enabled=True,
        spread_anomaly_multiplier=3.0,
        spread_avg_window=10,
        spread_min_samples=5,
    )
    try:
        worker.spreads_pips.extend([1.0, 1.1, 0.9])
        blocked, avg_spread, threshold = worker._spread_filter_metrics(current_spread_pips=10.0)
        assert blocked is False
        assert avg_spread == pytest.approx(0.0)
        assert threshold == pytest.approx(0.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_spread_pct_filter_blocks_crypto_entry(tmp_path):
    worker = _make_worker(
        tmp_path,
        spread_pct_filter_enabled=True,
        spread_max_pct_crypto=0.5,
        spread_max_pct_cfd=0.2,
    )
    try:
        worker.symbol = "BTC"
        worker.symbol_spec = worker.symbol_spec.__class__(
            symbol="BTC",
            tick_size=1.0,
            tick_value=1.0,
            contract_size=1.0,
            lot_min=0.01,
            lot_max=10.0,
            lot_step=0.01,
            price_precision=0,
            lot_precision=2,
            metadata={"epic": "CS.D.BITCOIN.CFD.IP"},
        )
        blocked = worker._spread_pct_allows_open(current_spread_pct=0.68, current_spread_pips=581.0)
        assert blocked is False
        events = [
            event
            for event in worker.store.load_events()
            if event["message"] == "Trade blocked by spread pct filter"
        ]
        assert events
        payload = events[-1]["payload"]
        assert payload["scope"] == "crypto"
        assert payload["current_spread_pips"] == pytest.approx(581.0)
        assert payload["limit_pct"] == pytest.approx(0.5)
    finally:
        worker.broker.close()
        worker.store.close()


def test_spread_pct_filter_blocks_non_fx_cfd_entry(tmp_path):
    worker = _make_worker(
        tmp_path,
        spread_pct_filter_enabled=True,
        spread_max_pct_cfd=0.15,
    )
    try:
        worker.symbol = "AUS200"
        worker.symbol_spec = worker.symbol_spec.__class__(
            symbol="AUS200",
            tick_size=1.0,
            tick_value=1.0,
            contract_size=1.0,
            lot_min=0.01,
            lot_max=10.0,
            lot_step=0.01,
            price_precision=1,
            lot_precision=2,
            metadata={"epic": "IX.D.ASX.IFS.IP"},
        )
        assert worker._spread_pct_allows_open(current_spread_pct=0.20, current_spread_pips=17.2) is False
    finally:
        worker.broker.close()
        worker.store.close()


def test_connectivity_check_blocks_open_on_high_latency(tmp_path):
    worker = _make_worker(
        tmp_path,
        connectivity_check_enabled=True,
        connectivity_max_latency_ms=500.0,
        connectivity_pong_timeout_sec=2.0,
    )
    try:
        worker.broker.get_connectivity_status = (  # type: ignore[assignment]
            lambda max_latency_ms, pong_timeout_sec: ConnectivityStatus(
                healthy=False,
                reason=f"latency_too_high:900.0ms>{max_latency_ms}ms",
                latency_ms=900.0,
                pong_ok=False,
            )
        )
        allowed = worker._connectivity_allows_open()
        assert allowed is False
        events = [
            event
            for event in worker.store.load_events()
            if event["message"] == "Trade blocked by connectivity check"
        ]
        assert events
        assert events[0]["level"] == "WARN"
    finally:
        worker.broker.close()
        worker.store.close()


def test_connectivity_check_allows_open_on_high_latency_when_pong_ok(tmp_path):
    worker = _make_worker(
        tmp_path,
        connectivity_check_enabled=True,
        connectivity_max_latency_ms=500.0,
        connectivity_pong_timeout_sec=2.0,
    )
    try:
        worker.broker.get_connectivity_status = (  # type: ignore[assignment]
            lambda max_latency_ms, pong_timeout_sec: ConnectivityStatus(
                healthy=False,
                reason=f"latency_too_high:900.0ms>{max_latency_ms}ms",
                latency_ms=900.0,
                pong_ok=True,
            )
        )
        allowed = worker._connectivity_allows_open()
        assert allowed is True
        block_events = [
            event
            for event in worker.store.load_events(limit=50)
            if event.get("message") == "Trade blocked by connectivity check"
        ]
        assert not block_events
        degraded_events = [
            event
            for event in worker.store.load_events(limit=50)
            if event.get("message") == "Connectivity degraded (high latency, trade allowed)"
        ]
        assert degraded_events
    finally:
        worker.broker.close()
        worker.store.close()


def test_connectivity_check_allows_open_on_request_dispatch_timeout(tmp_path):
    worker = _make_worker(
        tmp_path,
        connectivity_check_enabled=True,
        connectivity_max_latency_ms=500.0,
        connectivity_pong_timeout_sec=2.0,
    )
    try:
        worker.broker.get_connectivity_status = (  # type: ignore[assignment]
            lambda max_latency_ms, pong_timeout_sec: ConnectivityStatus(
                healthy=False,
                reason=(
                    "connectivity_check_failed:IG request dispatch timeout "
                    "(worker=app_non_trading, method=GET, path=/session)"
                ),
                latency_ms=None,
                pong_ok=False,
            )
        )
        allowed = worker._connectivity_allows_open()
        assert allowed is True
        block_events = [
            event
            for event in worker.store.load_events(limit=50)
            if event.get("message") == "Trade blocked by connectivity check"
        ]
        assert not block_events
        deferred_events = [
            event
            for event in worker.store.load_events(limit=50)
            if event.get("message")
            == "Connectivity check deferred by request dispatch timeout (trade allowed)"
        ]
        assert deferred_events
    finally:
        worker.broker.close()
        worker.store.close()


def test_connectivity_check_reuses_cached_status_within_probe_interval(tmp_path):
    worker = _make_worker(
        tmp_path,
        connectivity_check_enabled=True,
        connectivity_max_latency_ms=500.0,
        connectivity_pong_timeout_sec=2.0,
        stream_event_cooldown_sec=60.0,
        strategy_param_overrides={"worker_connectivity_check_interval_sec": 30.0},
    )
    try:
        calls = {"count": 0}

        def _fake_connectivity(max_latency_ms, pong_timeout_sec):
            _ = (max_latency_ms, pong_timeout_sec)
            calls["count"] += 1
            return ConnectivityStatus(
                healthy=False,
                reason="latency_too_high:900.0ms>500.0ms",
                latency_ms=900.0,
                pong_ok=False,
            )

        worker.broker.get_connectivity_status = _fake_connectivity  # type: ignore[assignment]

        with patch("xtb_bot.worker.time.time", return_value=1000.0):
            assert worker._connectivity_allows_open() is False
        with patch("xtb_bot.worker.time.time", return_value=1005.0):
            assert worker._connectivity_allows_open() is False

        assert calls["count"] == 1
        events = [
            event
            for event in worker.store.load_events(limit=50)
            if event.get("message") == "Trade blocked by connectivity check"
        ]
        assert len(events) == 1
    finally:
        worker.broker.close()
        worker.store.close()


def test_connectivity_check_uses_allowance_backoff_for_next_probe(tmp_path):
    worker = _make_worker(
        tmp_path,
        connectivity_check_enabled=True,
        connectivity_max_latency_ms=500.0,
        connectivity_pong_timeout_sec=2.0,
        stream_event_cooldown_sec=60.0,
        strategy_param_overrides={"worker_connectivity_check_interval_sec": 5.0},
    )
    try:
        calls = {"count": 0}

        def _fake_connectivity(max_latency_ms, pong_timeout_sec):
            _ = (max_latency_ms, pong_timeout_sec)
            calls["count"] += 1
            return ConnectivityStatus(
                healthy=False,
                reason=(
                    "connectivity_check_failed:IG API GET /session failed: 403 Forbidden "
                    '{"errorCode":"error.public-api.exceeded-account-allowance"}'
                ),
                latency_ms=None,
                pong_ok=False,
            )

        worker.broker.get_connectivity_status = _fake_connectivity  # type: ignore[assignment]

        with patch("xtb_bot.worker.time.time", return_value=1000.0):
            assert worker._connectivity_allows_open() is True
        assert worker._allowance_backoff_until_ts == pytest.approx(1030.0)
        assert worker._next_connectivity_probe_ts == pytest.approx(1030.0)

        with patch("xtb_bot.worker.time.time", return_value=1010.0):
            assert worker._connectivity_allows_open() is True
        assert calls["count"] == 1

        allowance_events = [
            event
            for event in worker.store.load_events(limit=50)
            if event.get("message") == "Broker allowance backoff active"
        ]
        assert not allowance_events
        connectivity_deferred_events = [
            event
            for event in worker.store.load_events(limit=50)
            if event.get("message")
            == "Connectivity check deferred by allowance backoff (trade allowed)"
        ]
        assert connectivity_deferred_events
        blocked_events = [
            event
            for event in worker.store.load_events(limit=50)
            if event.get("message") == "Trade blocked by connectivity check"
        ]
        assert not blocked_events
    finally:
        worker.broker.close()
        worker.store.close()


def test_stream_health_check_blocks_open_on_degraded_stream(tmp_path):
    worker = _make_worker(
        tmp_path,
        stream_health_check_enabled=True,
        stream_max_tick_age_sec=10.0,
        stream_event_cooldown_sec=30.0,
    )
    try:
        status = StreamHealthStatus(
            healthy=False,
            connected=False,
            reason="stream_disconnected:socket_closed",
            symbol="EURUSD",
            reconnect_attempts=2,
            total_reconnects=3,
            last_tick_age_sec=None,
            next_retry_in_sec=1.5,
        )
        worker.broker.get_stream_health_status = (  # type: ignore[assignment]
            lambda symbol, max_tick_age_sec: status
        )

        allowed = worker._stream_health_allows_open(worker._refresh_stream_health())
        assert allowed is False
        events = [
            event
            for event in worker.store.load_events()
            if event["message"] == "Trade blocked by stream health check"
        ]
        assert events
        assert events[0]["level"] == "WARN"
        assert events[0]["payload"]["reason"] == "stream_disconnected:socket_closed"
    finally:
        worker.broker.close()
        worker.store.close()


def test_stream_health_check_blocks_open_on_low_stream_hit_rate(tmp_path):
    worker = _make_worker(
        tmp_path,
        stream_health_check_enabled=True,
        stream_event_cooldown_sec=30.0,
    )
    try:
        status = StreamHealthStatus(
            healthy=True,
            connected=True,
            reason="ok",
            symbol="EURUSD",
            price_requests_total=40,
            stream_hits_total=18,
            rest_fallback_hits_total=22,
            stream_hit_rate_pct=45.0,
        )
        worker.broker.get_stream_health_status = (  # type: ignore[assignment]
            lambda symbol, max_tick_age_sec: status
        )

        allowed = worker._stream_health_allows_open(worker._refresh_stream_health())
        assert allowed is False
        events = [
            event
            for event in worker.store.load_events()
            if event["message"] == "Trade blocked by stream health check"
        ]
        assert events
        payload = events[0]["payload"]
        assert str(payload.get("effective_reason") or "").startswith("stream_low_hit_rate_rest_fallback:")
        assert payload.get("entry_block_override") is True
    finally:
        worker.broker.close()
        worker.store.close()


def test_stream_usage_metrics_are_recorded_in_events(tmp_path):
    worker = _make_worker(
        tmp_path,
        stream_health_check_enabled=True,
        stream_event_cooldown_sec=0.1,
    )
    try:
        status = StreamHealthStatus(
            healthy=True,
            connected=False,
            reason="stream_disconnected_rest_fallback",
            symbol="EURUSD",
            price_requests_total=12,
            stream_hits_total=9,
            rest_fallback_hits_total=3,
            stream_hit_rate_pct=75.0,
        )
        worker.broker.get_stream_health_status = (  # type: ignore[assignment]
            lambda symbol, max_tick_age_sec: status
        )

        worker._refresh_stream_health()

        metrics_events = [
            event
            for event in worker.store.load_events()
            if event["message"] == "Stream usage metrics"
        ]
        assert metrics_events
        payload = metrics_events[0]["payload"]
        assert payload["price_requests_total"] == 12
        assert payload["stream_hits_total"] == 9
        assert payload["rest_fallback_hits_total"] == 3
        assert payload["stream_hit_rate_pct"] == pytest.approx(75.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_hold_reason_logging_is_throttled_but_logs_on_reason_change(tmp_path, monkeypatch):
    worker = _make_worker(tmp_path, hold_reason_log_interval_sec=60.0)
    try:
        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])
        worker.prices.extend([1.1] * 30)

        hold_a = Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"reason": "no_ema_cross", "indicator": "g1"},
        )
        hold_b = Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={"reason": "adx_below_threshold", "indicator": "g1"},
        )

        worker._maybe_record_hold_reason(hold_a, current_spread_pips=0.8)
        now["ts"] += 10.0
        worker._maybe_record_hold_reason(hold_a, current_spread_pips=0.9)  # throttled
        now["ts"] += 10.0
        worker._maybe_record_hold_reason(hold_b, current_spread_pips=1.1)  # reason changed -> log
        now["ts"] += 70.0
        worker._maybe_record_hold_reason(hold_b, current_spread_pips=1.0)  # interval passed -> log

        events = [
            event
            for event in worker.store.load_events()
            if event["message"] == "Signal hold reason"
        ]
        events = list(reversed(events))
        assert len(events) == 3
        assert events[0]["payload"]["reason"] == "no_ema_cross"
        assert events[1]["payload"]["reason"] == "adx_below_threshold"
        assert events[2]["payload"]["reason"] == "adx_below_threshold"
        assert "utc_hour" in events[0]["payload"]
        assert "current_spread_pct" in events[0]["payload"]
        assert "average_spread_pips" in events[0]["payload"]
        assert "allowance_backoff_remaining_sec" in events[0]["payload"]
    finally:
        worker.broker.close()
        worker.store.close()


def test_hold_reason_metadata_basic_verbosity_uses_compact_common_keys(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        strategy_param_overrides={"hold_reason_metadata_verbosity": "basic"},
        hold_reason_log_interval_sec=1.0,
    )
    try:
        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])
        worker.prices.extend([1.1] * 30)

        signal = Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "reason": "no_signal",
                "indicator": "index_hybrid",
                "hold_state": "neutral",
                "soft_filter_reasons": ["higher_tf_bias_mismatch", "volume_below_threshold"],
                "soft_filter_count": 2,
                "zscore": 1.72,
                "debug_blob": {"a": 1, "b": 2},
                "custom_long_list": [1, 2, 3],
            },
        )
        worker._maybe_record_hold_reason(signal, current_spread_pips=0.8)

        events = [
            event
            for event in worker.store.load_events()
            if event["message"] == "Signal hold reason"
        ]
        assert len(events) == 1
        payload = events[0]["payload"]
        assert payload["metadata_verbosity"] == "basic"
        assert payload["metadata"]["reason"] == "no_signal"
        assert payload["metadata"]["indicator"] == "index_hybrid"
        assert payload["metadata"]["hold_state"] == "neutral"
        assert payload["metadata"]["soft_filter_reasons"] == ["higher_tf_bias_mismatch", "volume_below_threshold"]
        assert payload["metadata"]["soft_filter_count"] == 2
        assert payload["metadata"]["zscore"] == pytest.approx(1.72)
        assert "debug_blob" not in payload["metadata"]
        assert "custom_long_list" not in payload["metadata"]
    finally:
        worker.broker.close()
        worker.store.close()


def test_hold_reason_metadata_full_verbosity_keeps_strategy_payload(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        strategy_param_overrides={"hold_reason_metadata_verbosity": "full"},
        hold_reason_log_interval_sec=1.0,
    )
    try:
        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])
        worker.prices.extend([1.1] * 30)

        signal = Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "reason": "no_signal",
                "indicator": "index_hybrid",
                "debug_blob": {"a": 1, "b": 2},
            },
        )
        worker._maybe_record_hold_reason(signal, current_spread_pips=0.8)

        events = [
            event
            for event in worker.store.load_events()
            if event["message"] == "Signal hold reason"
        ]
        assert len(events) == 1
        payload = events[0]["payload"]
        assert payload["metadata_verbosity"] == "full"
        assert payload["metadata"]["debug_blob"] == {"a": 1, "b": 2}
    finally:
        worker.broker.close()
        worker.store.close()


def test_trend_following_hold_summary_logs_top_three_reasons(tmp_path, monkeypatch):
    worker = _make_worker(
        tmp_path,
        strategy_name="trend_following",
        hold_reason_log_interval_sec=3_600.0,
        strategy_param_overrides={
            "trend_following_hold_summary_enabled": True,
            "trend_following_hold_summary_interval_sec": 10.0,
            "trend_following_hold_summary_window": 20,
            "trend_following_hold_summary_min_samples": 6,
        },
    )
    try:
        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])
        worker.prices.extend([1.1] * 30)

        reasons = [
            "no_signal",
            "no_signal",
            "trend_filter_not_confirmed",
            "trend_filter_not_confirmed",
            "no_signal",
            "pullback_not_confirmed",
        ]
        for reason in reasons:
            worker._maybe_record_hold_reason(
                Signal(
                    side=Side.HOLD,
                    confidence=0.0,
                    stop_loss_pips=10.0,
                    take_profit_pips=20.0,
                    metadata={"reason": reason, "indicator": "ema_trend_following"},
                ),
                current_spread_pips=0.8,
            )
            now["ts"] += 2.0

        summaries = [
            event
            for event in worker.store.load_events(limit=30)
            if event["message"] == "Trend hold summary"
        ]
        assert len(summaries) == 1
        payload = summaries[0]["payload"]
        assert payload["strategy"] == "trend_following"
        assert payload["window_size"] == 6
        assert payload["configured_window_size"] == 20
        assert payload["min_samples"] == 6
        assert payload["top_reasons"][0]["reason"] == "no_signal"
        assert payload["top_reasons"][0]["count"] == 3
        assert payload["top_reasons"][1]["reason"] == "trend_filter_not_confirmed"
        assert payload["top_reasons"][1]["count"] == 2
        assert payload["top_reasons"][2]["reason"] == "pullback_not_confirmed"
        assert payload["top_reasons"][2]["count"] == 1
    finally:
        worker.broker.close()
        worker.store.close()


def test_non_trend_worker_does_not_log_trend_hold_summary(tmp_path):
    worker = _make_worker(tmp_path, strategy_name="momentum")
    try:
        worker.prices.extend([1.1] * 30)
        for _ in range(12):
            worker._maybe_record_hold_reason(
                Signal(
                    side=Side.HOLD,
                    confidence=0.0,
                    stop_loss_pips=10.0,
                    take_profit_pips=20.0,
                    metadata={"reason": "no_signal", "indicator": "momentum"},
                ),
                current_spread_pips=0.8,
            )
        summaries = [
            event
            for event in worker.store.load_events(limit=40)
            if event["message"] == "Trend hold summary"
        ]
        assert summaries == []
    finally:
        worker.broker.close()
        worker.store.close()


def test_indicator_debug_logs_each_signal_when_enabled(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.debug_indicators_enabled = True
        worker.debug_indicators_interval_sec = 0.0
        worker.prices.extend([1.1] * 30)

        signal = Signal(
            side=Side.HOLD,
            confidence=0.0,
            stop_loss_pips=10.0,
            take_profit_pips=20.0,
            metadata={
                "reason": "no_ema_cross",
                "indicator": "g1",
                "fast_ema": 1.1001,
                "slow_ema": 1.0998,
                "adx": 23.5,
            },
        )

        worker._maybe_record_indicator_debug(signal, current_spread_pips=0.8)
        worker._maybe_record_indicator_debug(signal, current_spread_pips=0.9)

        events = [
            event
            for event in worker.store.load_events()
            if event["message"] == "Strategy indicator snapshot"
        ]
        events = list(reversed(events))
        assert len(events) == 2
        assert events[0]["payload"]["signal_side"] == "hold"
        assert events[0]["payload"]["fast_ema"] == pytest.approx(1.1001)
        assert events[0]["payload"]["adx"] == pytest.approx(23.5)
    finally:
        worker.broker.close()
        worker.store.close()


def test_indicator_debug_respects_interval_throttle(tmp_path, monkeypatch):
    worker = _make_worker(tmp_path)
    try:
        now = {"ts": 1_700_000_000.0}
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: now["ts"])
        worker.debug_indicators_enabled = True
        worker.debug_indicators_interval_sec = 60.0
        worker.prices.extend([1.1] * 30)

        signal = Signal(
            side=Side.BUY,
            confidence=0.7,
            stop_loss_pips=15.0,
            take_profit_pips=30.0,
            metadata={"indicator": "g1", "trend_signal": "ema_trend_up"},
        )

        worker._maybe_record_indicator_debug(signal, current_spread_pips=0.8)
        now["ts"] += 10.0
        worker._maybe_record_indicator_debug(signal, current_spread_pips=0.8)  # throttled
        now["ts"] += 61.0
        worker._maybe_record_indicator_debug(signal, current_spread_pips=0.8)

        events = [
            event
            for event in worker.store.load_events()
            if event["message"] == "Strategy indicator snapshot"
        ]
        events = list(reversed(events))
        assert len(events) == 2
        assert events[0]["payload"]["signal_side"] == "buy"
        assert events[0]["payload"]["trend_signal"] == "ema_trend_up"
    finally:
        worker.broker.close()
        worker.store.close()


def test_magic_comment_is_unique_and_prefixed(tmp_path):
    worker = _make_worker(tmp_path, bot_magic_prefix="XTBMAG", bot_magic_instance="ABCD12")
    try:
        comment_one, uid_one = worker._build_magic_comment()
        comment_two, uid_two = worker._build_magic_comment()
        assert uid_one != uid_two
        assert comment_one != comment_two
        assert comment_one.startswith("XTBMAG:ABCD12:")
        assert comment_two.startswith("XTBMAG:ABCD12:")
        assert len(comment_one) <= 32
        assert len(comment_two) <= 32
    finally:
        worker.broker.close()
        worker.store.close()


def test_symbol_auto_disables_after_consecutive_epic_unavailable_errors(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.symbol_auto_disable_on_epic_unavailable = True
        worker.symbol_auto_disable_epic_unavailable_threshold = 3
        worker.symbol_auto_disable_min_error_window_sec = 0.0

        err = (
            "IG API GET /markets/CS.D.GOLD.CFD.IP failed: 404 Not Found "
            '{"errorCode":"error.service.marketdata.instrument.epic.unavailable"}'
        )
        worker._register_broker_error(err)
        worker._register_broker_error(err)
        assert worker._symbol_disabled is False

        worker._register_broker_error(err)
        assert worker._symbol_disabled is True
        assert worker._symbol_disabled_reason == "repeated_epic_unavailable_errors"

        events = [
            event
            for event in worker.store.load_events()
            if event["message"] == "Symbol disabled by safety guard"
        ]
        assert events
        payload = events[0]["payload"]
        assert payload["epic_unavailable_consecutive_errors"] == 3
        assert payload["disable_threshold"] == 3
    finally:
        worker.broker.close()
        worker.store.close()


def test_epic_unavailable_counter_resets_on_other_broker_errors(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.symbol_auto_disable_on_epic_unavailable = True
        worker.symbol_auto_disable_epic_unavailable_threshold = 2

        unavailable = (
            "IG API GET /markets/CS.D.GOLD.CFD.IP failed: 404 Not Found "
            '{"errorCode":"error.service.marketdata.instrument.epic.unavailable"}'
        )
        worker._register_broker_error(unavailable)
        assert worker._epic_unavailable_consecutive_errors == 1

        worker._register_broker_error("IG API GET /markets failed: 403 Forbidden")
        assert worker._epic_unavailable_consecutive_errors == 0

        worker._register_broker_error(unavailable)
        assert worker._epic_unavailable_consecutive_errors == 1
        assert worker._symbol_disabled is False
    finally:
        worker.broker.close()
        worker.store.close()


def test_symbol_auto_disables_after_consecutive_quote_unavailable_errors(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.symbol_auto_disable_on_epic_unavailable = True
        worker.symbol_auto_disable_epic_unavailable_threshold = 3
        worker.symbol_auto_disable_min_error_window_sec = 0.0

        err = (
            "IG snapshot for AAPL does not contain bid/offer "
            "(epic=UA.D.AAPL.CASH.IP market_status=TRADEABLE raw_bid=None raw_offer=None)"
        )
        worker._register_broker_error(err)
        worker._register_broker_error(err)
        assert worker._symbol_disabled is False

        worker._register_broker_error(err)
        assert worker._symbol_disabled is True
        assert worker._symbol_disabled_reason == "repeated_quote_unavailable_errors"

        events = [
            event
            for event in worker.store.load_events()
            if event["message"] == "Symbol disabled by safety guard"
        ]
        assert events
        payload = events[0]["payload"]
        assert payload["quote_unavailable_consecutive_errors"] == 3
        assert payload["disable_threshold"] == 3
    finally:
        worker.broker.close()
        worker.store.close()


def test_symbol_auto_disable_requires_min_error_window(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.symbol_auto_disable_on_epic_unavailable = True
        worker.symbol_auto_disable_epic_unavailable_threshold = 3
        worker.symbol_auto_disable_min_error_window_sec = 120.0

        err = (
            "IG API GET /markets/CS.D.GOLD.CFD.IP failed: 404 Not Found "
            '{"errorCode":"error.service.marketdata.instrument.epic.unavailable"}'
        )

        with patch("xtb_bot.worker.time.time", side_effect=[1000.0, 1010.0, 1020.0]):
            worker._register_broker_error(err)
            worker._register_broker_error(err)
            worker._register_broker_error(err)

        assert worker._symbol_disabled is False

        with patch("xtb_bot.worker.time.time", return_value=1135.0):
            worker._register_broker_error(err)

        assert worker._symbol_disabled is True
        assert worker._symbol_disabled_reason == "repeated_epic_unavailable_errors"
    finally:
        worker.broker.close()
        worker.store.close()


def test_run_stops_polling_broker_after_symbol_disabled(tmp_path):
    worker = _make_worker(tmp_path)
    thread: threading.Thread | None = None
    try:
        worker.symbol_auto_disable_on_epic_unavailable = True
        worker.symbol_auto_disable_epic_unavailable_threshold = 1
        worker.symbol_auto_disable_min_error_window_sec = 0.0

        calls = {"price": 0}

        def _boom(symbol: str):
            calls["price"] += 1
            raise BrokerError(
                'IG API GET /markets/CS.D.GOLD.CFD.IP failed: 404 Not Found {"errorCode":"error.service.marketdata.instrument.epic.unavailable"}'
            )

        worker.broker.get_price = _boom  # type: ignore[assignment]

        thread = threading.Thread(target=worker.run, daemon=True)
        thread.start()
        worker.stop_event.wait(0.5)
        # Ensure the first failure happened and symbol got disabled.
        assert calls["price"] >= 1
        assert worker._symbol_disabled is True

        previous_calls = calls["price"]
        # Let worker spin while disabled; it should not call get_price again.
        worker.stop_event.wait(0.3)
        assert calls["price"] == previous_calls
    finally:
        worker.stop_event.set()
        if thread is not None:
            thread.join(timeout=1.0)
        worker.broker.close()
        worker.store.close()


def test_run_does_not_fetch_account_snapshot_when_signal_is_hold_and_no_position(tmp_path):
    worker = _make_worker(tmp_path)
    thread: threading.Thread | None = None
    try:
        worker.strategy = _FakeStrategy(Side.HOLD)
        worker.poll_interval_sec = 0.05

        calls = {"snapshot": 0}
        original_get_snapshot = worker.broker.get_account_snapshot

        def _fake_snapshot():
            calls["snapshot"] += 1
            return original_get_snapshot()

        worker.broker.get_account_snapshot = _fake_snapshot  # type: ignore[assignment]

        thread = threading.Thread(target=worker.run, daemon=True)
        thread.start()
        worker.stop_event.wait(0.25)
        worker.stop_event.set()
        thread.join(timeout=1.0)

        assert calls["snapshot"] == 0
    finally:
        worker.stop_event.set()
        if thread is not None and thread.is_alive():
            thread.join(timeout=1.0)
        worker.broker.close()
        worker.store.close()


def test_run_blocks_open_when_signal_confidence_below_threshold(tmp_path):
    worker = _make_worker(tmp_path, min_confidence_for_entry=0.8)
    thread: threading.Thread | None = None
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.BUY,
                confidence=0.55,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={"source": "test"},
            )
        )
        worker.poll_interval_sec = 0.05

        thread = threading.Thread(target=worker.run, daemon=True)
        thread.start()
        worker.stop_event.wait(0.25)
        worker.stop_event.set()
        thread.join(timeout=1.0)

        assert worker.position_book.get("EURUSD") is None
        blocked_events = [
            event
            for event in worker.store.load_events()
            if event["message"] == "Trade blocked by confidence threshold"
        ]
        assert blocked_events
        assert len(blocked_events) == 1
        assert worker.iteration < 20
        payload = blocked_events[0]["payload"]
        assert payload["confidence"] == pytest.approx(0.55)
        assert payload["min_confidence_for_entry"] == pytest.approx(0.8)
        assert payload["signal"] == "buy"
    finally:
        worker.stop_event.set()
        if thread is not None and thread.is_alive():
            thread.join(timeout=1.0)
        worker.broker.close()
        worker.store.close()


def test_worker_execution_calibrator_blocks_high_cost_entry(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        calibrated = worker._execution_calibrated_entry_signal(
            Signal(
                side=Side.BUY,
                confidence=0.9,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={"indicator": "momentum"},
            ),
            current_spread_pips=4.5,
            current_spread_pct=0.25,
        )
        assert calibrated.side == Side.HOLD
        assert calibrated.metadata.get("reason") == "execution_cost_too_high"
        assert calibrated.metadata.get("blocked_signal_side") == "buy"
        assert "spread_cost_exceeds_stop_budget" in calibrated.metadata.get("execution_reasons", [])
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_execution_calibrator_penalizes_market_take_of_limit_hint(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        calibrated = worker._execution_calibrated_entry_signal(
            Signal(
                side=Side.BUY,
                confidence=0.9,
                stop_loss_pips=20.0,
                take_profit_pips=60.0,
                metadata={
                    "indicator": "momentum",
                    "suggested_order_type": "limit",
                    "entry_quality_penalty": 0.20,
                },
            ),
            current_spread_pips=2.0,
            current_spread_pct=0.05,
        )
        assert calibrated.side == Side.BUY
        assert calibrated.confidence < 0.9
        assert calibrated.metadata.get("execution_quality_status") == "adjusted"
        assert "market_execution_overrides_limit_hint" in calibrated.metadata.get("execution_reasons", [])
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_execution_calibrator_blocks_recent_directional_spike_entry(tmp_path):
    worker = _make_worker(
        tmp_path,
        symbol="US100",
        strategy_param_overrides={
            "entry_spike_guard_enabled": True,
            "entry_spike_guard_trend_only": True,
            "entry_spike_guard_lookback_samples": 2,
            "entry_spike_guard_max_window_sec": 10.0,
            "entry_spike_guard_atr_multiplier": 0.5,
            "entry_spike_guard_spread_multiplier": 2.0,
        },
    )
    try:
        base_ts = time.time()
        worker.prices.extend([20000.0, 20001.0, 20004.0])
        worker.price_timestamps.extend([base_ts - 2.0, base_ts - 1.0, base_ts])
        worker.volumes.extend([0.0, 0.0, 0.0])
        calibrated = worker._execution_calibrated_entry_signal(
            Signal(
                side=Side.BUY,
                confidence=0.9,
                stop_loss_pips=20.0,
                take_profit_pips=40.0,
                metadata={
                    "indicator": "momentum",
                    "atr_pips": 4.0,
                    "trend_signal": "ema_trend_up",
                },
            ),
            current_spread_pips=0.5,
            current_spread_pct=0.02,
        )
        assert calibrated.side == Side.HOLD
        assert calibrated.metadata.get("reason") == "entry_spike_detected"
        assert calibrated.metadata.get("blocked_signal_side") == "buy"
        assert float(calibrated.metadata.get("directional_move_pips") or 0.0) == pytest.approx(4.0)
        assert float(calibrated.metadata.get("spike_guard_threshold_pips") or 0.0) == pytest.approx(2.0)
        assert "entry_spike_detected" in calibrated.metadata.get("execution_reasons", [])
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_execution_calibrator_does_not_block_mean_reversion_signal_with_trend_only_spike_guard(tmp_path):
    worker = _make_worker(
        tmp_path,
        symbol="US100",
        strategy_name="mean_reversion_bb",
        strategy_param_overrides={
            "entry_spike_guard_enabled": True,
            "entry_spike_guard_trend_only": True,
            "entry_spike_guard_lookback_samples": 2,
            "entry_spike_guard_max_window_sec": 10.0,
            "entry_spike_guard_atr_multiplier": 0.5,
            "entry_spike_guard_spread_multiplier": 2.0,
        },
    )
    try:
        base_ts = time.time()
        worker.prices.extend([20000.0, 20001.0, 20004.0])
        worker.price_timestamps.extend([base_ts - 2.0, base_ts - 1.0, base_ts])
        worker.volumes.extend([0.0, 0.0, 0.0])
        calibrated = worker._execution_calibrated_entry_signal(
            Signal(
                side=Side.BUY,
                confidence=0.9,
                stop_loss_pips=20.0,
                take_profit_pips=40.0,
                metadata={
                    "indicator": "mean_reversion_bb",
                    "atr_pips": 4.0,
                    "trend_signal": "reclaim_from_lower_band",
                },
            ),
            current_spread_pips=0.5,
            current_spread_pct=0.02,
        )
        assert calibrated.side == Side.BUY
        assert calibrated.metadata.get("reason") != "entry_spike_detected"
    finally:
        worker.broker.close()
        worker.store.close()


def test_entry_spike_guard_ignores_exactly_insufficient_lookback_samples(tmp_path):
    worker = _make_worker(
        tmp_path,
        symbol="US100",
        strategy_param_overrides={
            "entry_spike_guard_enabled": True,
            "entry_spike_guard_trend_only": True,
            "entry_spike_guard_lookback_samples": 2,
            "entry_spike_guard_max_window_sec": 10.0,
            "entry_spike_guard_atr_multiplier": 0.5,
            "entry_spike_guard_spread_multiplier": 2.0,
        },
    )
    try:
        base_ts = time.time()
        worker.prices.extend([20000.0, 20004.0])
        worker.price_timestamps.extend([base_ts - 1.0, base_ts])

        payload = worker._entry_spike_guard_block_payload(
            Signal(
                side=Side.BUY,
                confidence=0.9,
                stop_loss_pips=20.0,
                take_profit_pips=40.0,
                metadata={
                    "indicator": "momentum",
                    "atr_pips": 4.0,
                    "trend_signal": "ema_trend_up",
                },
            ),
            current_spread_pips=0.5,
        )

        assert payload is None
    finally:
        worker.broker.close()
        worker.store.close()


def test_run_uses_signal_confidence_cap_for_entry_threshold(tmp_path):
    worker = _make_worker(tmp_path, min_confidence_for_entry=0.8)
    thread: threading.Thread | None = None
    try:
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.BUY,
                confidence=0.6,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "source": "test",
                    "indicator": "index_hybrid",
                    "confidence_threshold_cap": 0.55,
                },
            )
        )
        worker.poll_interval_sec = 0.05

        thread = threading.Thread(target=worker.run, daemon=True)
        thread.start()
        worker.stop_event.wait(0.25)
        worker.stop_event.set()
        thread.join(timeout=1.0)

        assert worker.position_book.get("EURUSD") is not None
        blocked_events = [
            event
            for event in worker.store.load_events()
            if event["message"] == "Trade blocked by confidence threshold"
        ]
        assert not blocked_events
    finally:
        worker.stop_event.set()
        if thread is not None and thread.is_alive():
            thread.join(timeout=1.0)
        worker.broker.close()
        worker.store.close()


def test_execution_mode_prefers_mode_specific_confidence_threshold(tmp_path):
    worker = _make_worker(
        tmp_path,
        mode=RunMode.EXECUTION,
        min_confidence_for_entry=0.55,
        strategy_param_overrides={"momentum_execution_min_confidence_for_entry": 0.85},
    )
    try:
        assert worker.min_confidence_for_entry == pytest.approx(0.85)
    finally:
        worker.broker.close()
        worker.store.close()


def test_execution_mode_prefers_mode_specific_trade_cooldown(tmp_path):
    worker = _make_worker(
        tmp_path,
        mode=RunMode.EXECUTION,
        momentum_trade_cooldown_sec=120.0,
        strategy_param_overrides={"momentum_execution_trade_cooldown_sec": 45.0},
    )
    try:
        assert worker.trade_cooldown_sec == pytest.approx(45.0)
        assert worker.trade_cooldown_win_sec == pytest.approx(45.0)
        assert worker.trade_cooldown_loss_sec == pytest.approx(45.0)
        assert worker.trade_cooldown_flat_sec == pytest.approx(45.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_execution_mode_prefers_mode_specific_g1_executor_controls(tmp_path):
    worker = _make_worker(
        tmp_path,
        strategy_name="g1",
        mode=RunMode.EXECUTION,
        min_confidence_for_entry=0.40,
        momentum_trade_cooldown_sec=120.0,
        strategy_param_overrides={
            "g1_execution_min_confidence_for_entry": 0.83,
            "g1_execution_trade_cooldown_sec": 45.0,
            "g1_protective_exit_enabled": False,
            "g1_protective_exit_loss_ratio": 0.74,
            "g1_protective_exit_allow_adx_regime_loss": False,
            "g1_debug_indicators": True,
            "g1_debug_indicators_interval_sec": 11.0,
            "g1_worker_connectivity_check_interval_sec": 17.0,
        },
    )
    try:
        assert worker.min_confidence_for_entry == pytest.approx(0.83)
        assert worker.trade_cooldown_sec == pytest.approx(45.0)
        assert worker.trade_cooldown_win_sec == pytest.approx(45.0)
        assert worker.trade_cooldown_loss_sec == pytest.approx(45.0)
        assert worker.trade_cooldown_flat_sec == pytest.approx(45.0)
        assert worker.protective_exit_enabled is False
        assert worker.protective_exit_loss_ratio == pytest.approx(0.74)
        assert worker.protective_exit_allow_adx_regime_loss is False
        assert worker.g1_protective_exit_enabled is False
        assert worker.g1_protective_exit_loss_ratio == pytest.approx(0.74)
        assert worker.g1_protective_exit_allow_adx_regime_loss is False
        assert worker.debug_indicators_enabled is True
        assert worker.debug_indicators_interval_sec == pytest.approx(11.0)
        assert worker.connectivity_check_interval_sec == pytest.approx(17.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_restores_price_history_from_state_store(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.store.append_price_sample("EURUSD", ts=1_700_000_001.0, close=1.1001, volume=10.0, max_rows_per_symbol=500)
        worker.store.append_price_sample("EURUSD", ts=1_700_000_002.0, close=1.1002, volume=20.0, max_rows_per_symbol=500)
        worker.store.append_price_sample("EURUSD", ts=1_700_000_003.0, close=1.1003, volume=30.0, max_rows_per_symbol=500)
    finally:
        worker.broker.close()
        worker.store.close()

    worker_restored = _make_worker(tmp_path)
    try:
        restored_prices = list(worker_restored.prices)
        restored_timestamps = list(worker_restored.price_timestamps)
        assert len(restored_prices) >= 3
        assert restored_prices[-3:] == pytest.approx([1.1001, 1.1002, 1.1003])
        assert restored_timestamps[-3:] == pytest.approx(
            [1_700_000_001.0, 1_700_000_002.0, 1_700_000_003.0]
        )
    finally:
        worker_restored.broker.close()
        worker_restored.store.close()


def test_worker_price_history_retention_is_not_limited_to_runtime_buffer(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        for idx in range(1, 201):
            worker._cache_price_sample(
                timestamp=float(1_700_000_000 + idx),
                close=1.1000 + idx * 0.00001,
                volume=10.0 + idx,
            )

        stored = worker.store.load_recent_price_history("EURUSD", limit=500)
        assert len(stored) == 200
    finally:
        worker.broker.close()
        worker.store.close()


def test_normalize_tick_timestamp_for_history_uses_local_fallback_on_stalled_ts(tmp_path, monkeypatch):
    worker = _make_worker(tmp_path)
    try:
        worker.price_timestamps.append(1_700_000_000.0)
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: 1_800_000_000.0)

        normalized = worker._normalize_tick_timestamp_for_history(1_700_000_000.0)
        assert normalized > 1_700_000_000.0
        assert normalized == pytest.approx(1_800_000_000.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_normalize_tick_timestamp_for_history_reanchors_future_history(tmp_path, monkeypatch):
    worker = _make_worker(tmp_path)
    try:
        worker.price_timestamps.append(1_900_000_000.0)
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: 1_800_000_000.0)

        normalized = worker._normalize_tick_timestamp_for_history(1_700_000_000.0)
        assert float(worker.price_timestamps[-1]) == pytest.approx(1_800_000_000.0)
        assert normalized > 1_800_000_000.0
        assert normalized <= 1_800_000_001.0
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_restores_and_repairs_compressed_timestamps_for_closed_candle_strategies(tmp_path, monkeypatch):
    worker = _make_worker(tmp_path, strategy_name="g1")
    try:
        base_ts = 1_700_000_000.0
        for idx in range(200):
            worker.store.append_price_sample(
                "EURUSD",
                ts=base_ts + idx * 0.001,
                close=1.1000 + idx * 0.00001,
                volume=10.0 + idx,
                max_rows_per_symbol=2_000,
            )
    finally:
        worker.broker.close()
        worker.store.close()

    restored_worker = _make_worker(tmp_path, strategy_name="g1")
    try:
        restored_worker.prices.clear()
        restored_worker.price_timestamps.clear()
        restored_worker.volumes.clear()
        monkeypatch.setattr("xtb_bot.worker.time.time", lambda: 1_800_000_000.0)

        restored_worker._restore_price_history()

        timestamps = list(restored_worker.price_timestamps)
        assert len(timestamps) >= 100
        assert all(timestamps[idx] > timestamps[idx - 1] for idx in range(1, len(timestamps)))
        span = timestamps[-1] - timestamps[0]
        assert span >= 60.0
    finally:
        restored_worker.broker.close()
        restored_worker.store.close()


def test_worker_builds_high_tf_context_from_candle_history_store(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        strategy = _HighTfContextCaptureStrategy()
        worker.strategy = strategy
        base_ts = 1_700_000_100.0
        for idx in range(220):
            worker.store.append_candle_sample(
                worker.symbol,
                resolution_sec=900,
                ts=base_ts + idx * 900.0,
                close=1.1000 + idx * 0.0002,
                volume=10.0 + idx,
                max_rows_per_symbol=500,
            )

        base_context = worker._build_strategy_context(
            current_volume=None,
            current_spread_pips=1.2,
        )
        assert len(base_context.prices) == 0

        effective_context = worker._build_strategy_context_for_strategy(
            strategy,
            base_context=base_context,
        )

        assert len(effective_context.prices) >= strategy.min_history
        assert effective_context.prices[-1] == pytest.approx(1.1000 + 219 * 0.0002)
        assert effective_context.current_spread_pips == pytest.approx(1.2)
        assert all(
            effective_context.timestamps[idx] > effective_context.timestamps[idx - 1]
            for idx in range(1, len(effective_context.timestamps))
        )
    finally:
        worker.broker.close()
        worker.store.close()


def test_build_strategy_context_uses_atomic_history_snapshot(tmp_path, monkeypatch):
    worker = _make_worker(tmp_path)
    try:
        monkeypatch.setattr(
            worker._history_runtime,
            "history_snapshot",
            lambda: (
                [1.1010, 1.1015, 1.1020],
                [100.0, 101.0, 102.0],
                [5.0, 6.0, 7.0],
            ),
        )
        worker.prices = _ExplodingDeque([1.0])
        worker.price_timestamps = _ExplodingDeque([1.0])
        worker.volumes = _ExplodingDeque([1.0])

        context = worker._build_strategy_context(
            current_volume=7.0,
            current_spread_pips=1.2,
        )

        assert context.prices == [1.1010, 1.1015, 1.1020]
        assert context.timestamps == [100.0, 101.0, 102.0]
        assert context.volumes == [5.0, 6.0, 7.0]
    finally:
        worker.broker.close()
        worker.store.close()


def test_worker_high_tf_context_preserves_candle_ohlc_shape(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        strategy = _HighTfContextCaptureStrategy()
        worker.strategy = strategy
        base_ts = 1_700_000_100.0
        candle_rows = [
            (1.1000, 1.1030, 1.0990, 1.1020),
            (1.1020, 1.1060, 1.1010, 1.1050),
            (1.1050, 1.1070, 1.1040, 1.1060),
            (1.1060, 1.1090, 1.1030, 1.1080),
            (1.1080, 1.1100, 1.1070, 1.1090),
            (1.1090, 1.1110, 1.1080, 1.1100),
            (1.1100, 1.1120, 1.1090, 1.1110),
            (1.1110, 1.1140, 1.1100, 1.1130),
            (1.1130, 1.1150, 1.1120, 1.1140),
            (1.1140, 1.1160, 1.1130, 1.1150),
            (1.1150, 1.1180, 1.1140, 1.1170),
            (1.1170, 1.1190, 1.1160, 1.1180),
        ]
        for idx, (open_price, high_price, low_price, close_price) in enumerate(candle_rows):
            worker.store.append_candle_sample(
                worker.symbol,
                resolution_sec=900,
                ts=base_ts + idx * 900.0,
                close=close_price,
                open_price=open_price,
                high_price=high_price,
                low_price=low_price,
                volume=10.0 + idx,
                max_rows_per_symbol=500,
            )

        effective_context = worker._build_strategy_context_for_strategy(
            strategy,
            base_context=worker._build_strategy_context(current_volume=None, current_spread_pips=1.2),
        )

        assert effective_context.prices[:4] == pytest.approx([1.1000, 1.1030, 1.0990, 1.1020])
        assert effective_context.prices[4:8] == pytest.approx([1.1020, 1.1060, 1.1010, 1.1050])
        assert effective_context.volumes[:4] == pytest.approx([0.0, 0.0, 0.0, 10.0])
        assert effective_context.volumes[4:8] == pytest.approx([0.0, 0.0, 0.0, 11.0])
    finally:
        worker.broker.close()
        worker.store.close()


def test_manual_close_sync_poll_interval_is_throttled_for_stale_open_position(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-manual-close-stale-poll-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)
        worker.manual_close_sync_interval_sec = 5.0
        worker._manual_close_sync_next_check_ts = 0.0
        worker._pending_close_verification_position_id = None
        worker._pending_close_retry_position_id = None

        def _fake_close_sync(_position_id, **kwargs):
            _ = kwargs
            return None

        worker.broker.get_position_close_sync = _fake_close_sync  # type: ignore[attr-defined]
        tick = PriceTick(symbol="EURUSD", bid=1.1015, ask=1.1017, timestamp=10.0)
        with patch("xtb_bot.worker.time.time", return_value=40_001.0):
            handled = worker._maybe_reconcile_execution_manual_close(position, tick)

        assert handled is False
        assert worker._manual_close_sync_next_check_ts == pytest.approx(40_001.0 + 120.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_manual_close_sync_poll_interval_uses_short_and_medium_stale_tiers(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-manual-close-stale-poll-2",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.manual_close_sync_interval_sec = 5.0
        worker._pending_close_verification_position_id = None
        worker._pending_close_retry_position_id = None

        short_tier_interval = worker._manual_close_sync_interval_for_position(
            position,
            now=1.0 + (45.0 * 60.0),
        )
        medium_tier_interval = worker._manual_close_sync_interval_for_position(
            position,
            now=1.0 + (3.0 * 60.0 * 60.0),
        )

        assert short_tier_interval == pytest.approx(3.0)
        assert medium_tier_interval == pytest.approx(10.0)
    finally:
        worker.broker.close()
        worker.store.close()


def test_manual_close_sync_poll_interval_stays_fast_for_pending_close_verification(tmp_path):
    worker = _make_worker(tmp_path, mode=RunMode.EXECUTION)
    try:
        position = Position(
            position_id="deal-manual-close-pending-poll-1",
            symbol="EURUSD",
            side=Side.BUY,
            volume=0.1,
            open_price=1.1000,
            stop_loss=1.0990,
            take_profit=1.1020,
            opened_at=1.0,
            status="open",
        )
        worker.position_book.upsert(position)
        worker.store.upsert_trade(position, worker.name, worker.strategy_name, worker.mode.value)
        worker.manual_close_sync_interval_sec = 5.0
        worker._manual_close_sync_next_check_ts = 0.0
        worker._pending_close_verification_position_id = position.position_id

        def _fake_close_sync(_position_id, **kwargs):
            _ = kwargs
            return None

        worker.broker.get_position_close_sync = _fake_close_sync  # type: ignore[attr-defined]
        tick = PriceTick(symbol="EURUSD", bid=1.1015, ask=1.1017, timestamp=10.0)
        with patch("xtb_bot.worker.time.time", return_value=40_001.0):
            handled = worker._maybe_reconcile_execution_manual_close(position, tick)

        assert handled is False
        assert worker._manual_close_sync_next_check_ts == pytest.approx(40_001.0 + 5.0)
    finally:
        worker.broker.close()
        worker.store.close()
