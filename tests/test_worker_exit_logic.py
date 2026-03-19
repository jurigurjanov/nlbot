from __future__ import annotations

import threading
import time
from unittest.mock import patch

import pytest

from xtb_bot.client import BrokerError, MockBrokerClient
from xtb_bot.config import RiskConfig
from xtb_bot.models import ConnectivityStatus, NewsEvent, Position, PriceTick, RunMode, Side, Signal, StreamHealthStatus, SymbolSpec
from xtb_bot.models import AccountSnapshot
from xtb_bot.position_book import PositionBook
from xtb_bot.risk_manager import RiskManager
from xtb_bot.state_store import StateStore
from xtb_bot.worker import SymbolWorker


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


def _make_worker(
    tmp_path,
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
    strategy_param_overrides: dict[str, object] | None = None,
    bot_magic_prefix: str = "XTBBOT",
    bot_magic_instance: str = "TEST01",
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
    if momentum_trade_cooldown_win_sec is not None:
        strategy_params["momentum_trade_cooldown_win_sec"] = momentum_trade_cooldown_win_sec
    if momentum_trade_cooldown_loss_sec is not None:
        strategy_params["momentum_trade_cooldown_loss_sec"] = momentum_trade_cooldown_loss_sec
    if momentum_trade_cooldown_flat_sec is not None:
        strategy_params["momentum_trade_cooldown_flat_sec"] = momentum_trade_cooldown_flat_sec
    if strategy_param_overrides:
        strategy_params.update(strategy_param_overrides)

    worker = SymbolWorker(
        symbol="EURUSD",
        mode=mode,
        strategy_name=strategy_name,
        strategy_params=strategy_params,
        broker=broker,
        store=store,
        risk=risk,
        position_book=PositionBook(),
        stop_event=threading.Event(),
        poll_interval_sec=1.0,
        poll_jitter_sec=0.0,
        default_volume=0.0,
        bot_magic_prefix=bot_magic_prefix,
        bot_magic_instance=bot_magic_instance,
    )
    worker.symbol_spec = broker.get_symbol_spec("EURUSD")
    return worker


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
        worker.symbol = "WTI"
        assert worker._pip_size_fallback() == pytest.approx(1.0)
        worker.symbol = "BRENT"
        assert worker._pip_size_fallback() == pytest.approx(1.0)
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


def test_worker_state_persistence_is_throttled_for_unchanged_state(tmp_path):
    worker = _make_worker(tmp_path)
    saved_states = []

    try:
        worker.worker_state_flush_interval_sec = 5.0
        worker.store.save_worker_state = lambda state: saved_states.append(state)  # type: ignore[method-assign]

        with patch("xtb_bot.worker.time.time", side_effect=[100.0, 100.0, 101.0, 106.0, 106.0]):
            worker._save_state(last_price=1.1000, last_error=None)
            worker._save_state(last_price=1.1001, last_error=None)
            worker._save_state(last_price=1.1002, last_error=None)

        assert len(saved_states) == 2
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
        assert result.timestamp == pytest.approx(200.0)
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


def test_worker_caps_market_data_allowance_backoff_to_five_seconds(tmp_path):
    worker = _make_worker(tmp_path, stream_event_cooldown_sec=60.0)
    try:
        worker.broker.get_public_api_backoff_remaining_sec = lambda: 30.0  # type: ignore[assignment]
        remaining = worker._handle_allowance_backoff_error(
            'IG API GET /markets/IX.D.SPTRD.DAILY.IP failed: 403 Forbidden '
            '{"errorCode":"error.public-api.exceeded-api-key-allowance"}'
        )
        assert remaining == pytest.approx(5.0)
        assert worker._allowance_backoff_until_ts == pytest.approx(time.time() + 5.0, abs=1.0)
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


def test_dynamic_exit_reason_ignored_for_non_index_hybrid_strategy(tmp_path):
    worker = _make_worker(tmp_path)
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


def test_mean_reversion_dynamic_exit_reason_on_exit_hint(tmp_path):
    worker = _make_worker(tmp_path)
    try:
        worker.strategy_name = "mean_reversion"
        worker.strategy = _FixedSignalStrategy(
            Signal(
                side=Side.HOLD,
                confidence=0.0,
                stop_loss_pips=10.0,
                take_profit_pips=20.0,
                metadata={
                    "indicator": "mean_reversion",
                    "reason": "mean_reversion_target_reached",
                    "exit_hint": "close_on_mean_reversion",
                    "zscore": 0.03,
                },
            )
        )
        worker.prices.append(1.1000)

        position = Position(
            position_id="paper-mr-1",
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
        reason = worker._strategy_dynamic_exit_reason(position, bid=1.1, ask=1.1002, signal=signal)
        assert reason == "strategy_exit:mean_reversion:mean_reverted"
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


def test_g1_dynamic_exit_reason_triggers_on_ema_invalidation_near_stop(tmp_path):
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


def test_g1_dynamic_exit_reason_does_not_trigger_while_position_is_in_profit(tmp_path):
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
        reason = worker._strategy_dynamic_exit_reason(position, bid=1.1002, ask=1.1004, signal=signal)
        assert reason is None
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


def test_close_position_finalizes_locally_when_broker_already_closed_position(tmp_path):
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
        assert "Broker position missing on close, finalized locally" in messages
        closed = [event for event in events if event.get("message") == "Position closed"]
        assert closed
        reason = str((closed[0].get("payload") or {}).get("reason") or "")
        assert reason.startswith("broker_position_missing_on_close:")
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


def test_handle_missing_broker_position_error_reconciles_active_position(tmp_path):
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
        assert worker.position_book.get("EURUSD") is None

        open_positions = worker.store.load_open_positions(mode="execution")
        assert "EURUSD" not in open_positions

        closed_events = [
            event for event in worker.store.load_events(limit=20) if event.get("message") == "Position closed"
        ]
        assert closed_events
        payload = closed_events[0].get("payload") or {}
        assert float(payload.get("close_price") or 0.0) == pytest.approx(1.0990)
        assert str(payload.get("reason") or "") == "broker_position_missing:stop_loss"
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
        assert str(payload.get("reason") or "").startswith("broker_position_missing:timeout:")
    finally:
        worker.broker.close()
        worker.store.close()


def test_trailing_moves_stop_to_breakeven_at_halfway_to_tp(tmp_path):
    worker = _make_worker(tmp_path, trailing_activation_ratio=0.5, trailing_distance_pips=10.0)
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
    worker = _make_worker(tmp_path, trailing_activation_ratio=0.0, trailing_distance_pips=10.0)
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


def test_trailing_respects_buy_breakeven_offset(tmp_path):
    worker = _make_worker(
        tmp_path,
        trailing_activation_ratio=0.0,
        trailing_distance_pips=10.0,
        trailing_breakeven_offset_pips=2.0,
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
    worker = _make_worker(tmp_path, trailing_activation_ratio=0.5, trailing_distance_pips=10.0)
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


def test_trailing_uses_asset_specific_breakeven_offsets(tmp_path):
    worker = _make_worker(
        tmp_path,
        trailing_activation_ratio=0.0,
        trailing_distance_pips=10.0,
        trailing_breakeven_offset_pips=0.0,
        trailing_breakeven_offset_pips_fx=2.0,
        trailing_breakeven_offset_pips_index=3.0,
        trailing_breakeven_offset_pips_commodity=6.0,
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
            assert worker._connectivity_allows_open() is False
        assert worker._allowance_backoff_until_ts == pytest.approx(1030.0)
        assert worker._next_connectivity_probe_ts == pytest.approx(1030.0)

        with patch("xtb_bot.worker.time.time", return_value=1010.0):
            assert worker._connectivity_allows_open() is False
        assert calls["count"] == 1

        allowance_events = [
            event
            for event in worker.store.load_events(limit=50)
            if event.get("message") == "Broker allowance backoff active"
        ]
        assert allowance_events
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
