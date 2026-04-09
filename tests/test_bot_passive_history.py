from __future__ import annotations

from collections import deque
import threading
import time

import pytest

from xtb_bot.bot import TradingBot
from xtb_bot.config import BotConfig
from xtb_bot.models import AccountSnapshot, AccountType, Position, PriceTick, RunMode, Side, SymbolSpec
from xtb_bot.risk_manager import RiskConfig


def _make_config(tmp_path) -> BotConfig:
    return BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        broker="ig",
        api_key="ig-key",
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot-passive.db",
        risk=RiskConfig(),
    )


def test_db_first_prime_disabled_by_default(tmp_path, monkeypatch):
    monkeypatch.delenv("XTB_DB_FIRST_PRIME_ENABLED", raising=False)
    cfg = _make_config(tmp_path)
    cfg.symbols = ["EURUSD", "GBPUSD"]
    bot = TradingBot(cfg)

    class _FakeBroker:
        def __init__(self) -> None:
            self.spec_calls = 0
            self.tick_calls = 0
            self.account_calls = 0

        def get_symbol_spec(self, symbol: str) -> SymbolSpec:
            _ = symbol
            self.spec_calls += 1
            raise AssertionError("db-first prime should be disabled by default")

        def get_price(self, symbol: str) -> PriceTick:
            _ = symbol
            self.tick_calls += 1
            raise AssertionError("db-first prime should be disabled by default")

        def get_account_snapshot(self) -> AccountSnapshot:
            self.account_calls += 1
            raise AssertionError("db-first prime should be disabled by default")

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    try:
        bot._prime_db_first_caches()
        assert fake.spec_calls == 0
        assert fake.tick_calls == 0
        assert fake.account_calls == 0
    finally:
        bot.stop()


def test_db_first_prime_can_be_limited_by_symbols(tmp_path, monkeypatch):
    monkeypatch.setenv("XTB_DB_FIRST_PRIME_ENABLED", "true")
    monkeypatch.setenv("XTB_DB_FIRST_PRIME_MAX_SYMBOLS", "1")
    monkeypatch.setenv("XTB_DB_FIRST_PRIME_ACCOUNT_ENABLED", "false")
    cfg = _make_config(tmp_path)
    cfg.symbols = ["EURUSD", "GBPUSD"]
    bot = TradingBot(cfg)

    class _FakeBroker:
        def __init__(self) -> None:
            self.spec_calls = 0
            self.tick_calls = 0
            self.account_calls = 0

        def get_symbol_spec(self, symbol: str) -> SymbolSpec:
            self.spec_calls += 1
            return SymbolSpec(
                symbol=symbol,
                tick_size=0.0001,
                tick_value=10.0,
                contract_size=100000.0,
                lot_min=0.01,
                lot_max=100.0,
                lot_step=0.01,
            )

        def get_price(self, symbol: str) -> PriceTick:
            self.tick_calls += 1
            return PriceTick(symbol=symbol, bid=1.1000, ask=1.1002, timestamp=1_700_000_000.0, volume=1.0)

        def get_account_snapshot(self) -> AccountSnapshot:
            self.account_calls += 1
            return AccountSnapshot(balance=10000.0, equity=10000.0, margin_free=10000.0, timestamp=1_700_000_000.0)

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    try:
        bot._prime_db_first_caches()
        assert fake.spec_calls == 1
        assert fake.tick_calls == 1
        assert fake.account_calls == 0
    finally:
        bot.stop()


def test_normalize_timestamp_seconds_handles_2038_seconds_millis_and_micros(monkeypatch):
    monkeypatch.setattr("xtb_bot.time_utils.time.time", lambda: 1_700_000_000.0)

    assert TradingBot._normalize_timestamp_seconds(2_147_483_647.0) == pytest.approx(2_147_483_647.0)
    assert TradingBot._normalize_timestamp_seconds(2_147_483_647_000.0) == pytest.approx(2_147_483_647.0)
    assert TradingBot._normalize_timestamp_seconds(2_147_483_647_000_000.0) == pytest.approx(2_147_483_647.0)
    assert TradingBot._normalize_timestamp_seconds(2_147_483_647_000_000_000.0) == pytest.approx(1_700_000_000.0)


def test_startup_symbol_spec_preload_fetches_all_symbols(tmp_path, monkeypatch):
    cfg = _make_config(tmp_path)
    cfg.symbols = ["EURUSD", "GBPUSD", "US100"]
    bot = TradingBot(cfg)

    class _FakeBroker:
        def __init__(self) -> None:
            self.spec_symbols: list[str] = []

        def get_symbol_spec(self, symbol: str) -> SymbolSpec:
            self.spec_symbols.append(symbol)
            return SymbolSpec(
                symbol=symbol,
                tick_size=0.0001 if symbol.endswith("USD") and len(symbol) == 6 else 1.0,
                tick_value=10.0,
                contract_size=100000.0 if symbol.endswith("USD") and len(symbol) == 6 else 1.0,
                lot_min=0.01,
                lot_max=100.0,
                lot_step=0.01,
                metadata={"epic": f"TEST.{symbol}.EPIC"},
            )

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    monkeypatch.setattr(bot, "_reserve_ig_non_trading_budget", lambda **kwargs: True)

    try:
        bot._preload_symbol_specs_on_startup()
        assert fake.spec_symbols == ["EURUSD", "GBPUSD", "US100"]
        for symbol in cfg.symbols:
            assert bot.store.load_broker_symbol_spec(symbol, max_age_sec=0.0) is not None
        events = bot.store.load_events(limit=20)
        preload_events = [event for event in events if event.get("message") == "Startup symbol specification preload complete"]
        assert preload_events
        payload = preload_events[0].get("payload") or {}
        assert int(payload.get("symbols_total") or 0) == 3
        assert int(payload.get("loaded_count") or 0) == 3
        assert int(payload.get("failed_count") or 0) == 0
    finally:
        bot.stop()


def test_startup_symbol_spec_preload_records_changed_spec_fields(tmp_path, monkeypatch):
    cfg = _make_config(tmp_path)
    cfg.symbols = ["GOLD"]
    bot = TradingBot(cfg)

    previous = SymbolSpec(
        symbol="GOLD",
        tick_size=0.1,
        tick_value=100.0,
        contract_size=100.0,
        lot_min=0.1,
        lot_max=100.0,
        lot_step=0.1,
        metadata={"epic": "CS.D.GOLD.CFD.IP"},
    )
    bot.store.upsert_broker_symbol_spec(
        symbol="GOLD",
        spec=previous,
        ts=1_700_000_000.0,
        source="seed_test",
    )

    class _FakeBroker:
        def get_symbol_spec(self, symbol: str) -> SymbolSpec:
            _ = symbol
            return SymbolSpec(
                symbol="GOLD",
                tick_size=0.1,
                tick_value=10.0,
                contract_size=100.0,
                lot_min=0.1,
                lot_max=100.0,
                lot_step=0.1,
                metadata={"epic": "CS.D.CFDGOLD.CFDGC.IP"},
            )

        def close(self) -> None:
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]
    monkeypatch.setattr(bot, "_reserve_ig_non_trading_budget", lambda **kwargs: True)

    try:
        bot._preload_symbol_specs_on_startup()
        events = bot.store.load_events(limit=30)
        changed_events = [event for event in events if event.get("message") == "Startup symbol specification changed"]
        assert changed_events
        payload = changed_events[0].get("payload") or {}
        changes = payload.get("changes") or {}
        assert isinstance(changes, dict)
        assert "tick_value" in changes
        assert "epic" in changes
    finally:
        bot.stop()


def test_startup_symbol_spec_preload_raises_when_any_symbol_fetch_fails(tmp_path, monkeypatch):
    cfg = _make_config(tmp_path)
    cfg.symbols = ["EURUSD", "GBPUSD"]
    bot = TradingBot(cfg)

    class _FakeBroker:
        def get_symbol_spec(self, symbol: str) -> SymbolSpec:
            if symbol == "GBPUSD":
                raise RuntimeError("symbol fetch failed")
            return SymbolSpec(
                symbol=symbol,
                tick_size=0.0001,
                tick_value=10.0,
                contract_size=100000.0,
                lot_min=0.01,
                lot_max=100.0,
                lot_step=0.01,
            )

        def close(self) -> None:
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]
    monkeypatch.setattr(bot, "_reserve_ig_non_trading_budget", lambda **kwargs: True)

    try:
        with pytest.raises(RuntimeError, match="GBPUSD"):
            bot._preload_symbol_specs_on_startup()
        events = bot.store.load_events(limit=20)
        preload_events = [event for event in events if event.get("message") == "Startup symbol specification preload complete"]
        assert preload_events
        payload = preload_events[0].get("payload") or {}
        assert int(payload.get("failed_count") or 0) == 1
    finally:
        bot.stop()


def test_startup_symbol_spec_preload_uses_fresh_cache_without_live_calls(tmp_path, monkeypatch):
    cfg = _make_config(tmp_path)
    cfg.symbols = ["EURUSD", "GBPUSD", "US100"]
    bot = TradingBot(cfg)

    now_ts = time.time()
    for symbol in cfg.symbols:
        bot.store.upsert_broker_symbol_spec(
            symbol=symbol,
            spec=SymbolSpec(
                symbol=symbol,
                tick_size=0.0001 if symbol.endswith("USD") and len(symbol) == 6 else 1.0,
                tick_value=10.0,
                contract_size=100000.0 if symbol.endswith("USD") and len(symbol) == 6 else 1.0,
                lot_min=0.01,
                lot_max=100.0,
                lot_step=0.01,
            ),
            ts=now_ts,
            source="seed_fresh_cache",
        )

    class _FakeBroker:
        def __init__(self) -> None:
            self.spec_calls = 0

        def get_symbol_spec(self, symbol: str) -> SymbolSpec:
            _ = symbol
            self.spec_calls += 1
            raise AssertionError("startup preload should use fresh cache without live calls")

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    monkeypatch.setattr(bot, "_reserve_ig_non_trading_budget", lambda **kwargs: True)

    try:
        bot._preload_symbol_specs_on_startup()
        assert fake.spec_calls == 0
        events = bot.store.load_events(limit=20)
        preload_events = [event for event in events if event.get("message") == "Startup symbol specification preload complete"]
        assert preload_events
        payload = preload_events[0].get("payload") or {}
        assert int(payload.get("loaded_count") or 0) == 3
        assert int(payload.get("loaded_live_count") or 0) == 0
        assert int(payload.get("loaded_cached_count") or 0) == 3
        assert int(payload.get("failed_count") or 0) == 0
    finally:
        bot.stop()


def test_startup_symbol_spec_preload_short_circuits_after_allowance_and_uses_cache(tmp_path, monkeypatch):
    cfg = _make_config(tmp_path)
    cfg.symbols = ["EURUSD", "GBPUSD", "US100"]
    bot = TradingBot(cfg)

    stale_ts = 1_700_000_000.0
    for symbol in cfg.symbols:
        bot.store.upsert_broker_symbol_spec(
            symbol=symbol,
            spec=SymbolSpec(
                symbol=symbol,
                tick_size=0.0001 if symbol.endswith("USD") and len(symbol) == 6 else 1.0,
                tick_value=10.0,
                contract_size=100000.0 if symbol.endswith("USD") and len(symbol) == 6 else 1.0,
                lot_min=0.01,
                lot_max=100.0,
                lot_step=0.01,
            ),
            ts=stale_ts,
            source="seed_stale_cache",
        )

    class _FakeBroker:
        def __init__(self) -> None:
            self.spec_symbols: list[str] = []

        def get_symbol_spec(self, symbol: str) -> SymbolSpec:
            self.spec_symbols.append(symbol)
            raise RuntimeError(
                'IG API GET /markets/CS.D.EURUSD.CFD.IP failed: 403 Forbidden {"errorCode":"error.public-api.exceeded-api-key-allowance"}'
            )

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    monkeypatch.setattr(bot, "_reserve_ig_non_trading_budget", lambda **kwargs: True)

    try:
        bot._preload_symbol_specs_on_startup()
        # First symbol probes live and trips allowance; remaining symbols should use cache fallback.
        assert fake.spec_symbols == ["EURUSD"]
        events = bot.store.load_events(limit=20)
        preload_events = [event for event in events if event.get("message") == "Startup symbol specification preload complete"]
        assert preload_events
        payload = preload_events[0].get("payload") or {}
        assert bool(payload.get("allowance_short_circuit")) is True
        assert int(payload.get("loaded_count") or 0) == 3
        assert int(payload.get("loaded_live_count") or 0) == 0
        assert int(payload.get("loaded_cached_count") or 0) == 3
        assert int(payload.get("failed_count") or 0) == 0
    finally:
        bot.stop()


def test_startup_symbol_spec_preload_short_circuits_after_allowance_and_uses_fallback_without_cache(
    tmp_path,
    monkeypatch,
):
    cfg = _make_config(tmp_path)
    cfg.symbols = ["EURUSD", "GBPUSD", "US100"]
    bot = TradingBot(cfg)

    class _FakeBroker:
        def __init__(self) -> None:
            self.spec_symbols: list[str] = []

        def get_symbol_spec(self, symbol: str) -> SymbolSpec:
            self.spec_symbols.append(symbol)
            raise RuntimeError(
                'IG API GET /markets/CS.D.EURUSD.CFD.IP failed: 403 Forbidden {"errorCode":"error.public-api.exceeded-api-key-allowance"}'
            )

        def _epic_attempt_order(self, symbol: str) -> list[str]:
            return [f"TEST.{symbol}.EPIC"]

        def _build_fallback_symbol_spec_for_epic(self, symbol: str, epic: str) -> SymbolSpec:
            return SymbolSpec(
                symbol=symbol,
                tick_size=0.0001 if symbol.endswith("USD") and len(symbol) == 6 else 1.0,
                tick_value=10.0,
                contract_size=100000.0 if symbol.endswith("USD") and len(symbol) == 6 else 1.0,
                lot_min=0.01,
                lot_max=100.0,
                lot_step=0.01,
                metadata={"epic": epic, "spec_origin": "critical_fallback"},
            )

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    monkeypatch.setattr(bot, "_reserve_ig_non_trading_budget", lambda **kwargs: True)
    monkeypatch.setattr(bot, "_broker_public_api_backoff_remaining_sec", lambda: 2.0 if fake.spec_symbols else 0.0)

    try:
        bot._preload_symbol_specs_on_startup()
        assert fake.spec_symbols == ["EURUSD"]
        for symbol in cfg.symbols:
            spec = bot.store.load_broker_symbol_spec(symbol, max_age_sec=0.0)
            assert spec is not None
            metadata = spec.metadata if isinstance(spec.metadata, dict) else {}
            assert metadata.get("spec_origin") == "critical_fallback_allowance"
        events = bot.store.load_events(limit=20)
        preload_events = [event for event in events if event.get("message") == "Startup symbol specification preload complete"]
        assert preload_events
        payload = preload_events[0].get("payload") or {}
        assert bool(payload.get("allowance_short_circuit")) is True
        assert int(payload.get("loaded_count") or 0) == 3
        assert int(payload.get("loaded_live_count") or 0) == 0
        assert int(payload.get("loaded_cached_count") or 0) == 0
        assert int(payload.get("loaded_fallback_count") or 0) == 3
        assert int(payload.get("failed_count") or 0) == 0
    finally:
        bot.stop()


def test_startup_symbol_spec_preload_uses_symbol_fallback_for_single_epic_unavailable_failure(
    tmp_path,
    monkeypatch,
):
    cfg = _make_config(tmp_path)
    cfg.symbols = ["EURUSD", "UK100"]
    bot = TradingBot(cfg)

    class _FakeBroker:
        def __init__(self) -> None:
            self.spec_symbols: list[str] = []

        def get_symbol_spec(self, symbol: str) -> SymbolSpec:
            self.spec_symbols.append(symbol)
            if symbol == "UK100":
                raise RuntimeError(
                    'IG API GET /markets/IX.D.FTSE.CASH.IP failed: 404 Not Found {"errorCode":"error.service.marketdata.instrument.epic.unavailable"}'
                )
            return SymbolSpec(
                symbol=symbol,
                tick_size=0.0001,
                tick_value=10.0,
                contract_size=100000.0,
                lot_min=0.01,
                lot_max=100.0,
                lot_step=0.01,
                metadata={"epic": f"TEST.{symbol}.EPIC"},
            )

        def _epic_attempt_order(self, symbol: str) -> list[str]:
            if symbol == "UK100":
                return ["IX.D.FTSE.DAILY.IP"]
            return [f"TEST.{symbol}.EPIC"]

        def _build_fallback_symbol_spec_for_epic(self, symbol: str, epic: str) -> SymbolSpec:
            return SymbolSpec(
                symbol=symbol,
                tick_size=1.0 if symbol == "UK100" else 0.0001,
                tick_value=10.0,
                contract_size=1.0 if symbol == "UK100" else 100000.0,
                lot_min=0.1 if symbol == "UK100" else 0.01,
                lot_max=100.0,
                lot_step=0.1 if symbol == "UK100" else 0.01,
                metadata={"epic": epic, "spec_origin": "critical_fallback"},
            )

        def close(self) -> None:
            return None

    class _FakeProxy:
        def __init__(self, broker: _FakeBroker) -> None:
            self._broker = broker

        @property
        def underlying(self) -> _FakeBroker:
            return self._broker

        def get_symbol_spec(self, symbol: str) -> SymbolSpec:
            return self._broker.get_symbol_spec(symbol)

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = _FakeProxy(fake)  # type: ignore[assignment]
    monkeypatch.setattr(bot, "_reserve_ig_non_trading_budget", lambda **kwargs: True)

    try:
        bot._preload_symbol_specs_on_startup()
        assert fake.spec_symbols == ["EURUSD", "UK100"]
        uk100_spec = bot.store.load_broker_symbol_spec("UK100", max_age_sec=0.0)
        assert uk100_spec is not None
        metadata = uk100_spec.metadata if isinstance(uk100_spec.metadata, dict) else {}
        assert metadata.get("spec_origin") == "critical_fallback_startup"
        assert metadata.get("epic") == "IX.D.FTSE.DAILY.IP"
        assert str(metadata.get("startup_fallback_reason") or "").startswith("ig_api_get_markets_ix_d_ftse_cash_ip_failed")
        events = bot.store.load_events(limit=20)
        preload_events = [event for event in events if event.get("message") == "Startup symbol specification preload complete"]
        assert preload_events
        payload = preload_events[0].get("payload") or {}
        assert int(payload.get("loaded_count") or 0) == 2
        assert int(payload.get("loaded_live_count") or 0) == 1
        assert int(payload.get("loaded_fallback_count") or 0) == 1
        assert int(payload.get("failed_count") or 0) == 0
    finally:
        bot.stop()


def test_db_first_tick_request_interval_respects_target_rpm(monkeypatch, tmp_path):
    monkeypatch.setenv("XTB_IG_NON_TRADING_BUDGET_ENABLED", "false")
    monkeypatch.setenv("XTB_DB_FIRST_TICK_POLL_INTERVAL_SEC", "0.5")
    monkeypatch.setenv("XTB_DB_FIRST_TICK_TARGET_RPM", "20")
    cfg = _make_config(tmp_path)
    cfg.ig_rest_market_min_interval_sec = 0.5
    bot = TradingBot(cfg)
    try:
        assert bot._db_first_tick_request_interval_sec() == pytest.approx(3.0)
    finally:
        bot.stop()


def test_db_first_tick_target_rpm_capped_by_local_budget_reserve(monkeypatch, tmp_path):
    monkeypatch.setenv("XTB_IG_NON_TRADING_BUDGET_ENABLED", "true")
    monkeypatch.setenv("XTB_IG_NON_TRADING_BUDGET_RPM", "24")
    monkeypatch.setenv("XTB_IG_NON_TRADING_BUDGET_RESERVE_RPM", "4")
    monkeypatch.setenv("XTB_DB_FIRST_TICK_POLL_INTERVAL_SEC", "0.5")
    monkeypatch.setenv("XTB_DB_FIRST_TICK_TARGET_RPM", "24")
    cfg = _make_config(tmp_path)
    cfg.ig_rest_market_min_interval_sec = 0.5
    bot = TradingBot(cfg)
    try:
        assert bot._db_first_tick_target_rpm == pytest.approx(20.0)
        assert bot._db_first_tick_request_interval_sec() == pytest.approx(3.0)
    finally:
        bot.stop()


def test_db_first_tick_target_rpm_invalid_env_falls_back_to_default(monkeypatch, tmp_path):
    monkeypatch.setenv("XTB_IG_NON_TRADING_BUDGET_ENABLED", "false")
    monkeypatch.setenv("XTB_DB_FIRST_TICK_POLL_INTERVAL_SEC", "0.5")
    monkeypatch.setenv("XTB_DB_FIRST_TICK_TARGET_RPM", "broken")
    cfg = _make_config(tmp_path)
    cfg.ig_rest_market_min_interval_sec = 0.5
    bot = TradingBot(cfg)
    try:
        assert bot._db_first_tick_target_rpm == pytest.approx(24.0)
        assert bot._db_first_tick_request_interval_sec() == pytest.approx(2.5)
    finally:
        bot.stop()


def test_ig_non_trading_budget_reserve_defaults_to_twelve_for_ig(monkeypatch, tmp_path):
    monkeypatch.setenv("XTB_IG_NON_TRADING_BUDGET_ENABLED", "true")
    monkeypatch.setenv("XTB_IG_NON_TRADING_BUDGET_RPM", "24")
    monkeypatch.delenv("XTB_IG_NON_TRADING_BUDGET_RESERVE_RPM", raising=False)
    monkeypatch.setenv("XTB_DB_FIRST_TICK_POLL_INTERVAL_SEC", "0.5")
    monkeypatch.setenv("XTB_DB_FIRST_TICK_TARGET_RPM", "24")
    cfg = _make_config(tmp_path)
    cfg.ig_rest_market_min_interval_sec = 0.5
    bot = TradingBot(cfg)
    try:
        assert bot._ig_non_trading_budget_reserve_rpm == pytest.approx(12.0)
        assert bot._db_first_tick_target_rpm == pytest.approx(12.0)
    finally:
        bot.stop()


def test_db_first_tick_effective_hard_max_age_covers_active_cycle(monkeypatch, tmp_path):
    monkeypatch.setenv("XTB_IG_NON_TRADING_BUDGET_ENABLED", "false")
    monkeypatch.setenv("XTB_DB_FIRST_TICK_POLL_INTERVAL_SEC", "0.5")
    monkeypatch.setenv("XTB_DB_FIRST_TICK_TARGET_RPM", "24")
    monkeypatch.setenv("XTB_DB_FIRST_TICK_HARD_MAX_AGE_SEC", "10")
    cfg = _make_config(tmp_path)
    cfg.ig_rest_market_min_interval_sec = 0.5
    bot = TradingBot(cfg)
    try:
        # 60 / 24 = 2.5s request interval; 5 active symbols => 12.5s cycle.
        assert bot._db_first_tick_effective_hard_max_age_sec(5) == pytest.approx(15.0)
    finally:
        bot.stop()


def test_cached_passive_history_refresh_skips_when_another_refresh_holds_lock(tmp_path):
    bot = TradingBot(_make_config(tmp_path))
    try:
        bot._passive_history_cursor = 3
        assert bot._passive_history_refresh_lock.acquire(timeout=0.1) is True
        try:
            bot.store.load_latest_broker_tick = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not read cached tick"))  # type: ignore[assignment]
            bot._refresh_passive_price_history_from_cached_ticks()
        finally:
            bot._passive_history_refresh_lock.release()
        assert bot._passive_history_cursor == 3
    finally:
        bot.stop()


def test_passive_history_refresh_skips_when_another_refresh_holds_lock(tmp_path):
    bot = TradingBot(_make_config(tmp_path))
    try:
        bot._passive_history_cursor = 2
        assert bot._passive_history_refresh_lock.acquire(timeout=0.1) is True
        try:
            bot.broker.get_price = lambda symbol: (_ for _ in ()).throw(AssertionError(f"should not fetch {symbol}"))  # type: ignore[assignment]
            bot._refresh_passive_price_history(force=True)
        finally:
            bot._passive_history_refresh_lock.release()
        assert bot._passive_history_cursor == 2
    finally:
        bot.stop()


def test_ig_non_trading_budget_defers_request_when_bucket_empty(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))
    monkeypatch.setattr(bot._ig_non_trading_budget, "try_consume", lambda tokens=1.0: False)
    monkeypatch.setattr(bot._ig_non_trading_budget, "available_tokens", lambda: 0.0)
    bot._ig_non_trading_budget_warn_interval_sec = 0.0
    try:
        allowed = bot._reserve_ig_non_trading_budget(
            scope="unit_test_budget",
            wait_timeout_sec=0.0,
        )
        assert allowed is False
        assert bot._ig_non_trading_budget_blocked_total >= 1
        events = bot.store.load_events(limit=5)
        assert any(str(event.get("message")) == "IG non-trading request deferred by local budget" for event in events)
    finally:
        bot.stop()


def test_db_first_symbol_spec_refresh_candidates_respect_retry_backoff(tmp_path):
    bot = TradingBot(_make_config(tmp_path))
    try:
        now_ts = time.time()
        bot._db_first_symbol_spec_retry_after_ts_by_symbol["EURUSD"] = now_ts + 45.0
        eligible, next_retry_remaining = bot._filter_db_first_symbol_spec_refresh_candidates(
            ["EURUSD", "GBPUSD"],
            now_ts=now_ts,
        )
        assert eligible == ["GBPUSD"]
        assert next_retry_remaining == pytest.approx(45.0)
    finally:
        bot.stop()


def test_db_first_symbol_spec_refresh_failure_backoff_grows_per_symbol(tmp_path):
    bot = TradingBot(_make_config(tmp_path))
    try:
        first = bot._record_db_first_symbol_spec_refresh_failure(
            "EURUSD",
            "error.public-api.exceeded-api-key-allowance",
            request_interval_sec=6.0,
            now_ts=1_000.0,
        )
        second = bot._record_db_first_symbol_spec_refresh_failure(
            "EURUSD",
            "error.public-api.exceeded-api-key-allowance",
            request_interval_sec=6.0,
            now_ts=1_040.0,
        )
        assert first == pytest.approx(30.0)
        assert second == pytest.approx(60.0)
        assert bot._db_first_symbol_spec_retry_after_ts_by_symbol["EURUSD"] == pytest.approx(1_100.0)
    finally:
        bot.stop()


def test_db_first_tick_symbol_buckets_prioritize_active_workers(tmp_path):
    cfg = _make_config(tmp_path)
    cfg.symbols = ["EURUSD", "GBPUSD", "US100", "WTI"]
    bot = TradingBot(cfg)

    class _AliveWorker:
        def is_alive(self) -> bool:
            return True

        def join(self, timeout: float | None = None) -> None:
            _ = timeout
            return None

    class _DeadWorker:
        def is_alive(self) -> bool:
            return False

        def join(self, timeout: float | None = None) -> None:
            _ = timeout
            return None

    with bot._workers_lock:
        bot.workers["US100"] = _AliveWorker()  # type: ignore[assignment]
        bot.workers["GBPUSD"] = _DeadWorker()  # type: ignore[assignment]
    bot.position_book.upsert(
        Position(
            position_id="pos-open-1",
            symbol="WTI",
            side=Side.BUY,
            volume=0.1,
            open_price=70.0,
            stop_loss=69.0,
            take_profit=72.0,
            opened_at=1.0,
            status="open",
        )
    )
    try:
        active, passive = bot._db_first_tick_symbol_buckets()
        assert active == ["US100", "WTI"]
        assert passive == ["EURUSD", "GBPUSD"]
        assert bot._db_first_tick_priority_symbols == ["WTI"]
    finally:
        bot.stop()


def test_db_first_tick_symbol_buckets_detects_near_breakout_priority(tmp_path):
    cfg = _make_config(tmp_path)
    cfg.symbols = ["US100", "EURUSD"]
    bot = TradingBot(cfg)

    class _AliveWorker:
        def __init__(self, prices: list[float]) -> None:
            self.prices = deque(prices, maxlen=128)

        def is_alive(self) -> bool:
            return True

        def join(self, timeout: float | None = None) -> None:
            _ = timeout
            return None

    with bot._workers_lock:
        bot.workers["US100"] = _AliveWorker([100.0, 101.0, 102.5, 104.0, 105.0, 106.5, 108.0, 109.8])  # type: ignore[assignment]
        bot.workers["EURUSD"] = _AliveWorker([1.1000, 1.1010, 1.1020, 1.1012, 1.1008, 1.1011, 1.1010, 1.1012])  # type: ignore[assignment]
    try:
        active, passive = bot._db_first_tick_symbol_buckets()
        assert active == ["US100", "EURUSD"]
        assert passive == []
        assert bot._db_first_tick_priority_symbols == ["US100"]
    finally:
        bot.stop()


def test_fetch_db_first_tick_uses_stream_only_without_budget(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))

    class _FakeBroker:
        def __init__(self) -> None:
            self.stream_calls = 0
            self.rest_calls = 0

        def get_price_stream_only(self, symbol: str, wait_timeout_sec: float = 0.0) -> PriceTick | None:
            _ = wait_timeout_sec
            self.stream_calls += 1
            return PriceTick(symbol=symbol, bid=1.1000, ask=1.1002, timestamp=1_700_000_000.0, volume=1.0)

        def get_price(self, symbol: str) -> PriceTick:
            _ = symbol
            self.rest_calls += 1
            raise AssertionError("REST get_price should not be used when stream-only tick is available")

        def close(self) -> None:
            return None

    def _fail_budget(**kwargs: object) -> bool:
        _ = kwargs
        raise AssertionError("Local non-trading budget must not be consumed for stream-only tick")

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    monkeypatch.setattr(bot, "_reserve_ig_non_trading_budget", _fail_budget)
    try:
        tick = bot._fetch_db_first_tick("EURUSD", request_interval_sec=3.0)
        assert tick is not None
        assert fake.stream_calls == 1
        assert fake.rest_calls == 0
    finally:
        bot.stop()


def test_fetch_db_first_tick_falls_back_to_budgeted_rest_when_stream_only_misses(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))

    class _FakeBroker:
        def __init__(self) -> None:
            self.stream_calls = 0
            self.rest_calls = 0

        def get_price_stream_only(self, symbol: str, wait_timeout_sec: float = 0.0) -> PriceTick | None:
            _ = (symbol, wait_timeout_sec)
            self.stream_calls += 1
            return None

        def get_price(self, symbol: str) -> PriceTick:
            self.rest_calls += 1
            return PriceTick(symbol=symbol, bid=1.1000, ask=1.1002, timestamp=1_700_000_001.0, volume=1.0)

        def close(self) -> None:
            return None

    budget_calls: list[dict[str, object]] = []

    def _allow_budget(**kwargs: object) -> bool:
        budget_calls.append(dict(kwargs))
        return True

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    monkeypatch.setattr(bot, "_reserve_ig_non_trading_budget", _allow_budget)
    try:
        tick = bot._fetch_db_first_tick("EURUSD", request_interval_sec=3.0)
        assert tick is not None
        assert fake.stream_calls == 1
        assert fake.rest_calls == 1
        assert budget_calls and budget_calls[0].get("scope") == "db_first_tick"
    finally:
        bot.stop()


def test_fetch_db_first_tick_returns_none_when_budget_denied(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))

    class _FakeBroker:
        def __init__(self) -> None:
            self.stream_calls = 0
            self.rest_calls = 0

        def get_price_stream_only(self, symbol: str, wait_timeout_sec: float = 0.0) -> PriceTick | None:
            _ = (symbol, wait_timeout_sec)
            self.stream_calls += 1
            return None

        def get_price(self, symbol: str) -> PriceTick:
            _ = symbol
            self.rest_calls += 1
            raise AssertionError("REST get_price must not be called when budget denies request")

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    monkeypatch.setattr(bot, "_reserve_ig_non_trading_budget", lambda **kwargs: False)
    try:
        tick = bot._fetch_db_first_tick("EURUSD", request_interval_sec=3.0)
        assert tick is None
        assert fake.stream_calls == 1
        assert fake.rest_calls == 0
    finally:
        bot.stop()


def test_fetch_db_first_tick_returns_none_when_stream_rest_fallback_block_active(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))

    class _FakeBroker:
        def __init__(self) -> None:
            self.stream_calls = 0
            self.block_calls = 0
            self.rest_calls = 0

        def get_price_stream_only(self, symbol: str, wait_timeout_sec: float = 0.0) -> PriceTick | None:
            _ = (symbol, wait_timeout_sec)
            self.stream_calls += 1
            return None

        def get_stream_rest_fallback_block_remaining_sec(self, symbol: str) -> float:
            _ = symbol
            self.block_calls += 1
            return 30.0

        def get_price(self, symbol: str) -> PriceTick:
            _ = symbol
            self.rest_calls += 1
            raise AssertionError("REST get_price must not be called while stream REST fallback is blocked")

        def close(self) -> None:
            return None

    def _fail_budget(**kwargs: object) -> bool:
        _ = kwargs
        raise AssertionError("Local non-trading budget must not be consumed while stream REST fallback is blocked")

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    monkeypatch.setattr(bot, "_reserve_ig_non_trading_budget", _fail_budget)
    try:
        tick = bot._fetch_db_first_tick("GOLD", request_interval_sec=3.0)
        assert tick is None
        assert fake.stream_calls == 1
        assert fake.block_calls == 1
        assert fake.rest_calls == 0
    finally:
        bot.stop()


def test_register_stream_tick_hook_persists_tick_into_broker_cache(tmp_path):
    bot = TradingBot(_make_config(tmp_path))

    class _FakeBroker:
        def __init__(self) -> None:
            self.handler = None

        def set_stream_tick_handler(self, handler) -> None:
            self.handler = handler

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    try:
        bot._register_stream_tick_persistence_hook()
        assert callable(fake.handler)
        fake.handler(
            PriceTick(
                symbol="ZZTEST",
                bid=1.2345,
                ask=1.2349,
                timestamp=1_700_000_000.0,
                volume=12.0,
            )
        )
        cached = bot.store.load_latest_broker_tick("ZZTEST", max_age_sec=0.0)
        assert cached is not None
        assert cached.bid == pytest.approx(1.2345)
        assert cached.ask == pytest.approx(1.2349)
        assert cached.timestamp == pytest.approx(1_700_000_000.0)
        assert cached.volume == pytest.approx(12.0)
        memory_cached = bot._load_latest_tick_from_memory_cache("ZZTEST", max_age_sec=0.0)
        assert memory_cached is not None
        assert memory_cached.bid == pytest.approx(1.2345)
        assert memory_cached.ask == pytest.approx(1.2349)
        assert memory_cached.timestamp == pytest.approx(1_700_000_000.0)
        assert memory_cached.volume == pytest.approx(12.0)
    finally:
        bot.stop()


def test_pre_warm_history_fetches_all_symbols_even_when_local_history_exists(tmp_path, monkeypatch):
    cfg = _make_config(tmp_path)
    cfg.symbols = ["EURUSD", "GBPUSD"]
    bot = TradingBot(cfg)

    keep_rows = bot._price_history_keep_rows_for_symbol("EURUSD")
    target_points = max(2, min(int(bot._history_prefetch_points), int(keep_rows)))
    for index in range(target_points):
        bot.store.append_price_sample(
            symbol="EURUSD",
            ts=1_700_000_000.0 + float(index),
            close=1.1000 + float(index) * 0.00001,
            volume=1.0,
            max_rows_per_symbol=keep_rows,
        )

    class _FakeBroker:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def fetch_recent_price_history(self, symbol: str, *, resolution: str, points: int):
            _ = resolution
            self.calls.append(symbol)
            base = 1.2000 if symbol == "EURUSD" else 1.3000
            return [
                (1_710_000_000.0 + idx, base + idx * 0.0001, 5.0 + idx)
                for idx in range(points)
            ]

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    monkeypatch.setattr(bot, "_reserve_ig_non_trading_budget", lambda **kwargs: True)
    try:
        bot._pre_warm_history()
        assert fake.calls == ["EURUSD", "GBPUSD"]
        events = bot.store.load_events(limit=20)
        warmup_events = [event for event in events if event.get("message") == "Startup history pre-warm complete"]
        assert warmup_events
        payload = warmup_events[0].get("payload") or {}
        assert int(payload.get("symbols_total") or 0) == 2
        assert int(payload.get("symbols_fetched") or 0) == 2
    finally:
        bot.stop()


def test_multi_strategy_history_prefetch_includes_component_requirements(tmp_path):
    cfg = _make_config(tmp_path)
    cfg.symbols = ["TOPIX"]
    cfg.strategy = "momentum"
    cfg.strategy_params = {
        "fast_window": 3,
        "slow_window": 5,
        "multi_strategy_enabled": True,
        "multi_strategy_names": "momentum,g2,index_hybrid",
    }
    cfg.strategy_params_map = {
        "momentum": dict(cfg.strategy_params),
        "g2": {
            "g2_candle_timeframe_sec": 900,
            "g2_ema_fast": 3,
            "g2_ema_trend": 5,
            "g2_ema_macro": 200,
            "g2_rsi_window": 5,
            "g2_atr_window": 5,
            "g2_session_filter_enabled": False,
        },
        "index_hybrid": {},
    }
    bot = TradingBot(cfg)
    try:
        keep_rows = bot._price_history_keep_rows_for_symbol("TOPIX")
        plan = dict(bot._history_prefetch_plan_for_symbol("TOPIX"))

        assert keep_rows > bot._history_keep_rows_min
        assert plan["MINUTE_15"] >= 209
    finally:
        bot.stop()


def test_bot_multi_strategy_default_components_include_donchian_and_exclude_oil(tmp_path):
    cfg = _make_config(tmp_path)
    cfg.strategy = "momentum"
    cfg.strategy_params = {
        "fast_window": 3,
        "slow_window": 5,
        "multi_strategy_enabled": True,
    }
    bot = TradingBot(cfg)
    try:
        names = bot._resolve_multi_strategy_component_names("momentum", cfg.strategy_params)

        assert "donchian_breakout" in names
        assert "oil" not in names
        assert names[0] == "momentum"
    finally:
        bot.stop()


def test_multi_strategy_history_prefetch_skips_unsupported_component_symbols(tmp_path):
    cfg = _make_config(tmp_path)
    cfg.symbols = ["EURUSD"]
    cfg.strategy = "momentum"
    cfg.strategy_params = {
        "fast_window": 3,
        "slow_window": 5,
        "multi_strategy_enabled": True,
        "multi_strategy_names": "momentum,g2",
    }
    cfg.strategy_params_map = {
        "momentum": dict(cfg.strategy_params),
        "g2": {
            "g2_candle_timeframe_sec": 900,
            "g2_ema_fast": 3,
            "g2_ema_trend": 5,
            "g2_ema_macro": 200,
            "g2_rsi_window": 5,
            "g2_atr_window": 5,
            "g2_session_filter_enabled": False,
        },
    }
    bot = TradingBot(cfg)
    try:
        keep_rows = bot._price_history_keep_rows_for_symbol("EURUSD")
        plan = bot._history_prefetch_plan_for_symbol("EURUSD")
        base_keep_rows = bot._estimate_history_keep_rows("momentum", cfg.strategy_params)

        assert keep_rows == base_keep_rows
        assert plan == [("MINUTE", min(int(bot._history_prefetch_points), int(keep_rows)))]
    finally:
        bot.stop()


def test_history_prefetch_fetches_coarser_candle_history_when_tick_rows_have_short_coverage(
    tmp_path,
    monkeypatch,
):
    cfg = _make_config(tmp_path)
    cfg.symbols = ["TOPIX"]
    cfg.strategy = "g2"
    cfg.strategy_params = {
        "g2_candle_timeframe_sec": 900,
        "g2_ema_fast": 3,
        "g2_ema_trend": 5,
        "g2_ema_macro": 200,
        "g2_rsi_window": 5,
        "g2_atr_window": 5,
        "g2_session_filter_enabled": False,
    }
    bot = TradingBot(cfg)

    keep_rows = bot._price_history_keep_rows_for_symbol("TOPIX")
    for index in range(400):
        bot.store.append_price_sample(
            symbol="TOPIX",
            ts=1_700_000_000.0 + float(index),
            close=3600.0 + float(index) * 0.1,
            volume=1.0,
            max_rows_per_symbol=keep_rows,
        )

    class _FakeBroker:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, int]] = []

        def fetch_recent_price_history(self, symbol: str, *, resolution: str, points: int):
            self.calls.append((symbol, resolution, points))
            spacing = {
                "MINUTE": 60.0,
                "MINUTE_2": 120.0,
                "MINUTE_5": 300.0,
                "MINUTE_15": 900.0,
            }.get(resolution, 60.0)
            return [
                (1_700_001_000.0 + idx * spacing, 3650.0 + idx * 0.5, 5.0 + idx)
                for idx in range(points)
            ]

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    monkeypatch.setattr(bot, "_reserve_ig_non_trading_budget", lambda **kwargs: True)
    try:
        bot._seed_startup_history_for_fast_start()
        resolutions = [resolution for _, resolution, _ in fake.calls]
        assert "MINUTE" in resolutions
        coarse_resolution = next((resolution for resolution in resolutions if resolution != "MINUTE"), None)
        assert coarse_resolution is not None
        coarse_rows = bot.store.load_recent_candle_history(
            "TOPIX",
            resolution_sec=bot._history_resolution_seconds(coarse_resolution) or 900,
            limit=500,
        )
        assert coarse_rows
    finally:
        bot.stop()


def test_history_prefetch_coverage_uses_candle_history_for_coarse_resolutions(tmp_path):
    cfg = _make_config(tmp_path)
    cfg.symbols = ["TOPIX"]
    bot = TradingBot(cfg)
    try:
        base_ts = 1_700_000_100.0
        for idx in range(210):
            bot.store.append_candle_sample(
                "TOPIX",
                resolution_sec=900,
                ts=base_ts + idx * 900.0,
                close=3600.0 + idx * 0.5,
                volume=10.0 + idx,
                max_rows_per_symbol=500,
            )

        assert bot._history_prefetch_has_time_coverage(
            "TOPIX",
            resolution="MINUTE_15",
            target_points=200,
        )
    finally:
        bot.stop()


def test_history_prefetch_coverage_uses_candle_history_for_minute_resolution(tmp_path):
    cfg = _make_config(tmp_path)
    cfg.symbols = ["US100"]
    bot = TradingBot(cfg)
    try:
        base_ts = 1_700_000_100.0
        for idx in range(300):
            bot.store.append_candle_sample(
                "US100",
                resolution_sec=60,
                ts=base_ts + idx * 60.0,
                close=18000.0 + idx * 0.5,
                volume=10.0 + idx,
                max_rows_per_symbol=600,
            )

        assert bot._history_prefetch_has_time_coverage(
            "US100",
            resolution="MINUTE",
            target_points=300,
        )
    finally:
        bot.stop()


def test_prefill_stores_coarse_history_in_candle_table_without_bloating_raw_ticks(tmp_path, monkeypatch):
    cfg = _make_config(tmp_path)
    cfg.symbols = ["TOPIX"]
    bot = TradingBot(cfg)

    class _FakeBroker:
        def fetch_recent_price_history(self, symbol: str, *, resolution: str, points: int):
            assert symbol == "TOPIX"
            assert resolution == "MINUTE_15"
            return [
                (
                    1_700_100_100.0 + idx * 900.0,
                    3650.0 + idx * 0.5,
                    10.0 + idx,
                    3649.0 + idx * 0.5,
                    3652.0 + idx * 0.5,
                    3647.5 + idx * 0.5,
                )
                for idx in range(points)
            ]

        def close(self) -> None:
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]
    monkeypatch.setattr(bot, "_reserve_ig_non_trading_budget", lambda **kwargs: True)
    monkeypatch.setattr(bot, "_symbols_to_run", lambda: ["TOPIX"])
    monkeypatch.setattr(bot, "_history_prefetch_plan_for_symbol", lambda symbol: [("MINUTE_15", 32)])
    monkeypatch.setattr(bot, "_history_prefetch_has_time_coverage", lambda *args, **kwargs: False)
    try:
        summary = bot._prefill_price_history_from_broker(force_all_symbols=True, max_symbols=1)
        raw_rows = bot.store.load_recent_price_history("TOPIX", limit=100)
        coarse_rows = bot.store.load_recent_candle_history("TOPIX", resolution_sec=900, limit=100)

        assert summary["symbols_fetched"] == 1
        assert len(raw_rows) == 0
        assert len(coarse_rows) == 32
        assert float(coarse_rows[0]["open"]) == pytest.approx(3649.0)
        assert float(coarse_rows[0]["high"]) == pytest.approx(3652.0)
        assert float(coarse_rows[0]["low"]) == pytest.approx(3647.5)
        assert float(coarse_rows[0]["close"]) == pytest.approx(3650.0)
    finally:
        bot.stop()


def test_prefill_stores_minute_history_in_candle_table_for_minute_coverage(tmp_path, monkeypatch):
    cfg = _make_config(tmp_path)
    cfg.symbols = ["US100"]
    bot = TradingBot(cfg)

    class _FakeBroker:
        def fetch_recent_price_history(self, symbol: str, *, resolution: str, points: int):
            assert symbol == "US100"
            assert resolution == "MINUTE"
            return [
                (
                    1_700_100_100.0 + idx * 60.0,
                    18000.0 + idx * 0.5,
                    10.0 + idx,
                    17999.0 + idx * 0.5,
                    18002.0 + idx * 0.5,
                    17997.5 + idx * 0.5,
                )
                for idx in range(points)
            ]

        def close(self) -> None:
            return None

    bot.broker = _FakeBroker()  # type: ignore[assignment]
    monkeypatch.setattr(bot, "_reserve_ig_non_trading_budget", lambda **kwargs: True)
    monkeypatch.setattr(bot, "_symbols_to_run", lambda: ["US100"])
    monkeypatch.setattr(bot, "_history_prefetch_plan_for_symbol", lambda symbol: [("MINUTE", 32)])
    monkeypatch.setattr(bot, "_history_prefetch_has_time_coverage", lambda *args, **kwargs: False)
    try:
        summary = bot._prefill_price_history_from_broker(force_all_symbols=True, max_symbols=1)
        raw_rows = bot.store.load_recent_price_history("US100", limit=100)
        minute_rows = bot.store.load_recent_candle_history("US100", resolution_sec=60, limit=100)

        assert summary["symbols_fetched"] == 1
        assert len(raw_rows) == 32
        assert len(minute_rows) == 32
        assert float(minute_rows[0]["open"]) == pytest.approx(17999.0)
        assert float(minute_rows[0]["high"]) == pytest.approx(18002.0)
        assert float(minute_rows[0]["low"]) == pytest.approx(17997.5)
        assert float(minute_rows[0]["close"]) == pytest.approx(18000.0)
    finally:
        bot.stop()


def test_prefill_price_history_respects_max_symbols_without_counting_covered_symbols(tmp_path, monkeypatch):
    cfg = _make_config(tmp_path)
    cfg.symbols = ["EURUSD", "GBPUSD", "TOPIX"]
    bot = TradingBot(cfg)

    class _FakeBroker:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def fetch_recent_price_history(self, symbol: str, *, resolution: str, points: int):
            _ = resolution, points
            self.calls.append(symbol)
            return [(1_700_000_000.0, 1.0, 1.0)]

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    monkeypatch.setattr(bot, "_reserve_ig_non_trading_budget", lambda **kwargs: True)
    monkeypatch.setattr(
        bot,
        "_history_prefetch_has_time_coverage",
        lambda symbol, **kwargs: symbol == "EURUSD",
    )
    try:
        bot._prefill_price_history_from_broker(force_all_symbols=False, max_symbols=1)
        assert fake.calls == ["GBPUSD"]
    finally:
        bot.stop()


def test_prefill_price_history_budget_defer_is_not_counted_as_failure(tmp_path, monkeypatch):
    cfg = _make_config(tmp_path)
    cfg.symbols = ["US100", "US2000"]
    bot = TradingBot(cfg)

    class _UnusedBroker:
        def fetch_recent_price_history(self, symbol: str, *, resolution: str, points: int):
            raise AssertionError("history fetch should be deferred before broker call")

        def close(self) -> None:
            return None

    bot.broker = _UnusedBroker()  # type: ignore[assignment]
    monkeypatch.setattr(bot, "_history_prefetch_has_time_coverage", lambda *args, **kwargs: False)
    monkeypatch.setattr(bot, "_reserve_ig_non_trading_budget", lambda **kwargs: False)
    try:
        summary = bot._prefill_price_history_from_broker(force_all_symbols=False, max_symbols=2)
        assert summary["symbols_fetched"] == 0
        assert summary["symbols_failed"] == 0
        assert summary["symbols_deferred"] == 2
        assert summary["deferred_symbols"] == ["US100", "US2000"]
    finally:
        bot.stop()


def test_runtime_deferred_startup_tasks_complete_history_catchup_in_fast_startup(tmp_path, monkeypatch):
    cfg = _make_config(tmp_path)
    cfg.mode = RunMode.EXECUTION
    cfg.symbols = ["EURUSD", "GBPUSD"]
    bot = TradingBot(cfg)
    calls = {"spec": 0, "prefetch": 0}
    coverage_checks = iter([False, True])

    monkeypatch.setattr(bot, "_preload_symbol_specs_on_startup", lambda: calls.__setitem__("spec", calls["spec"] + 1))
    monkeypatch.setattr(
        bot,
        "_history_prefetch_complete_for_all_symbols",
        lambda: next(coverage_checks),
    )

    def _fake_prefill(*, force_all_symbols: bool = False, max_symbols: int | None = None):
        calls["prefetch"] += 1
        assert force_all_symbols is False
        assert max_symbols == 2
        return {
            "symbols_total": 2,
            "symbols_fetched": 1,
            "symbols_skipped": 0,
            "symbols_failed": 0,
            "appended_samples": 300,
        }

    monkeypatch.setattr(bot, "_prefill_price_history_from_broker", _fake_prefill)
    try:
        bot._runtime_deferred_symbol_spec_preload_completed = False
        bot._runtime_deferred_history_prewarm_completed = False
        bot._runtime_deferred_startup_completion_recorded = False

        bot._runtime_complete_deferred_startup_tasks(force=True)

        assert calls == {"spec": 1, "prefetch": 1}
        assert bot._runtime_deferred_symbol_spec_preload_completed is True
        assert bot._runtime_deferred_history_prewarm_completed is True
        assert bot._runtime_deferred_startup_completion_recorded is True
        events = bot.store.load_events(limit=10)
        assert any(event.get("message") == "Runtime startup history catch-up" for event in events)
        assert any(event.get("message") == "Startup fast-path deferred tasks complete" for event in events)
    finally:
        bot.stop()


def test_worker_health_watchdog_triggers_reconcile_for_dead_worker(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))

    class _DeadWorker:
        def is_alive(self) -> bool:
            return False

        def join(self, timeout: float | None = None) -> None:
            _ = timeout
            return None

    with bot._workers_lock:
        bot.workers["EURUSD"] = _DeadWorker()  # type: ignore[assignment]

    calls = {"reconcile": 0}
    monkeypatch.setattr(
        bot,
        "_reconcile_workers",
        lambda now_utc=None: calls.__setitem__("reconcile", calls["reconcile"] + 1),
    )
    try:
        dead = bot._worker_health_check_once()
        assert dead == ["EURUSD"]
        assert calls["reconcile"] == 1
        events = bot.store.load_events(limit=10)
        watchdog_events = [
            event
            for event in events
            if event.get("message") == "Worker health watchdog detected stopped workers"
        ]
        assert watchdog_events
    finally:
        bot.stop()


def test_db_first_tick_symbol_selector_prefers_active_with_periodic_passive(tmp_path):
    bot = TradingBot(_make_config(tmp_path))
    try:
        active = ["US100", "WTI"]
        passive = ["GOLD", "BRENT"]
        bot._db_first_tick_passive_every_n_active = 3
        sequence = [bot._select_db_first_tick_symbol(active, passive) for _ in range(8)]
        assert sequence == ["US100", "WTI", "US100", "GOLD", "WTI", "US100", "WTI", "BRENT"]
    finally:
        bot.stop()


def test_db_first_tick_symbol_selector_prioritizes_high_priority_active_symbols_five_to_one(tmp_path):
    bot = TradingBot(_make_config(tmp_path))
    try:
        active = ["US100", "WTI", "DE40"]
        bot._db_first_tick_priority_symbols = ["US100"]
        sequence = [bot._select_db_first_tick_symbol(active, []) for _ in range(12)]
        assert sequence == ["US100", "US100", "US100", "US100", "US100", "WTI", "US100", "US100", "US100", "US100", "US100", "DE40"]
    finally:
        bot.stop()


def test_force_passive_history_refresh_skips_when_allowance_backoff_active(tmp_path):
    bot = TradingBot(_make_config(tmp_path))

    class _FakeBroker:
        def __init__(self) -> None:
            self.price_calls = 0

        def get_public_api_backoff_remaining_sec(self) -> float:
            return 2.5

        def get_market_data_wait_remaining_sec(self) -> float:
            return 0.0

        def get_price(self, symbol: str) -> PriceTick:
            _ = symbol
            self.price_calls += 1
            raise AssertionError("get_price must not be called during active allowance backoff")

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    try:
        bot._refresh_passive_price_history(force=True)
        assert fake.price_calls == 0
    finally:
        bot.stop()


def test_force_passive_history_refresh_does_not_block_on_rest_market_wait(tmp_path):
    bot = TradingBot(_make_config(tmp_path))

    class _FakeBroker:
        def __init__(self) -> None:
            self.price_calls = 0

        def get_public_api_backoff_remaining_sec(self) -> float:
            return 0.0

        def get_market_data_wait_remaining_sec(self) -> float:
            return 0.4

        def get_price(self, symbol: str) -> PriceTick:
            _ = symbol
            self.price_calls += 1
            raise AssertionError("get_price must not be called while market wait is active in force mode")

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    try:
        bot._refresh_passive_price_history(force=True)
        assert fake.price_calls == 0
    finally:
        bot.stop()


def test_passive_history_refresh_stops_cycle_on_allowance_error(tmp_path):
    cfg = _make_config(tmp_path)
    cfg.symbols = ["EURUSD", "GBPUSD"]
    bot = TradingBot(cfg)

    class _FakeBroker:
        def __init__(self) -> None:
            self.requested_symbols: list[str] = []

        def get_public_api_backoff_remaining_sec(self) -> float:
            return 0.0

        def get_market_data_wait_remaining_sec(self) -> float:
            return 0.0

        def get_price(self, symbol: str) -> PriceTick:
            self.requested_symbols.append(symbol)
            raise RuntimeError(
                'IG API GET /markets failed: 403 Forbidden {"errorCode":"error.public-api.exceeded-api-key-allowance"}'
            )

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    try:
        bot._refresh_passive_price_history(force=False)
        assert fake.requested_symbols == ["EURUSD"]
    finally:
        bot.stop()


def test_monitor_workers_runs_sync_paths_even_during_allowance_backoff(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))
    calls = {"sync": 0, "missing_backfill": 0, "reconcile": 0, "refresh": 0, "housekeeping": 0}

    monkeypatch.setattr(bot, "_broker_public_api_backoff_remaining_sec", lambda: 5.0)
    monkeypatch.setattr(bot, "_runtime_sync_open_positions", lambda force=False: calls.__setitem__("sync", calls["sync"] + 1))
    monkeypatch.setattr(
        bot,
        "_runtime_backfill_missing_on_broker_trades",
        lambda force=False: calls.__setitem__("missing_backfill", calls["missing_backfill"] + 1),
    )
    monkeypatch.setattr(bot, "_reconcile_workers", lambda now_utc=None: calls.__setitem__("reconcile", calls["reconcile"] + 1))
    monkeypatch.setattr(bot, "_refresh_passive_price_history", lambda force=False: calls.__setitem__("refresh", calls["refresh"] + 1))
    monkeypatch.setattr(bot.store, "run_housekeeping", lambda force=False: calls.__setitem__("housekeeping", calls["housekeeping"] + 1))

    try:
        bot._monitor_workers()
        assert calls["housekeeping"] == 1
        assert calls["reconcile"] == 1
        assert calls["sync"] == 1
        assert calls["missing_backfill"] == 1
        assert calls["refresh"] == 1
    finally:
        bot.stop()


def test_db_first_disconnect_error_is_globally_throttled_and_reconnect_is_attempted(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))
    reconnect_calls = {"count": 0}
    recorded_events: list[tuple[str, str | None, str, dict[str, object] | None]] = []

    def _record_event(level, symbol, message, payload):
        recorded_events.append((str(level), symbol, str(message), payload if isinstance(payload, dict) else None))

    def _fake_reconnect(now_ts=None):
        _ = now_ts
        reconnect_calls["count"] += 1
        return False

    monkeypatch.setattr(bot.store, "record_event", _record_event)
    monkeypatch.setattr(bot, "_maybe_reconnect_broker_for_db_first", _fake_reconnect)
    bot._db_first_disconnect_error_log_interval_sec = 60.0

    try:
        bot._maybe_record_db_first_tick_cache_refresh_error("EURUSD", Exception("IG API client is not connected"))
        bot._maybe_record_db_first_tick_cache_refresh_error("GBPUSD", Exception("IG API client is not connected"))

        assert reconnect_calls["count"] == 2
        disconnect_events = [
            item for item in recorded_events if item[2] == "DB-first tick cache paused: broker disconnected"
        ]
        assert len(disconnect_events) == 1
        assert disconnect_events[0][1] is None
    finally:
        bot.stop()


def test_monitor_workers_tolerates_housekeeping_failure(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))
    calls = {"sync": 0, "reconcile": 0, "refresh": 0}

    monkeypatch.setattr(bot, "_broker_public_api_backoff_remaining_sec", lambda: 0.0)
    monkeypatch.setattr(bot, "_runtime_sync_open_positions", lambda force=False: calls.__setitem__("sync", calls["sync"] + 1))
    monkeypatch.setattr(bot, "_reconcile_workers", lambda now_utc=None: calls.__setitem__("reconcile", calls["reconcile"] + 1))
    monkeypatch.setattr(bot, "_refresh_passive_price_history", lambda force=False: calls.__setitem__("refresh", calls["refresh"] + 1))
    monkeypatch.setattr(bot.store, "run_housekeeping", lambda force=False: (_ for _ in ()).throw(RuntimeError("db busy")))

    try:
        bot._monitor_workers()
        assert calls["sync"] == 1
        assert calls["reconcile"] == 1
        assert calls["refresh"] == 1
    finally:
        bot.stop()


def test_monitor_workers_tolerates_runtime_sync_failure_and_continues(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))
    calls = {"missing": 0, "closed": 0, "reconcile": 0, "refresh": 0}
    recorded_events: list[tuple[str, str | None, str, dict[str, object] | None]] = []

    monkeypatch.setattr(bot, "_flush_ig_rate_limit_worker_metrics", lambda now_monotonic: None)
    monkeypatch.setattr(bot, "_maybe_record_trade_reason_summary", lambda now_monotonic: None)
    monkeypatch.setattr(
        bot,
        "_runtime_sync_open_positions",
        lambda force=False: (_ for _ in ()).throw(RuntimeError("sync exploded")),
    )
    monkeypatch.setattr(
        bot,
        "_runtime_backfill_missing_on_broker_trades",
        lambda: calls.__setitem__("missing", calls["missing"] + 1),
    )
    monkeypatch.setattr(
        bot,
        "_runtime_backfill_closed_trade_details",
        lambda: calls.__setitem__("closed", calls["closed"] + 1),
    )
    monkeypatch.setattr(
        bot,
        "_reconcile_workers",
        lambda now_utc=None: calls.__setitem__("reconcile", calls["reconcile"] + 1),
    )
    monkeypatch.setattr(
        bot,
        "_refresh_passive_price_history",
        lambda force=False: calls.__setitem__("refresh", calls["refresh"] + 1),
    )
    monkeypatch.setattr(
        bot.store,
        "record_event",
        lambda level, symbol, message, payload=None: recorded_events.append(
            (str(level), symbol, str(message), payload if isinstance(payload, dict) else None)
        ),
    )

    try:
        bot._monitor_workers()
        assert calls["missing"] == 1
        assert calls["closed"] == 1
        assert calls["reconcile"] == 1
        assert calls["refresh"] == 1
        failure_events = [item for item in recorded_events if item[2] == "Runtime monitor task failed"]
        assert len(failure_events) == 1
        assert failure_events[0][3] == {
            "task": "runtime_sync_open_positions",
            "error": "sync exploded",
            "error_type": "RuntimeError",
        }
    finally:
        bot.stop()


def test_stop_records_shutdown_requested_and_completed_events(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))
    recorded_events: list[tuple[str, str | None, str, dict[str, object] | None]] = []

    monkeypatch.setattr(
        bot.store,
        "record_event",
        lambda level, symbol, message, payload=None: recorded_events.append(
            (str(level), symbol, str(message), payload if isinstance(payload, dict) else None)
        ),
    )

    bot.request_graceful_stop(reason="signal:SIGTERM", source="cli_signal_handler")
    bot.stop()

    shutdown_requested = [item for item in recorded_events if item[2] == "Bot shutdown requested"]
    shutdown_completed = [item for item in recorded_events if item[2] == "Bot shutdown completed"]

    assert len(shutdown_requested) == 1
    assert shutdown_requested[0][3] is not None
    assert shutdown_requested[0][3]["reason"] == "signal:SIGTERM"
    assert shutdown_requested[0][3]["source"] == "cli_signal_handler"
    assert shutdown_requested[0][3]["force_close"] is True

    assert len(shutdown_completed) == 1
    assert shutdown_completed[0][3] is not None
    assert shutdown_completed[0][3]["reason"] == "signal:SIGTERM"
    assert shutdown_completed[0][3]["alive_worker_count"] == 0


def test_worker_health_watchdog_restarts_stale_heartbeat_worker(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))
    recorded_events: list[tuple[str, str | None, str, dict[str, object] | None]] = []
    restarts: list[tuple[str, str]] = []

    class _HungWorker:
        name = "worker-EURUSD"
        strategy_name = "momentum"
        poll_interval_sec = 1.0
        worker_state_flush_interval_sec = 5.0

        def __init__(self) -> None:
            self._last_saved_worker_state_ts = time.time() - 120.0
            self._last_saved_worker_state_signature = (None, "simulated_stall")

        @staticmethod
        def is_alive() -> bool:
            return True

        @staticmethod
        def join(timeout: float | None = None) -> None:
            _ = timeout
            return None

    with bot._workers_lock:
        bot.workers["EURUSD"] = _HungWorker()  # type: ignore[assignment]

    monkeypatch.setattr(
        bot.store,
        "record_event",
        lambda level, symbol, message, payload=None: recorded_events.append(
            (str(level), symbol, str(message), payload if isinstance(payload, dict) else None)
        ),
    )
    monkeypatch.setattr(
        bot,
        "_restart_worker_for_symbol",
        lambda symbol, reason: restarts.append((str(symbol), str(reason))) or True,
    )

    try:
        stale = bot._worker_stale_heartbeat_check_once()
        assert stale == ["EURUSD"]
        assert restarts == [("EURUSD", "watchdog_stale_heartbeat")]
        assert any(
            item[2] == "Worker health watchdog detected stale heartbeat"
            and item[1] == "EURUSD"
            and item[3] is not None
            and item[3].get("last_error") == "simulated_stall"
            for item in recorded_events
        )
    finally:
        bot.stop()


def test_worker_health_watchdog_treats_legacy_monotonic_heartbeat_timestamp_as_fresh(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))
    recorded_events: list[tuple[str, str | None, str, dict[str, object] | None]] = []
    restarts: list[tuple[str, str]] = []
    now_monotonic = time.monotonic()

    class _LegacyWorker:
        name = "worker-EURUSD"
        strategy_name = "momentum"
        poll_interval_sec = 1.0
        worker_state_flush_interval_sec = 5.0

        def __init__(self) -> None:
            self._last_saved_worker_state_ts = now_monotonic - 5.0
            self._last_saved_worker_state_signature = (None, "healthy_recent_flush")

        @staticmethod
        def is_alive() -> bool:
            return True

        @staticmethod
        def join(timeout: float | None = None) -> None:
            _ = timeout
            return None

    with bot._workers_lock:
        bot.workers["EURUSD"] = _LegacyWorker()  # type: ignore[assignment]

    monkeypatch.setattr(
        bot.store,
        "record_event",
        lambda level, symbol, message, payload=None: recorded_events.append(
            (str(level), symbol, str(message), payload if isinstance(payload, dict) else None)
        ),
    )
    monkeypatch.setattr(
        bot,
        "_restart_worker_for_symbol",
        lambda symbol, reason: restarts.append((str(symbol), str(reason))) or True,
    )

    try:
        stale = bot._worker_stale_heartbeat_check_once()
        assert stale == []
        assert restarts == []
        assert not any(item[2] == "Worker health watchdog detected stale heartbeat" for item in recorded_events)
    finally:
        bot.stop()


def test_worker_health_watchdog_skips_stale_restart_during_blocking_broker_operation(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))
    recorded_events: list[tuple[str, str | None, str, dict[str, object] | None]] = []
    restarts: list[tuple[str, str]] = []
    now = time.time()

    class _BlockedWorker:
        name = "worker-EURUSD"
        strategy_name = "momentum"
        poll_interval_sec = 1.0
        worker_state_flush_interval_sec = 5.0

        def __init__(self) -> None:
            self._last_saved_worker_state_ts = now - 120.0
            self._last_saved_worker_state_signature = (None, "opening_position")

        @staticmethod
        def is_alive() -> bool:
            return True

        @staticmethod
        def join(timeout: float | None = None) -> None:
            _ = timeout
            return None

        @staticmethod
        def watchdog_blocking_operation_status() -> dict[str, object]:
            return {
                "operation": "open_position",
                "started_at": now - 30.0,
                "grace_sec": 180.0,
            }

    with bot._workers_lock:
        bot.workers["EURUSD"] = _BlockedWorker()  # type: ignore[assignment]

    monkeypatch.setattr(
        bot.store,
        "record_event",
        lambda level, symbol, message, payload=None: recorded_events.append(
            (str(level), symbol, str(message), payload if isinstance(payload, dict) else None)
        ),
    )
    monkeypatch.setattr(
        bot,
        "_restart_worker_for_symbol",
        lambda symbol, reason: restarts.append((str(symbol), str(reason))) or True,
    )

    try:
        stale = bot._worker_stale_heartbeat_check_once()
        assert stale == []
        assert restarts == []
        assert not any(item[2] == "Worker health watchdog detected stale heartbeat" for item in recorded_events)
    finally:
        bot.stop()


def test_worker_health_watchdog_restarts_stale_worker_after_blocking_operation_grace_expires(
    tmp_path,
    monkeypatch,
):
    bot = TradingBot(_make_config(tmp_path))
    recorded_events: list[tuple[str, str | None, str, dict[str, object] | None]] = []
    restarts: list[tuple[str, str]] = []
    now = time.time()

    class _ExpiredBlockedWorker:
        name = "worker-EURUSD"
        strategy_name = "momentum"
        poll_interval_sec = 1.0
        worker_state_flush_interval_sec = 5.0

        def __init__(self) -> None:
            self._last_saved_worker_state_ts = now - 120.0
            self._last_saved_worker_state_signature = (None, "opening_position")

        @staticmethod
        def is_alive() -> bool:
            return True

        @staticmethod
        def join(timeout: float | None = None) -> None:
            _ = timeout
            return None

        @staticmethod
        def watchdog_blocking_operation_status() -> dict[str, object]:
            return {
                "operation": "open_position",
                "started_at": now - 240.0,
                "grace_sec": 180.0,
            }

    with bot._workers_lock:
        bot.workers["EURUSD"] = _ExpiredBlockedWorker()  # type: ignore[assignment]

    monkeypatch.setattr(
        bot.store,
        "record_event",
        lambda level, symbol, message, payload=None: recorded_events.append(
            (str(level), symbol, str(message), payload if isinstance(payload, dict) else None)
        ),
    )
    monkeypatch.setattr(
        bot,
        "_restart_worker_for_symbol",
        lambda symbol, reason: restarts.append((str(symbol), str(reason))) or True,
    )

    try:
        stale = bot._worker_stale_heartbeat_check_once()
        assert stale == ["EURUSD"]
        assert restarts == [("EURUSD", "watchdog_stale_heartbeat")]
        stale_events = [
            item for item in recorded_events if item[2] == "Worker health watchdog detected stale heartbeat"
        ]
        assert len(stale_events) == 1
        assert stale_events[0][3] is not None
        assert stale_events[0][3]["blocking_operation"] == "open_position"
    finally:
        bot.stop()


def test_runtime_monitor_watchdog_escalates_stale_main_loop(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))
    recorded_events: list[tuple[str, str | None, str, dict[str, object] | None]] = []
    trace_dumps: list[str] = []
    aborts: list[str] = []

    monkeypatch.setattr(
        bot.store,
        "record_event",
        lambda level, symbol, message, payload=None: recorded_events.append(
            (str(level), symbol, str(message), payload if isinstance(payload, dict) else None)
        ),
    )
    monkeypatch.setattr(bot, "_dump_runtime_thread_traces", lambda reason: trace_dumps.append(str(reason)))
    monkeypatch.setattr(bot, "_abort_process_for_watchdog", lambda reason: aborts.append(str(reason)))

    now_monotonic = time.monotonic()
    bot._runtime_monitor_watchdog_enabled = True
    bot._runtime_monitor_last_started_monotonic = now_monotonic - 400.0
    bot._runtime_monitor_last_completed_monotonic = now_monotonic - 400.0
    bot._runtime_monitor_last_progress_monotonic = now_monotonic - 400.0

    try:
        assert bot._monitor_loop_health_check_once() is True
        assert trace_dumps == ["runtime_monitor_stale"]
        assert aborts == []
        assert any(item[2] == "Runtime monitor watchdog detected stale main loop" for item in recorded_events)

        bot._runtime_monitor_stall_first_detected_monotonic = time.monotonic() - 31.0
        bot._runtime_monitor_last_started_monotonic = time.monotonic() - 400.0
        bot._runtime_monitor_last_completed_monotonic = time.monotonic() - 400.0
        bot._runtime_monitor_last_progress_monotonic = time.monotonic() - 400.0
        assert bot._monitor_loop_health_check_once() is True
        assert aborts == ["runtime_monitor_stale"]
    finally:
        bot.stop()


def test_runtime_monitor_watchdog_treats_recent_task_progress_as_healthy(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))
    recorded_events: list[tuple[str, str | None, str, dict[str, object] | None]] = []

    monkeypatch.setattr(
        bot.store,
        "record_event",
        lambda level, symbol, message, payload=None: recorded_events.append(
            (str(level), symbol, str(message), payload if isinstance(payload, dict) else None)
        ),
    )

    now_monotonic = time.monotonic()
    bot._runtime_monitor_watchdog_enabled = True
    bot._runtime_monitor_last_started_monotonic = now_monotonic - 400.0
    bot._runtime_monitor_last_completed_monotonic = now_monotonic - 400.0
    bot._runtime_monitor_last_progress_monotonic = now_monotonic - 5.0
    bot._runtime_monitor_active_task_name = "runtime_sync_open_positions"

    try:
        assert bot._monitor_loop_health_check_once() is False
        assert not any(item[2] == "Runtime monitor watchdog detected stale main loop" for item in recorded_events)
    finally:
        bot.stop()


def test_monitor_workers_defers_noncritical_ig_tasks_when_account_non_trading_is_hot(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))
    calls = {"sync": 0, "missing": 0, "closed": 0, "startup": 0, "reconcile": 0}
    recorded_events: list[tuple[str, str | None, str, dict[str, object] | None]] = []

    class _StoppedCacheThread:
        @staticmethod
        def is_alive() -> bool:
            return False

        @staticmethod
        def join(timeout: float | None = None) -> None:
            _ = timeout
            return None

    bot._last_ig_account_non_trading_snapshot = {
        "used_value": 30,
        "limit_value": 30,
        "remaining_value": 0,
        "window_sec": 60.0,
        "blocked_total": 123,
    }

    monkeypatch.setattr(bot, "_flush_ig_rate_limit_worker_metrics", lambda now_monotonic: None)
    monkeypatch.setattr(bot, "_maybe_record_trade_reason_summary", lambda now_monotonic: None)
    monkeypatch.setattr(
        bot,
        "_runtime_sync_open_positions",
        lambda force=False: calls.__setitem__("sync", calls["sync"] + 1),
    )
    monkeypatch.setattr(
        bot,
        "_runtime_backfill_missing_on_broker_trades",
        lambda force=False: calls.__setitem__("missing", calls["missing"] + 1),
    )
    monkeypatch.setattr(
        bot,
        "_runtime_backfill_closed_trade_details",
        lambda force=False: calls.__setitem__("closed", calls["closed"] + 1),
    )
    monkeypatch.setattr(
        bot,
        "_runtime_complete_deferred_startup_tasks",
        lambda force=False: calls.__setitem__("startup", calls["startup"] + 1),
    )
    monkeypatch.setattr(
        bot,
        "_reconcile_workers",
        lambda now_utc=None: calls.__setitem__("reconcile", calls["reconcile"] + 1),
    )
    monkeypatch.setattr(bot, "_db_first_enabled", lambda: True)
    bot._db_first_cache_threads = {"cache": _StoppedCacheThread()}  # type: ignore[assignment]
    monkeypatch.setattr(
        bot.store,
        "record_event",
        lambda level, symbol, message, payload=None: recorded_events.append(
            (str(level), symbol, str(message), payload if isinstance(payload, dict) else None)
        ),
    )

    try:
        bot._monitor_workers()
        assert calls["sync"] == 1
        assert calls["missing"] == 0
        assert calls["closed"] == 0
        assert calls["startup"] == 0
        assert calls["reconcile"] == 1
        deferred = [item for item in recorded_events if item[2] == "Runtime monitor deferred non-critical IG tasks"]
        assert len(deferred) == 1
        assert deferred[0][3] is not None
        assert deferred[0][3]["deferred_tasks"] == [
            "runtime_backfill_missing_on_broker_trades",
            "runtime_backfill_closed_trade_details",
            "runtime_complete_deferred_startup_tasks",
        ]
        assert deferred[0][3]["used_value"] == 30
    finally:
        bot.stop()


def test_abort_process_for_watchdog_requests_graceful_shutdown(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))
    interrupt_calls: list[str] = []
    recorded_events: list[tuple[str, str | None, str, dict[str, object] | None]] = []
    event_flushes: list[float] = []
    multi_flushes: list[float] = []

    monkeypatch.setattr(
        bot.store,
        "record_event",
        lambda level, symbol, message, payload=None: recorded_events.append(
            (str(level), symbol, str(message), payload if isinstance(payload, dict) else None)
        ),
    )
    monkeypatch.setattr(
        bot.store,
        "flush_event_async_writes",
        lambda timeout_sec=1.0: event_flushes.append(float(timeout_sec)) or True,
    )
    monkeypatch.setattr(
        bot.store,
        "flush_multi_async_writes",
        lambda timeout_sec=1.0: multi_flushes.append(float(timeout_sec)) or True,
    )
    monkeypatch.setattr("xtb_bot.bot.core._thread.interrupt_main", lambda: interrupt_calls.append("called"))

    try:
        with pytest.raises(SystemExit) as exc:
            bot._abort_process_for_watchdog(reason="runtime_monitor_stale")

        assert exc.value.code == 1
        assert bot.stop_event.is_set()
        assert bot._worker_health_stop_event.is_set()
        assert interrupt_calls == ["called"]
        assert event_flushes == [2.0]
        assert multi_flushes == [2.0]
        assert any(item[2] == "Runtime watchdog aborting process" for item in recorded_events)
    finally:
        bot.stop()


def test_passive_history_refresh_makes_timestamps_monotonic_when_broker_ts_stalls(tmp_path):
    bot = TradingBot(_make_config(tmp_path))

    class _FakeBroker:
        def __init__(self) -> None:
            self.price_calls = 0

        def get_public_api_backoff_remaining_sec(self) -> float:
            return 0.0

        def get_market_data_wait_remaining_sec(self) -> float:
            return 0.0

        def get_price(self, symbol: str) -> PriceTick:
            self.price_calls += 1
            mid = 1.1000 + self.price_calls * 0.0001
            return PriceTick(
                symbol=symbol,
                bid=mid - 0.00005,
                ask=mid + 0.00005,
                timestamp=1_700_000_000.0,
                volume=10.0 + self.price_calls,
            )

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    try:
        bot._refresh_passive_price_history(force=True)
        bot._refresh_passive_price_history(force=True)

        samples = bot.store.load_recent_price_history("EURUSD", limit=10)
        assert len(samples) >= 2
        assert float(samples[-1]["ts"]) > float(samples[-2]["ts"])
    finally:
        bot.stop()


def test_passive_history_refresh_clamps_future_broker_timestamp(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))

    class _FakeBroker:
        def __init__(self) -> None:
            self.price_calls = 0

        def get_public_api_backoff_remaining_sec(self) -> float:
            return 0.0

        def get_market_data_wait_remaining_sec(self) -> float:
            return 0.0

        def get_price(self, symbol: str) -> PriceTick:
            self.price_calls += 1
            mid = 1.1000 + self.price_calls * 0.0001
            return PriceTick(
                symbol=symbol,
                bid=mid - 0.00005,
                ask=mid + 0.00005,
                timestamp=1_900_000_000.0,
                volume=10.0 + self.price_calls,
            )

        def close(self) -> None:
            return None

    fake = _FakeBroker()
    bot.broker = fake  # type: ignore[assignment]
    monkeypatch.setattr("xtb_bot.bot.core.time.time", lambda: 1_800_000_000.0)
    try:
        bot._refresh_passive_price_history(force=True)
        bot._refresh_passive_price_history(force=True)

        samples = bot.store.load_recent_price_history("EURUSD", limit=10)
        assert len(samples) >= 2
        assert float(samples[-2]["ts"]) == pytest.approx(1_800_000_000.0)
        assert float(samples[-1]["ts"]) > float(samples[-2]["ts"])
        assert float(samples[-1]["ts"]) <= 1_800_000_001.0
    finally:
        bot.stop()


def test_monitor_workers_records_trade_reason_summary_snapshot(tmp_path, monkeypatch):
    bot = TradingBot(_make_config(tmp_path))
    bot._trade_reason_summary_enabled = True
    bot._trade_reason_summary_interval_sec = 60.0
    bot._trade_reason_summary_window_sec = 3_600.0
    bot._trade_reason_summary_scan_limit = 1_000
    bot._trade_reason_summary_top = 10
    bot._last_trade_reason_summary_monotonic = 0.0

    bot.store.record_event("WARN", "EURUSD", "Trade blocked by spread filter", {"spread": 4.2})
    bot.store.record_event(
        "ERROR",
        "DE40",
        "Broker error",
        {
            "error": (
                "IG API POST /positions/otc failed: 400 Bad Request "
                '{"errorCode":"error.service.create.otc.position.instrument.invalid"}'
            )
        },
    )

    monkeypatch.setattr(bot.store, "run_housekeeping", lambda force=False: {"ran": False})
    monkeypatch.setattr(bot, "_broker_public_api_backoff_remaining_sec", lambda: 1.0)
    monkeypatch.setattr(bot, "_reconcile_workers", lambda now_utc=None: None)

    try:
        bot._monitor_workers()
        snapshots = [
            event
            for event in bot.store.load_events(limit=50)
            if str(event.get("message") or "") == "Trade reason summary snapshot"
        ]
        assert snapshots
        payload = snapshots[0].get("payload") or {}
        by_kind = payload.get("by_kind") or {}
        assert int(by_kind.get("block") or 0) >= 1
        assert int(by_kind.get("reject") or 0) >= 1
    finally:
        bot.stop()


def test_runtime_sync_interval_scales_down_for_stale_open_positions(tmp_path):
    bot = TradingBot(_make_config(tmp_path))
    try:
        base_interval = bot._runtime_broker_sync_active_interval_sec
        assert bot._runtime_sync_interval_sec(
            has_local_open_positions=True,
            has_pending_opens=False,
            oldest_open_age_sec=10.0 * 60.0 * 60.0,
        ) == pytest.approx(120.0)
        assert bot._runtime_sync_interval_sec(
            has_local_open_positions=True,
            has_pending_opens=False,
            oldest_open_age_sec=3.0 * 60.0 * 60.0,
        ) == pytest.approx(10.0)
        assert bot._runtime_sync_interval_sec(
            has_local_open_positions=True,
            has_pending_opens=False,
            oldest_open_age_sec=45.0 * 60.0,
        ) == pytest.approx(3.0)
        assert bot._runtime_sync_interval_sec(
            has_local_open_positions=True,
            has_pending_opens=False,
            oldest_open_age_sec=5.0 * 60.0,
        ) == pytest.approx(base_interval)
    finally:
        bot.stop()


def test_runtime_sync_interval_keeps_fast_mode_when_pending_opens_exist(tmp_path):
    bot = TradingBot(_make_config(tmp_path))
    try:
        base_interval = bot._runtime_broker_sync_active_interval_sec
        assert bot._runtime_sync_interval_sec(
            has_local_open_positions=True,
            has_pending_opens=True,
            oldest_open_age_sec=12.0 * 60.0 * 60.0,
        ) == pytest.approx(base_interval)
    finally:
        bot.stop()


def test_db_first_loop_backoff_grows_and_clears(tmp_path):
    bot = TradingBot(_make_config(tmp_path))
    try:
        first = bot._record_db_first_loop_failure(
            "tick",
            base_interval_sec=1.0,
            now_monotonic=100.0,
        )
        second = bot._record_db_first_loop_failure(
            "tick",
            base_interval_sec=1.0,
            now_monotonic=101.0,
        )
        remaining = bot._db_first_loop_backoff_remaining_sec("tick", now_monotonic=101.5)

        assert first == pytest.approx(1.0)
        assert second == pytest.approx(2.0)
        assert remaining == pytest.approx(1.5)

        bot._clear_db_first_loop_backoff("tick")

        assert bot._db_first_loop_backoff_remaining_sec("tick", now_monotonic=102.0) == pytest.approx(0.0)
    finally:
        bot.stop()
