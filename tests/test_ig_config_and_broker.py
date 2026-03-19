from __future__ import annotations

import pytest

from xtb_bot.bot import TradingBot
from xtb_bot.client import MockBrokerClient, XtbApiClient
from xtb_bot.config import BotConfig, ConfigError, RiskConfig, load_config
from xtb_bot.ig_client import IgApiClient
from xtb_bot.models import AccountType, RunMode


def _clear_env(monkeypatch) -> None:
    for key in (
        "BROKER",
        "IG_BROKER",
        "IG_IDENTIFIER",
        "IG_PASSWORD",
        "IG_API_KEY",
        "IG_STREAM_ENABLED",
        "IG_STREAM_TICK_MAX_AGE_SEC",
        "IG_REST_MARKET_MIN_INTERVAL_SEC",
        "IG_WORKER_POLL_JITTER_SEC",
        "IG_SYMBOL_EPICS",
        "IG_SYMBOLS",
        "IG_SYMBOLS_MOMENTUM",
        "IG_SYMBOLS_MOMENTUM_INDEX",
        "IG_SYMBOLS_MOMENTUM_FX",
        "IG_SYMBOLS_G1",
        "IG_SYMBOLS_INDEX_HYBRID",
        "IG_SYMBOLS_CRYPTO_TREND_FOLLOWING",
        "XTB_SYMBOLS",
        "XTB_SYMBOLS_MOMENTUM",
        "XTB_SYMBOLS_MOMENTUM_INDEX",
        "XTB_SYMBOLS_MOMENTUM_FX",
        "XTB_SYMBOLS_G1",
        "XTB_SYMBOLS_INDEX_HYBRID",
        "XTB_SYMBOLS_CRYPTO_TREND_FOLLOWING",
        "IG_INDEX_HYBRID_SYMBOLS",
        "XTB_INDEX_HYBRID_SYMBOLS",
        "IG_ACCOUNT_TYPE",
        "IG_MODE",
        "IG_STRATEGY",
        "IG_STRATEGY_PARAMS",
        "IG_STRATEGY_PRESETS",
        "IG_STRATEGY_PARAMS_MOMENTUM",
        "IG_STRATEGY_PARAMS_MOMENTUM_INDEX",
        "IG_STRATEGY_PARAMS_MOMENTUM_FX",
        "IG_STRATEGY_PARAMS_G1",
        "IG_STRATEGY_PARAMS_INDEX_HYBRID",
        "IG_STRATEGY_PARAMS_CRYPTO_TREND_FOLLOWING",
        "BOT_STRICT_BROKER_CONNECT",
        "IG_STRICT_BROKER_CONNECT",
        "XTB_STRICT_BROKER_CONNECT",
        "XTB_USER_ID",
        "XTB_PASSWORD",
        "XTB_STRATEGY",
        "XTB_STRATEGY_PARAMS",
        "XTB_STRATEGY_PRESETS",
        "XTB_STRATEGY_PARAMS_MOMENTUM",
        "XTB_STRATEGY_PARAMS_MOMENTUM_INDEX",
        "XTB_STRATEGY_PARAMS_MOMENTUM_FX",
        "XTB_STRATEGY_PARAMS_G1",
        "XTB_STRATEGY_PARAMS_INDEX_HYBRID",
        "XTB_STRATEGY_PARAMS_CRYPTO_TREND_FOLLOWING",
    ):
        monkeypatch.delenv(key, raising=False)


def test_load_config_ig_credentials_from_env(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "ig")
    monkeypatch.setenv("IG_IDENTIFIER", "ig-user")
    monkeypatch.setenv("IG_PASSWORD", "ig-pass")
    monkeypatch.setenv("IG_API_KEY", "ig-key")
    monkeypatch.setenv("IG_STREAM_ENABLED", "false")
    monkeypatch.setenv("IG_STREAM_TICK_MAX_AGE_SEC", "12")
    monkeypatch.setenv("IG_REST_MARKET_MIN_INTERVAL_SEC", "2.5")
    monkeypatch.setenv("IG_WORKER_POLL_JITTER_SEC", "0.7")
    monkeypatch.setenv("IG_SYMBOL_EPICS", '{"EURUSD":"CS.D.EURUSD.CFD.IP"}')

    cfg = load_config()
    assert cfg.broker == "ig"
    assert cfg.user_id == "ig-user"
    assert cfg.password == "ig-pass"
    assert cfg.api_key == "ig-key"
    assert cfg.ig_stream_enabled is False
    assert cfg.ig_stream_tick_max_age_sec == pytest.approx(12.0)
    assert cfg.ig_rest_market_min_interval_sec == pytest.approx(2.5)
    assert cfg.worker_poll_jitter_sec == pytest.approx(0.7)
    assert cfg.symbol_epics == {"EURUSD": "CS.D.EURUSD.CFD.IP"}


def test_load_config_defaults_to_ig_when_broker_not_set(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("IG_IDENTIFIER", "ig-user")
    monkeypatch.setenv("IG_PASSWORD", "ig-pass")
    monkeypatch.setenv("IG_API_KEY", "ig-key")

    cfg = load_config()
    assert cfg.broker == "ig"


def test_load_config_ig_requires_api_key(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "ig")
    monkeypatch.setenv("IG_IDENTIFIER", "ig-user")
    monkeypatch.setenv("IG_PASSWORD", "ig-pass")

    with pytest.raises(ConfigError, match="IG credentials are required"):
        load_config()


def test_load_config_ig_stream_override_has_priority_over_env(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "ig")
    monkeypatch.setenv("IG_IDENTIFIER", "ig-user")
    monkeypatch.setenv("IG_PASSWORD", "ig-pass")
    monkeypatch.setenv("IG_API_KEY", "ig-key")
    monkeypatch.setenv("IG_STREAM_ENABLED", "false")

    cfg = load_config(ig_stream_enabled_override=True)
    assert cfg.ig_stream_enabled is True


def test_load_config_strict_broker_connect_from_env(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "ig")
    monkeypatch.setenv("IG_IDENTIFIER", "ig-user")
    monkeypatch.setenv("IG_PASSWORD", "ig-pass")
    monkeypatch.setenv("IG_API_KEY", "ig-key")
    monkeypatch.setenv("BOT_STRICT_BROKER_CONNECT", "true")

    cfg = load_config()
    assert cfg.strict_broker_connect is True


def test_load_config_index_hybrid_uses_strategy_specific_symbols(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "ig")
    monkeypatch.setenv("IG_IDENTIFIER", "ig-user")
    monkeypatch.setenv("IG_PASSWORD", "ig-pass")
    monkeypatch.setenv("IG_API_KEY", "ig-key")
    monkeypatch.setenv("IG_STRATEGY", "index_hybrid")
    monkeypatch.setenv("IG_SYMBOLS", "EURUSD,GBPUSD")
    monkeypatch.setenv("IG_INDEX_HYBRID_SYMBOLS", "US500,US100,DE40,UK100")

    cfg = load_config()
    assert cfg.strategy == "index_hybrid"
    assert cfg.symbols == ["US500", "US100", "DE40", "UK100"]


def test_load_config_ig_uses_strategy_scoped_symbols_for_active_strategy(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "ig")
    monkeypatch.setenv("IG_IDENTIFIER", "ig-user")
    monkeypatch.setenv("IG_PASSWORD", "ig-pass")
    monkeypatch.setenv("IG_API_KEY", "ig-key")
    monkeypatch.setenv("IG_STRATEGY", "momentum")
    monkeypatch.setenv("IG_SYMBOLS", "EURUSD,GBPUSD")
    monkeypatch.setenv("IG_SYMBOLS_MOMENTUM", "US500,US100")
    monkeypatch.setenv("IG_SYMBOLS_G1", "XAUUSD")

    cfg = load_config()
    assert cfg.strategy == "momentum"
    assert cfg.symbols == ["US500", "US100"]


def test_load_config_ig_uses_momentum_index_alias_symbols_and_params(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "ig")
    monkeypatch.setenv("IG_IDENTIFIER", "ig-user")
    monkeypatch.setenv("IG_PASSWORD", "ig-pass")
    monkeypatch.setenv("IG_API_KEY", "ig-key")
    monkeypatch.setenv("IG_STRATEGY", "momentum_index")
    monkeypatch.setenv("IG_SYMBOLS", "EURUSD,GBPUSD")
    monkeypatch.setenv("IG_SYMBOLS_MOMENTUM_INDEX", "US100,US500,DE40")
    monkeypatch.setenv(
        "IG_STRATEGY_PARAMS_MOMENTUM_INDEX",
        '{"momentum_max_spread_pips":1.8,"momentum_execution_min_confidence_for_entry":0.7}',
    )

    cfg = load_config()
    assert cfg.strategy == "momentum_index"
    assert cfg.symbols == ["US100", "US500", "DE40"]
    assert cfg.strategy_params["momentum_max_spread_pips"] == pytest.approx(1.8)
    assert cfg.strategy_params["momentum_execution_min_confidence_for_entry"] == pytest.approx(0.7)


def test_load_config_xtb_uses_strategy_scoped_symbols_for_active_strategy(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "xtb")
    monkeypatch.setenv("XTB_USER_ID", "xtb-user")
    monkeypatch.setenv("XTB_PASSWORD", "xtb-pass")
    monkeypatch.setenv("XTB_STRATEGY", "g1")
    monkeypatch.setenv("XTB_SYMBOLS", "EURUSD,GBPUSD")
    monkeypatch.setenv("XTB_SYMBOLS_G1", "US500,US100,DE40")
    monkeypatch.setenv("XTB_SYMBOLS_MOMENTUM", "USDJPY")

    cfg = load_config()
    assert cfg.strategy == "g1"
    assert cfg.symbols == ["US500", "US100", "DE40"]


def test_load_config_index_hybrid_prefers_new_scoped_symbols_over_legacy(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "ig")
    monkeypatch.setenv("IG_IDENTIFIER", "ig-user")
    monkeypatch.setenv("IG_PASSWORD", "ig-pass")
    monkeypatch.setenv("IG_API_KEY", "ig-key")
    monkeypatch.setenv("IG_STRATEGY", "index_hybrid")
    monkeypatch.setenv("IG_SYMBOLS", "EURUSD,GBPUSD")
    monkeypatch.setenv("IG_INDEX_HYBRID_SYMBOLS", "US500,US100,DE40,UK100")
    monkeypatch.setenv("IG_SYMBOLS_INDEX_HYBRID", "US500,US100")

    cfg = load_config()
    assert cfg.strategy == "index_hybrid"
    assert cfg.symbols == ["US500", "US100"]


def test_load_config_index_hybrid_defaults_to_base_index_symbols(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "ig")
    monkeypatch.setenv("IG_IDENTIFIER", "ig-user")
    monkeypatch.setenv("IG_PASSWORD", "ig-pass")
    monkeypatch.setenv("IG_API_KEY", "ig-key")
    monkeypatch.setenv("IG_STRATEGY", "index_hybrid")

    cfg = load_config()
    assert cfg.strategy == "index_hybrid"
    assert cfg.symbols == ["US500", "US100", "DE40", "UK100"]


def test_load_config_xtb_strategy_params_layers(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "xtb")
    monkeypatch.setenv("XTB_USER_ID", "xtb-user")
    monkeypatch.setenv("XTB_PASSWORD", "xtb-pass")
    monkeypatch.setenv("XTB_STRATEGY", "momentum")
    monkeypatch.setenv(
        "XTB_STRATEGY_PARAMS",
        '{"momentum_confirm_bars":3,"momentum_min_stop_loss_pips":20.0}',
    )
    monkeypatch.setenv(
        "XTB_STRATEGY_PRESETS",
        '{"momentum":{"momentum_confirm_bars":4}}',
    )
    monkeypatch.setenv(
        "XTB_STRATEGY_PARAMS_MOMENTUM",
        '{"momentum_confirm_bars":5}',
    )

    cfg = load_config()
    assert cfg.strategy == "momentum"
    assert cfg.strategy_params["momentum_confirm_bars"] == 5
    assert cfg.strategy_params["momentum_min_stop_loss_pips"] == 20.0


def test_load_config_xtb_strategy_presets_pick_active_strategy(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "xtb")
    monkeypatch.setenv("XTB_USER_ID", "xtb-user")
    monkeypatch.setenv("XTB_PASSWORD", "xtb-pass")
    monkeypatch.setenv("XTB_STRATEGY", "g1")
    monkeypatch.setenv(
        "XTB_STRATEGY_PRESETS",
        '{"momentum":{"momentum_confirm_bars":4},"g1":{"g1_fast_ema_window":13}}',
    )

    cfg = load_config()
    assert cfg.strategy == "g1"
    assert cfg.strategy_params["g1_fast_ema_window"] == 13
    # unchanged default, because momentum preset must not apply when strategy=g1
    assert cfg.strategy_params["momentum_confirm_bars"] == 2


def test_load_config_ig_strategy_specific_params_env(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "ig")
    monkeypatch.setenv("IG_IDENTIFIER", "ig-user")
    monkeypatch.setenv("IG_PASSWORD", "ig-pass")
    monkeypatch.setenv("IG_API_KEY", "ig-key")
    monkeypatch.setenv("IG_STRATEGY", "index_hybrid")
    monkeypatch.setenv(
        "IG_STRATEGY_PARAMS_INDEX_HYBRID",
        '{"index_fast_ema_window":13,"index_session_filter_enabled":false}',
    )

    cfg = load_config()
    assert cfg.strategy == "index_hybrid"
    assert cfg.strategy_params["index_fast_ema_window"] == 13
    assert cfg.strategy_params["index_session_filter_enabled"] is False


def test_load_config_rejects_invalid_strategy_preset_payload(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "xtb")
    monkeypatch.setenv("XTB_USER_ID", "xtb-user")
    monkeypatch.setenv("XTB_PASSWORD", "xtb-pass")
    monkeypatch.setenv("XTB_STRATEGY", "momentum")
    monkeypatch.setenv("XTB_STRATEGY_PRESETS", '{"momentum":1}')

    with pytest.raises(ConfigError, match="XTB_STRATEGY_PRESETS.momentum"):
        load_config()


def test_bot_picks_ig_client_when_broker_is_ig(tmp_path):
    config = BotConfig(
        user_id="ig-user",
        password="ig-pass",
        app_name="ig-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "ig-bot.db",
        risk=RiskConfig(),
        broker="ig",
        api_key="ig-key",
        ig_stream_enabled=False,
        symbol_epics={"EURUSD": "CS.D.EURUSD.CFD.IP"},
    )
    bot = TradingBot(config)
    try:
        assert isinstance(bot.broker, IgApiClient)
        assert bot.broker._stream_enabled is False  # type: ignore[attr-defined]
        assert bot.broker.stream_tick_max_age_sec == pytest.approx(config.ig_stream_tick_max_age_sec)
        assert bot.broker.rest_market_min_interval_sec == pytest.approx(config.ig_rest_market_min_interval_sec)
    finally:
        bot.store.close()


def test_bot_uses_xtb_client_when_broker_is_xtb(tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "xtb-bot.db",
        risk=RiskConfig(),
        broker="xtb",
    )
    bot = TradingBot(config)
    try:
        assert isinstance(bot.broker, XtbApiClient)
    finally:
        bot.store.close()


def test_connect_broker_fallbacks_to_mock_when_not_strict(tmp_path):
    config = BotConfig(
        user_id="ig-user",
        password="ig-pass",
        app_name="ig-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "ig-bot.db",
        risk=RiskConfig(),
        broker="ig",
        api_key="ig-key",
        strict_broker_connect=False,
    )
    bot = TradingBot(config)
    try:
        bot.broker.connect = lambda: (_ for _ in ()).throw(RuntimeError("connect failed"))  # type: ignore[assignment]
        bot._connect_broker()
        assert isinstance(bot.broker, MockBrokerClient)
    finally:
        bot.store.close()


def test_connect_broker_raises_when_strict_enabled(tmp_path):
    config = BotConfig(
        user_id="ig-user",
        password="ig-pass",
        app_name="ig-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "ig-bot.db",
        risk=RiskConfig(),
        broker="ig",
        api_key="ig-key",
        strict_broker_connect=True,
    )
    bot = TradingBot(config)
    try:
        bot.broker.connect = lambda: (_ for _ in ()).throw(RuntimeError("connect failed"))  # type: ignore[assignment]
        with pytest.raises(RuntimeError, match="connect failed"):
            bot._connect_broker()
    finally:
        bot.store.close()


def test_index_hybrid_skips_non_index_symbols_on_worker_start(tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["EURUSD", "US100", "WTI"],
        strategy="index_hybrid",
        strategy_params={},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot.db",
        risk=RiskConfig(),
        broker="xtb",
    )
    bot = TradingBot(config)

    started_symbols: list[str] = []

    class _DummyWorker:
        def __init__(self, symbol: str):
            self.symbol = symbol

        def start(self) -> None:
            started_symbols.append(self.symbol)

        def is_alive(self) -> bool:
            return False

    bot._make_worker = lambda assignment, stop_event: _DummyWorker(assignment.symbol)  # type: ignore[method-assign]
    bot._start_workers()

    try:
        assert started_symbols == ["US100"]
        events = bot.store.load_events(limit=20)
        skipped = [
            event
            for event in events
            if event.get("message") == "Symbol skipped by strategy filter"
        ]
        assert any(event.get("symbol") == "EURUSD" for event in skipped)
        assert any(event.get("symbol") == "WTI" for event in skipped)
    finally:
        bot.store.close()


def test_crypto_trend_following_skips_non_crypto_symbols_on_worker_start(tmp_path):
    config = BotConfig(
        user_id="1",
        password="1",
        app_name="xtb-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.PAPER,
        symbols=["BTC", "ETH", "US100", "EURUSD"],
        strategy="crypto_trend_following",
        strategy_params={},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "bot.db",
        risk=RiskConfig(),
        broker="xtb",
    )
    bot = TradingBot(config)

    started_symbols: list[str] = []

    class _DummyWorker:
        def __init__(self, symbol: str):
            self.symbol = symbol

        def start(self) -> None:
            started_symbols.append(self.symbol)

        def is_alive(self) -> bool:
            return False

    bot._make_worker = lambda assignment, stop_event: _DummyWorker(assignment.symbol)  # type: ignore[method-assign]
    bot._start_workers()

    try:
        assert started_symbols == ["BTC", "ETH"]
        events = bot.store.load_events(limit=20)
        skipped = [
            event
            for event in events
            if event.get("message") == "Symbol skipped by strategy filter"
        ]
        assert any(event.get("symbol") == "US100" for event in skipped)
        assert any(event.get("symbol") == "EURUSD" for event in skipped)
    finally:
        bot.store.close()
