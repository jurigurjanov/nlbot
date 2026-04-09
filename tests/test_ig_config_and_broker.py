from __future__ import annotations

import pytest

from xtb_bot.bot import TradingBot
from xtb_bot.client import MockBrokerClient, XtbApiClient
from xtb_bot.config import (
    DEFAULT_STRATEGY_PARAMS,
    BotConfig,
    ConfigError,
    RiskConfig,
    compact_strategy_params,
    load_config,
)
from xtb_bot.ig_client import IgApiClient
from xtb_bot.ig_proxy import RateLimitedBrokerProxy
from xtb_bot.models import AccountType, RunMode
from xtb_bot.symbols import is_commodity_symbol, is_index_symbol, resolve_index_market


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
        "XTB_IG_PROXY_ENABLED",
        "XTB_IG_PROXY_POLLERS_ENABLED",
        "IG_SYMBOL_EPICS",
        "IG_SYMBOLS",
        "IG_SYMBOLS_MOMENTUM",
        "IG_SYMBOLS_MOMENTUM_INDEX",
        "IG_SYMBOLS_MOMENTUM_FX",
        "IG_SYMBOLS_G1",
        "IG_SYMBOLS_G2",
        "IG_SYMBOLS_OIL",
        "IG_SYMBOLS_INDEX_HYBRID",
        "IG_SYMBOLS_CRYPTO_TREND_FOLLOWING",
        "XTB_SYMBOLS",
        "XTB_SYMBOLS_MOMENTUM",
        "XTB_SYMBOLS_MOMENTUM_INDEX",
        "XTB_SYMBOLS_MOMENTUM_FX",
        "XTB_SYMBOLS_G1",
        "XTB_SYMBOLS_G2",
        "XTB_SYMBOLS_OIL",
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
        "IG_STRATEGY_PARAMS_G2",
        "IG_STRATEGY_PARAMS_OIL",
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
        "XTB_STRATEGY_PARAMS_G2",
        "XTB_STRATEGY_PARAMS_OIL",
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


def test_compact_strategy_params_selects_primary_live_knobs_for_mean_reversion_bb():
    params = {
        "mean_reversion_bb_window": 20,
        "mean_reversion_bb_std_dev": 2.2,
        "mean_reversion_bb_entry_mode": "reentry",
        "mean_reversion_bb_volume_confirmation": True,
        "mean_reversion_bb_execution_min_confidence_for_entry": 0.75,
        "mean_reversion_bb_risk_reward_ratio": 2.2,
        "mean_reversion_bb_debug_unused": 123,
    }

    compact = compact_strategy_params("mean_reversion_bb", params)

    assert compact == {
        "mean_reversion_bb_window": 20,
        "mean_reversion_bb_std_dev": 2.2,
        "mean_reversion_bb_entry_mode": "reentry",
        "mean_reversion_bb_volume_confirmation": True,
        "mean_reversion_bb_execution_min_confidence_for_entry": 0.75,
        "mean_reversion_bb_risk_reward_ratio": 2.2,
    }


def test_compact_strategy_params_uses_alias_family_for_momentum_fx():
    params = {
        "fast_window": 8,
        "slow_window": 21,
        "momentum_entry_mode": "cross_or_trend",
        "momentum_volume_confirmation": True,
        "momentum_execution_min_confidence_for_entry": 0.68,
        "irrelevant_key": "ignore",
    }

    compact = compact_strategy_params("momentum_fx", params)

    assert compact == {
        "fast_window": 8,
        "slow_window": 21,
        "momentum_entry_mode": "cross_or_trend",
        "momentum_volume_confirmation": True,
        "momentum_execution_min_confidence_for_entry": 0.68,
    }


def test_compact_strategy_params_keeps_index_hybrid_volatility_trigger_visible():
    params = {
        "index_parameter_surface_mode": "tiered",
        "index_parameter_surface_tier": "conservative",
        "index_fast_ema_window": 21,
        "index_trend_volatility_explosion_min_ratio": 1.2,
        "index_mean_reversion_confidence_base": 0.10,
        "index_hybrid_execution_min_confidence_for_entry": 0.70,
    }

    compact = compact_strategy_params("index_hybrid", params)

    assert compact["index_trend_volatility_explosion_min_ratio"] == pytest.approx(1.2)
    assert compact["index_mean_reversion_confidence_base"] == pytest.approx(0.10)


def test_default_fast_fail_loss_floor_is_raised_for_ig_noise():
    assert DEFAULT_STRATEGY_PARAMS["protective_fast_fail_loss_ratio"] == pytest.approx(0.45)
    assert DEFAULT_STRATEGY_PARAMS["protective_fast_fail_loss_ratio_by_symbol"]["DE40"] == pytest.approx(0.45)
    assert DEFAULT_STRATEGY_PARAMS["protective_fast_fail_loss_ratio_by_symbol"]["US100"] == pytest.approx(0.45)
    assert DEFAULT_STRATEGY_PARAMS["protective_fast_fail_loss_ratio_by_symbol"]["NK20"] == pytest.approx(0.45)


def test_default_kama_and_index_trigger_thresholds_are_relaxed_for_entry_timing():
    assert DEFAULT_STRATEGY_PARAMS["momentum_kama_min_efficiency_ratio"] == pytest.approx(0.12)
    assert DEFAULT_STRATEGY_PARAMS["g1_kama_min_efficiency_ratio"] == pytest.approx(0.12)
    assert DEFAULT_STRATEGY_PARAMS["index_trend_volatility_explosion_min_ratio"] == pytest.approx(1.2)
    assert DEFAULT_STRATEGY_PARAMS["index_mean_reversion_confidence_base"] == pytest.approx(0.10)


def test_default_profit_unlocking_thresholds_are_relaxed_for_ig_execution():
    assert DEFAULT_STRATEGY_PARAMS["trailing_breakeven_offset_pips"] == pytest.approx(2.5)
    assert DEFAULT_STRATEGY_PARAMS["momentum_regime_min_adx"] == pytest.approx(21.0)
    assert DEFAULT_STRATEGY_PARAMS["mean_reversion_bb_std_dev"] == pytest.approx(2.2)
    assert DEFAULT_STRATEGY_PARAMS["mean_reversion_bb_trade_cooldown_loss_sec"] == pytest.approx(600.0)
    assert DEFAULT_STRATEGY_PARAMS["g1_use_incomplete_candle_for_entry"] is True


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


def test_load_config_does_not_use_opposite_broker_strict_connect_env(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "ig")
    monkeypatch.setenv("IG_IDENTIFIER", "ig-user")
    monkeypatch.setenv("IG_PASSWORD", "ig-pass")
    monkeypatch.setenv("IG_API_KEY", "ig-key")
    monkeypatch.setenv("XTB_STRICT_BROKER_CONNECT", "true")

    cfg = load_config()

    assert cfg.strict_broker_connect is False


def test_load_config_xtb_does_not_use_ig_strict_connect_env(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "xtb")
    monkeypatch.setenv("XTB_USER_ID", "xtb-user")
    monkeypatch.setenv("XTB_PASSWORD", "xtb-pass")
    monkeypatch.setenv("IG_STRICT_BROKER_CONNECT", "true")

    cfg = load_config()

    assert cfg.strict_broker_connect is False


def test_load_config_symbol_epics_invalid_json_raises_config_error(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "ig")
    monkeypatch.setenv("IG_IDENTIFIER", "ig-user")
    monkeypatch.setenv("IG_PASSWORD", "ig-pass")
    monkeypatch.setenv("IG_API_KEY", "ig-key")
    monkeypatch.setenv("IG_SYMBOL_EPICS", '{"EURUSD":')

    with pytest.raises(ConfigError, match="symbol_epics must be a JSON object"):
        load_config()


def test_ig_client_has_default_epic_candidates_for_jpn225():
    client = IgApiClient(
        identifier="ig-user",
        password="ig-pass",
        api_key="ig-key",
        account_type=AccountType.DEMO,
    )
    attempts = client._epic_attempt_order("JPN225")
    assert attempts
    assert any(epic.startswith("IX.D.") for epic in attempts)
    assert any("NIKKEI" in epic or "JP225" in epic or "JPN225" in epic for epic in attempts)


def test_us2000_is_treated_as_index_symbol():
    assert is_index_symbol("US2000") is True


def test_topix_is_treated_as_index_symbol():
    assert is_index_symbol("TOPIX") is True


def test_nk20_is_treated_as_index_symbol():
    assert is_index_symbol("NK20") is True


def test_nk_prefix_is_treated_as_europe_index_market():
    assert resolve_index_market("NK25") == "europe"


@pytest.mark.parametrize(
    "symbol",
    ["CARBON", "UKNATGAS", "GASOLINE", "WHEAT", "COCOA", "CORN", "SOYBEANOIL", "OATS"],
)
def test_new_commodity_symbols_are_treated_as_commodities(symbol):
    assert is_commodity_symbol(symbol) is True


@pytest.mark.parametrize("symbol", ["XAUJPY", "xageur"])
def test_precious_metal_prefixed_symbols_are_treated_as_commodities(symbol):
    assert is_commodity_symbol(symbol) is True


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


def test_load_config_xtb_rejects_disabled_oil_strategy(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "xtb")
    monkeypatch.setenv("XTB_USER_ID", "xtb-user")
    monkeypatch.setenv("XTB_PASSWORD", "xtb-pass")
    monkeypatch.setenv("XTB_STRATEGY", "oil")
    monkeypatch.setenv("XTB_SYMBOLS", "EURUSD,WTI")
    monkeypatch.setenv("XTB_SYMBOLS_OIL", "WTI,BRENT")
    monkeypatch.setenv("XTB_STRATEGY_PARAMS_OIL", '{"g1_execution_min_confidence_for_entry":0.72}')

    with pytest.raises(ConfigError, match="Unknown strategy: oil|strategy must be one of"):
        load_config()


def test_load_config_xtb_uses_strategy_scoped_symbols_for_g2(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "xtb")
    monkeypatch.setenv("XTB_USER_ID", "xtb-user")
    monkeypatch.setenv("XTB_PASSWORD", "xtb-pass")
    monkeypatch.setenv("XTB_STRATEGY", "g2")
    monkeypatch.setenv("XTB_SYMBOLS", "EURUSD,WTI,US100")
    monkeypatch.setenv("XTB_SYMBOLS_G2", "US100,DE40,TOPIX")
    monkeypatch.setenv("XTB_STRATEGY_PARAMS_G2", '{"g2_execution_min_confidence_for_entry":0.71}')

    cfg = load_config()
    assert cfg.strategy == "g2"
    assert cfg.symbols == ["US100", "DE40", "TOPIX"]
    assert cfg.strategy_params["g2_execution_min_confidence_for_entry"] == pytest.approx(0.71)


def test_load_config_g2_filters_generic_symbols_to_indices(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "ig")
    monkeypatch.setenv("IG_IDENTIFIER", "ig-user")
    monkeypatch.setenv("IG_PASSWORD", "ig-pass")
    monkeypatch.setenv("IG_API_KEY", "ig-key")
    monkeypatch.setenv("IG_STRATEGY", "g2")
    monkeypatch.setenv("IG_SYMBOLS", "EURUSD,WTI,US100,DE40")

    cfg = load_config()
    assert cfg.strategy == "g2"
    assert cfg.symbols == ["US100", "DE40"]


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
    assert cfg.symbols == ["US500", "US100", "US2000", "DE40", "UK100", "NK20", "JPN225", "AUS200"]


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
    assert cfg.strategy_params["momentum_confirm_bars"] == 1


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
    assert cfg.strategy_params["index_session_filter_enabled"] is True
    assert cfg.strategy_params["index_parameter_surface_mode"] == "tiered"
    assert cfg.strategy_params["index_parameter_surface_tier"] == "conservative"
    assert cfg.strategy_params["index_parameter_surface_locked"] is True
    assert "index_session_filter_enabled" in cfg.strategy_params["index_parameter_surface_ignored_overrides"]


def test_load_config_index_hybrid_open_surface_allows_locked_override(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "ig")
    monkeypatch.setenv("IG_IDENTIFIER", "ig-user")
    monkeypatch.setenv("IG_PASSWORD", "ig-pass")
    monkeypatch.setenv("IG_API_KEY", "ig-key")
    monkeypatch.setenv("IG_STRATEGY", "index_hybrid")
    monkeypatch.setenv(
        "IG_STRATEGY_PARAMS_INDEX_HYBRID",
        '{"index_parameter_surface_mode":"open","index_parameter_surface_tier":"aggressive","index_fast_ema_window":13,"index_session_filter_enabled":false}',
    )

    cfg = load_config()
    assert cfg.strategy == "index_hybrid"
    assert cfg.strategy_params["index_parameter_surface_mode"] == "open"
    assert cfg.strategy_params["index_parameter_surface_tier"] == "aggressive"
    assert cfg.strategy_params["index_parameter_surface_locked"] is False
    assert cfg.strategy_params["index_fast_ema_window"] == 13
    assert cfg.strategy_params["index_session_filter_enabled"] is False


def test_load_config_index_hybrid_tiered_surface_preserves_mode_overrides(monkeypatch):
    _clear_env(monkeypatch)
    monkeypatch.setenv("BROKER", "ig")
    monkeypatch.setenv("IG_IDENTIFIER", "ig-user")
    monkeypatch.setenv("IG_PASSWORD", "ig-pass")
    monkeypatch.setenv("IG_API_KEY", "ig-key")
    monkeypatch.setenv("IG_STRATEGY", "index_hybrid")
    monkeypatch.setenv(
        "IG_STRATEGY_PARAMS_INDEX_HYBRID",
        '{"index_zscore_mode":"classic","index_auto_correct_regime_thresholds":false,'
        '"index_enforce_gap_hysteresis":false,"index_gap_hysteresis_min":0.00025}',
    )

    cfg = load_config()
    assert cfg.strategy == "index_hybrid"
    assert cfg.strategy_params["index_parameter_surface_mode"] == "tiered"
    assert cfg.strategy_params["index_parameter_surface_locked"] is True
    assert cfg.strategy_params["index_zscore_mode"] == "classic"
    assert cfg.strategy_params["index_auto_correct_regime_thresholds"] is False
    assert cfg.strategy_params["index_enforce_gap_hysteresis"] is False
    assert cfg.strategy_params["index_gap_hysteresis_min"] == 0.00025


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


def test_bot_wraps_ig_client_with_proxy_when_enabled(tmp_path, monkeypatch):
    monkeypatch.setenv("XTB_IG_PROXY_ENABLED", "true")
    monkeypatch.setenv("XTB_IG_PROXY_POLLERS_ENABLED", "false")
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
        storage_path=tmp_path / "ig-proxy-bot.db",
        risk=RiskConfig(),
        broker="ig",
        api_key="ig-key",
        ig_stream_enabled=False,
        symbol_epics={"EURUSD": "CS.D.EURUSD.CFD.IP"},
    )
    bot = TradingBot(config)
    try:
        assert isinstance(bot.broker, RateLimitedBrokerProxy)
        assert isinstance(bot.broker.underlying, IgApiClient)
        assert bot.broker._stream_enabled is False  # type: ignore[attr-defined]
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


def test_connect_broker_never_fallbacks_to_mock_in_execution(tmp_path):
    config = BotConfig(
        user_id="ig-user",
        password="ig-pass",
        app_name="ig-bot",
        account_type=AccountType.DEMO,
        mode=RunMode.EXECUTION,
        symbols=["EURUSD"],
        strategy="momentum",
        strategy_params={"fast_window": 3, "slow_window": 5, "stop_loss_pips": 10, "take_profit_pips": 20},
        poll_interval_sec=1.0,
        default_volume=0.1,
        storage_path=tmp_path / "ig-execution-bot.db",
        risk=RiskConfig(),
        broker="ig",
        api_key="ig-key",
        strict_broker_connect=False,
    )
    bot = TradingBot(config)
    try:
        original_broker = bot.broker
        bot.broker.connect = lambda: (_ for _ in ()).throw(RuntimeError("connect failed"))  # type: ignore[assignment]
        with pytest.raises(RuntimeError, match="connect failed"):
            bot._connect_broker()
        assert bot.broker is original_broker
        assert isinstance(bot.broker, MockBrokerClient) is False
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
