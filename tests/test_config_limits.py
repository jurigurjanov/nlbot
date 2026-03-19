from __future__ import annotations

import pytest

from xtb_bot.config import ConfigError, load_config


def _set_required_env(monkeypatch):
    monkeypatch.setenv("BROKER", "xtb")
    monkeypatch.delenv("IG_BROKER", raising=False)
    monkeypatch.delenv("IG_IDENTIFIER", raising=False)
    monkeypatch.delenv("IG_PASSWORD", raising=False)
    monkeypatch.delenv("IG_API_KEY", raising=False)
    monkeypatch.setenv("XTB_USER_ID", "1")
    monkeypatch.setenv("XTB_PASSWORD", "1")
    monkeypatch.delenv("XTB_DEFAULT_VOLUME", raising=False)
    monkeypatch.delenv("XTB_STREAM_TICK_MAX_AGE_SEC", raising=False)
    monkeypatch.delenv("XTB_REST_MARKET_MIN_INTERVAL_SEC", raising=False)
    monkeypatch.delenv("XTB_WORKER_POLL_JITTER_SEC", raising=False)
    monkeypatch.delenv("XTB_PASSIVE_HISTORY_POLL_INTERVAL_SEC", raising=False)
    monkeypatch.delenv("XTB_PRICE_HISTORY_KEEP_ROWS_MIN", raising=False)


def test_config_rejects_more_than_fifteen_open_positions(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_MAX_OPEN_POSITIONS", "16")

    with pytest.raises(ConfigError, match="max_open_positions"):
        load_config()


def test_config_accepts_fifteen_open_positions(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_MAX_OPEN_POSITIONS", "15")

    config = load_config()
    assert config.risk.max_open_positions == 15


def test_config_rejects_non_positive_open_slot_lease_sec(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_OPEN_SLOT_LEASE_SEC", "0")

    with pytest.raises(ConfigError, match="open_slot_lease_sec"):
        load_config()


def test_config_accepts_positive_open_slot_lease_sec(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_OPEN_SLOT_LEASE_SEC", "45")

    config = load_config()
    assert config.risk.open_slot_lease_sec == 45.0


def test_config_rejects_tp_sl_ratio_below_two(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_MIN_TP_SL_RATIO", "1.9")

    with pytest.raises(ConfigError, match="min_tp_sl_ratio"):
        load_config()


def test_config_accepts_tp_sl_ratio_three(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_MIN_TP_SL_RATIO", "3.0")

    config = load_config()
    assert config.risk.min_tp_sl_ratio == 3.0


def test_config_rejects_invalid_trailing_activation_ratio(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_TRAILING_ACTIVATION_RATIO", "1.1")

    with pytest.raises(ConfigError, match="trailing_activation_ratio"):
        load_config()


def test_config_accepts_zero_trailing_activation_ratio(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_TRAILING_ACTIVATION_RATIO", "0")

    config = load_config()
    assert config.risk.trailing_activation_ratio == 0.0


def test_config_rejects_negative_trailing_activation_ratio(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_TRAILING_ACTIVATION_RATIO", "-0.1")

    with pytest.raises(ConfigError, match="trailing_activation_ratio"):
        load_config()


def test_config_rejects_non_positive_trailing_distance(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_TRAILING_DISTANCE_PIPS", "0")

    with pytest.raises(ConfigError, match="trailing_distance_pips"):
        load_config()


def test_config_accepts_zero_trailing_breakeven_offset(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_TRAILING_BREAKEVEN_OFFSET_PIPS", "0")

    config = load_config()
    assert config.risk.trailing_breakeven_offset_pips == 0.0


def test_config_rejects_negative_trailing_breakeven_offset(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_TRAILING_BREAKEVEN_OFFSET_PIPS", "-0.1")

    with pytest.raises(ConfigError, match="trailing_breakeven_offset_pips"):
        load_config()


def test_config_uses_legacy_breakeven_offset_as_fallback_for_profiles(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_TRAILING_BREAKEVEN_OFFSET_PIPS", "2")
    monkeypatch.delenv("XTB_TRAILING_BREAKEVEN_OFFSET_PIPS_FX", raising=False)
    monkeypatch.delenv("XTB_TRAILING_BREAKEVEN_OFFSET_PIPS_INDEX", raising=False)
    monkeypatch.delenv("XTB_TRAILING_BREAKEVEN_OFFSET_PIPS_COMMODITY", raising=False)

    config = load_config()
    assert config.risk.trailing_breakeven_offset_pips == 2.0
    assert config.risk.trailing_breakeven_offset_pips_fx == 2.0
    assert config.risk.trailing_breakeven_offset_pips_index == 2.0
    assert config.risk.trailing_breakeven_offset_pips_commodity == 2.0


def test_config_asset_specific_breakeven_offsets_override_legacy(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_TRAILING_BREAKEVEN_OFFSET_PIPS", "2")
    monkeypatch.setenv("XTB_TRAILING_BREAKEVEN_OFFSET_PIPS_INDEX", "8")

    config = load_config()
    assert config.risk.trailing_breakeven_offset_pips == 2.0
    assert config.risk.trailing_breakeven_offset_pips_fx == 2.0
    assert config.risk.trailing_breakeven_offset_pips_index == 8.0
    assert config.risk.trailing_breakeven_offset_pips_commodity == 2.0


def test_config_rejects_negative_asset_specific_breakeven_offset(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_TRAILING_BREAKEVEN_OFFSET_PIPS_INDEX", "-1")

    with pytest.raises(ConfigError, match="trailing_breakeven_offset_pips_index"):
        load_config()


def test_config_rejects_invalid_news_action(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_NEWS_EVENT_ACTION", "hold")

    with pytest.raises(ConfigError, match="news_event_action"):
        load_config()


def test_config_rejects_negative_news_event_buffer(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_NEWS_EVENT_BUFFER_MIN", "-1")

    with pytest.raises(ConfigError, match="news_event_buffer_min"):
        load_config()


def test_config_rejects_non_positive_spread_multiplier(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_SPREAD_ANOMALY_MULTIPLIER", "1.0")

    with pytest.raises(ConfigError, match="spread_anomaly_multiplier"):
        load_config()


def test_config_rejects_spread_min_samples_above_window(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_SPREAD_AVG_WINDOW", "10")
    monkeypatch.setenv("XTB_SPREAD_MIN_SAMPLES", "11")

    with pytest.raises(ConfigError, match="spread_min_samples"):
        load_config()


def test_config_rejects_non_positive_connectivity_latency(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_CONNECTIVITY_MAX_LATENCY_MS", "0")

    with pytest.raises(ConfigError, match="connectivity_max_latency_ms"):
        load_config()


def test_config_rejects_margin_safety_buffer_below_one(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_MARGIN_SAFETY_BUFFER", "0.99")

    with pytest.raises(ConfigError, match="margin_safety_buffer"):
        load_config()


def test_config_rejects_non_positive_margin_fallback_leverage(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_MARGIN_FALLBACK_LEVERAGE", "0")

    with pytest.raises(ConfigError, match="margin_fallback_leverage"):
        load_config()


def test_config_rejects_margin_weekend_multiplier_below_one(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_MARGIN_WEEKEND_MULTIPLIER", "0.95")

    with pytest.raises(ConfigError, match="margin_weekend_multiplier"):
        load_config()


def test_config_rejects_margin_holiday_multiplier_below_one(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_MARGIN_HOLIDAY_MULTIPLIER", "0.90")

    with pytest.raises(ConfigError, match="margin_holiday_multiplier"):
        load_config()


def test_config_rejects_invalid_margin_holiday_dates(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_MARGIN_HOLIDAY_DATES_UTC", "2026-12-24,2026-99-01")

    with pytest.raises(ConfigError, match="margin_holiday_dates_utc"):
        load_config()


def test_config_parses_extended_margin_fields(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_MARGIN_OVERHEAD_PCT", "2.5")
    monkeypatch.setenv("XTB_MARGIN_WEEKEND_MULTIPLIER", "1.25")
    monkeypatch.setenv("XTB_MARGIN_WEEKEND_START_HOUR_UTC", "19")
    monkeypatch.setenv("XTB_MARGIN_HOLIDAY_MULTIPLIER", "1.15")
    monkeypatch.setenv("XTB_MARGIN_HOLIDAY_DATES_UTC", "2026-12-24,2026-12-25")
    monkeypatch.setenv("XTB_MARGIN_COMMISSION_PER_LOT", "4.0")
    monkeypatch.setenv("XTB_MARGIN_MIN_FREE_AFTER_OPEN", "150")
    monkeypatch.setenv("XTB_MARGIN_MIN_FREE_AFTER_OPEN_PCT", "3.0")
    monkeypatch.setenv("XTB_EXTERNAL_CASHFLOW_REBASE_ENABLED", "true")
    monkeypatch.setenv("XTB_EXTERNAL_CASHFLOW_REBASE_MIN_ABS", "250")
    monkeypatch.setenv("XTB_EXTERNAL_CASHFLOW_REBASE_MIN_PCT", "5")

    config = load_config()

    assert config.risk.margin_overhead_pct == pytest.approx(2.5)
    assert config.risk.margin_weekend_multiplier == pytest.approx(1.25)
    assert config.risk.margin_weekend_start_hour_utc == 19
    assert config.risk.margin_holiday_multiplier == pytest.approx(1.15)
    assert config.risk.margin_holiday_dates_utc == ("2026-12-24", "2026-12-25")
    assert config.risk.margin_commission_per_lot == pytest.approx(4.0)
    assert config.risk.margin_min_free_after_open == pytest.approx(150.0)
    assert config.risk.margin_min_free_after_open_pct == pytest.approx(3.0)
    assert config.risk.external_cashflow_rebase_enabled is True
    assert config.risk.external_cashflow_rebase_min_abs == pytest.approx(250.0)
    assert config.risk.external_cashflow_rebase_min_pct == pytest.approx(5.0)


def test_config_rejects_non_positive_connectivity_pong_timeout(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_CONNECTIVITY_PONG_TIMEOUT_SEC", "0")

    with pytest.raises(ConfigError, match="connectivity_pong_timeout_sec"):
        load_config()


def test_config_rejects_non_positive_stream_max_tick_age(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_STREAM_MAX_TICK_AGE_SEC", "0")

    with pytest.raises(ConfigError, match="stream_max_tick_age_sec"):
        load_config()


def test_config_rejects_non_positive_stream_event_cooldown(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_STREAM_EVENT_COOLDOWN_SEC", "0")

    with pytest.raises(ConfigError, match="stream_event_cooldown_sec"):
        load_config()


def test_config_rejects_non_positive_hold_reason_log_interval(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_HOLD_REASON_LOG_INTERVAL_SEC", "0")

    with pytest.raises(ConfigError, match="hold_reason_log_interval_sec"):
        load_config()


def test_config_rejects_invalid_magic_prefix(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_BOT_MAGIC_PREFIX", "bad prefix")

    with pytest.raises(ConfigError, match="bot_magic_prefix"):
        load_config()


def test_config_accepts_valid_magic_prefix_and_instance(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_BOT_MAGIC_PREFIX", "XTBBOT01")
    monkeypatch.setenv("XTB_BOT_MAGIC_INSTANCE", "inst_001")

    config = load_config()
    assert config.bot_magic_prefix == "XTBBOT01"
    assert config.bot_magic_instance == "inst_001"


def test_config_rejects_json_config_file_path(monkeypatch, tmp_path):
    _set_required_env(monkeypatch)
    config_path = tmp_path / "config.json"
    config_path.write_text("{}", encoding="utf-8")

    with pytest.raises(ConfigError, match="no longer supported"):
        load_config(config_path=str(config_path))


def test_config_rejects_non_positive_epic_unavailable_disable_threshold(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_SYMBOL_AUTO_DISABLE_EPIC_UNAVAILABLE_THRESHOLD", "0")

    with pytest.raises(ConfigError, match="symbol_auto_disable_epic_unavailable_threshold"):
        load_config()


def test_config_rejects_non_positive_ig_stream_tick_max_age(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_STREAM_TICK_MAX_AGE_SEC", "0")

    with pytest.raises(ConfigError, match="ig_stream_tick_max_age_sec"):
        load_config()


def test_config_rejects_negative_ig_rest_market_min_interval(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_REST_MARKET_MIN_INTERVAL_SEC", "-0.1")

    with pytest.raises(ConfigError, match="ig_rest_market_min_interval_sec"):
        load_config()


def test_config_rejects_negative_worker_poll_jitter(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_WORKER_POLL_JITTER_SEC", "-1")

    with pytest.raises(ConfigError, match="worker_poll_jitter_sec"):
        load_config()


def test_config_rejects_non_positive_passive_history_poll_interval(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_PASSIVE_HISTORY_POLL_INTERVAL_SEC", "0")

    with pytest.raises(ConfigError, match="passive_history_poll_interval_sec"):
        load_config()


def test_config_rejects_too_small_price_history_keep_rows_min(monkeypatch):
    _set_required_env(monkeypatch)
    monkeypatch.setenv("XTB_PRICE_HISTORY_KEEP_ROWS_MIN", "99")

    with pytest.raises(ConfigError, match="price_history_keep_rows_min"):
        load_config()
