from __future__ import annotations

from xtb_bot.strategy_profiles import apply_strategy_profile, enforce_strategy_parameter_surface


def test_apply_strategy_profile_preserves_user_overrides_for_non_index_strategies():
    merged, applied = apply_strategy_profile(
        "mean_reversion_bb",
        {
            "mean_reversion_bb_risk_reward_ratio": 2.4,
            "mean_reversion_bb_min_confidence_for_entry": 0.61,
            "_strategy_profile_preserve_keys": [
                "mean_reversion_bb_risk_reward_ratio",
                "mean_reversion_bb_min_confidence_for_entry",
            ],
        },
        "conservative",
    )

    assert applied is True
    assert merged["mean_reversion_bb_risk_reward_ratio"] == 2.4
    assert merged["mean_reversion_bb_min_confidence_for_entry"] == 0.61
    assert merged["mean_reversion_bb_take_profit_mode"] == "rr"


def test_apply_strategy_profile_keeps_index_hybrid_profile_authoritative_for_locked_keys():
    merged, applied = apply_strategy_profile(
        "index_hybrid",
        {
            "index_risk_reward_ratio": 9.9,
            "index_zscore_mode": "classic",
        },
        "conservative",
    )

    assert applied is True
    assert merged["index_risk_reward_ratio"] == 2.2
    assert merged["index_zscore_mode"] == "classic"
    assert merged["index_trend_volatility_explosion_min_ratio"] == 1.2


def test_apply_strategy_profile_relaxes_index_hybrid_volatility_trigger_across_tiers():
    conservative, applied_conservative = apply_strategy_profile("index_hybrid", {}, "conservative")
    aggressive, applied_aggressive = apply_strategy_profile("index_hybrid", {}, "aggressive")

    assert applied_conservative is True
    assert applied_aggressive is True
    assert conservative["index_trend_volatility_explosion_min_ratio"] == 1.2
    assert aggressive["index_trend_volatility_explosion_min_ratio"] == 1.2


def test_enforce_strategy_parameter_surface_preserves_index_hybrid_mode_overrides():
    merged, applied = enforce_strategy_parameter_surface(
        "index_hybrid",
        {
            "index_parameter_surface_mode": "tiered",
            "index_parameter_surface_tier": "conservative",
            "index_zscore_mode": "classic",
            "index_auto_correct_regime_thresholds": False,
            "index_enforce_gap_hysteresis": False,
            "index_gap_hysteresis_min": 0.00025,
            "index_window_sync_mode": "warn",
            "index_zscore_ema_window": 77,
            "index_mean_reversion_confidence_base": 0.14,
        },
    )

    assert applied is True
    assert merged["index_parameter_surface_locked"] is True
    assert merged["index_zscore_mode"] == "classic"
    assert merged["index_auto_correct_regime_thresholds"] is False
    assert merged["index_enforce_gap_hysteresis"] is False
    assert merged["index_gap_hysteresis_min"] == 0.00025
    assert merged["index_window_sync_mode"] == "warn"
    assert merged["index_zscore_ema_window"] == 77
    assert merged["index_mean_reversion_confidence_base"] == 0.14


def test_enforce_strategy_parameter_surface_drops_locked_index_hybrid_owned_keys_and_stale_metadata():
    merged, applied = enforce_strategy_parameter_surface(
        "index_hybrid",
        {
            "index_parameter_surface_mode": "tiered",
            "index_parameter_surface_tier": "conservative",
            "index_require_context_tick_size": True,
            "index_max_spread_pips_by_symbol": {"NK20": 9.0},
            "index_parameter_surface_ignored_overrides": ["stale"],
            "index_parameter_surface_ignored_count": 99,
        },
    )

    assert applied is True
    assert "index_require_context_tick_size" not in merged
    assert "index_max_spread_pips_by_symbol" not in merged
    assert merged["index_parameter_surface_ignored_overrides"] == []
    assert merged["index_parameter_surface_ignored_count"] == 0


def test_enforce_strategy_parameter_surface_locks_donchian_breakout_to_live_core():
    merged, applied = enforce_strategy_parameter_surface(
        "donchian_breakout",
        {
            "donchian_breakout_window": 30,
            "donchian_breakout_risk_reward_ratio": 2.6,
            "donchian_breakout_session_filter_enabled": True,
            "donchian_breakout_rsi_filter_enabled": True,
            "donchian_breakout_regime_filter_enabled": True,
        },
    )

    assert applied is True
    assert merged["donchian_breakout_parameter_surface_locked"] is True
    assert merged["donchian_breakout_window"] == 30
    assert merged["donchian_breakout_risk_reward_ratio"] == 2.6
    assert "donchian_breakout_session_filter_enabled" not in merged
    assert "donchian_breakout_rsi_filter_enabled" not in merged
    assert "donchian_breakout_regime_filter_enabled" not in merged


def test_enforce_strategy_parameter_surface_resets_donchian_breakout_stale_metadata():
    merged, applied = enforce_strategy_parameter_surface(
        "donchian_breakout",
        {
            "donchian_breakout_window": 30,
            "donchian_breakout_parameter_surface_allowed_overrides": ["stale"],
            "donchian_breakout_parameter_surface_ignored_overrides": ["stale"],
            "donchian_breakout_parameter_surface_ignored_count": 99,
        },
    )

    assert applied is True
    assert merged["donchian_breakout_parameter_surface_locked"] is True
    assert merged["donchian_breakout_parameter_surface_ignored_overrides"] == []
    assert merged["donchian_breakout_parameter_surface_ignored_count"] == 0
