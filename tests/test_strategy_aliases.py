from __future__ import annotations

from xtb_bot.config import resolve_strategy_param
from xtb_bot.strategies import available_strategies, create_strategy


def test_available_strategies_contains_momentum_aliases():
    names = available_strategies()
    assert "momentum" in names
    assert "momentum_index" in names
    assert "momentum_fx" in names
    assert "crypto_trend_following" in names


def test_create_strategy_accepts_momentum_aliases():
    for alias in ("momentum_index", "momentum_fx"):
        strategy = create_strategy(alias, {"fast_window": 3, "slow_window": 5})
        assert strategy.name == "momentum"


def test_resolve_strategy_param_uses_momentum_alias_fallback_with_mode():
    params = {
        "momentum_execution_min_confidence_for_entry": 0.65,
    }
    value = resolve_strategy_param(
        params,
        "momentum_index",
        "min_confidence_for_entry",
        0.0,
        mode="execution",
    )
    assert value == 0.65


def test_resolve_strategy_param_prefers_alias_specific_value_over_base_alias():
    params = {
        "momentum_execution_min_confidence_for_entry": 0.65,
        "momentum_index_execution_min_confidence_for_entry": 0.78,
    }
    value = resolve_strategy_param(
        params,
        "momentum_index",
        "min_confidence_for_entry",
        0.0,
        mode="execution",
    )
    assert value == 0.78


def test_resolve_strategy_param_uses_crypto_trend_alias_fallback_with_mode():
    params = {
        "trend_following_execution_min_confidence_for_entry": 0.74,
    }
    value = resolve_strategy_param(
        params,
        "crypto_trend_following",
        "min_confidence_for_entry",
        0.0,
        mode="execution",
    )
    assert value == 0.74


def test_resolve_strategy_param_prefers_crypto_strategy_specific_value_over_trend_alias():
    params = {
        "trend_following_execution_min_confidence_for_entry": 0.74,
        "crypto_trend_following_execution_min_confidence_for_entry": 0.81,
    }
    value = resolve_strategy_param(
        params,
        "crypto_trend_following",
        "min_confidence_for_entry",
        0.0,
        mode="execution",
    )
    assert value == 0.81
