from __future__ import annotations

import pytest

from xtb_bot.config import resolve_strategy_param
from xtb_bot.models import Side
from xtb_bot.strategies import available_strategies, create_strategy
from xtb_bot.strategies.base import Strategy, StrategyContext


def test_available_strategies_contains_momentum_aliases():
    names = available_strategies()
    assert "momentum" in names
    assert "multi_strategy" in names
    assert "momentum_index" in names
    assert "momentum_fx" in names
    assert "crypto_trend_following" in names
    assert "oil" not in names
    assert "g2" in names


def test_create_strategy_accepts_momentum_aliases():
    for alias in ("momentum_index", "momentum_fx"):
        strategy = create_strategy(alias, {"fast_window": 3, "slow_window": 5})
        assert strategy.name == "momentum"


def test_create_strategy_rejects_disabled_oil_strategy():
    with pytest.raises(ValueError, match="Unknown strategy: oil"):
        create_strategy("oil", {})


def test_create_strategy_supports_multi_strategy_carrier():
    strategy = create_strategy("multi_strategy", {"stop_loss_pips": 10, "take_profit_pips": 20})
    assert strategy.name == "multi_strategy"


def test_multi_strategy_carrier_signal_is_explicit_runtime_only_hold():
    strategy = create_strategy(
        "multi_strategy",
        {
            "stop_loss_pips": 10,
            "take_profit_pips": 20,
            "_multi_strategy_base_component": "momentum",
            "multi_strategy_names": "momentum,g1",
        },
    )

    signal = strategy.generate_signal(
        StrategyContext(symbol="EURUSD", prices=[1.1, 1.2]),
    )

    assert signal.side == Side.HOLD
    assert signal.metadata["reason"] == "carrier_runtime_only"
    assert signal.metadata["carrier_only"] is True
    assert signal.metadata["base_component"] == "momentum"
    assert signal.metadata["components"] == ["momentum", "g1"]


def test_base_hold_prefers_strategy_computed_risk_distances():
    strategy = create_strategy(
        "momentum",
        {
            "fast_window": 3,
            "slow_window": 5,
            "stop_loss_pips": 10.0,
            "take_profit_pips": 5.0,
            "momentum_risk_reward_ratio": 3.0,
        },
    )

    signal = strategy.hold()

    assert signal.stop_loss_pips == pytest.approx(strategy.stop_loss_pips)  # type: ignore[attr-defined]
    assert signal.take_profit_pips == pytest.approx(strategy.take_profit_pips)  # type: ignore[attr-defined]
    assert signal.take_profit_pips == pytest.approx(30.0)


def test_base_extract_finite_prices_rejects_non_positive_values():
    cleaned, invalid = Strategy._extract_finite_prices([1.0, 0.0, -1.0, float("nan"), "bad", 2.5])

    assert cleaned == pytest.approx([1.0, 2.5])
    assert invalid == 4


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


def test_resolve_strategy_param_does_not_fallback_from_disabled_oil_alias():
    params = {
        "g1_execution_min_confidence_for_entry": 0.68,
    }
    value = resolve_strategy_param(
        params,
        "oil",
        "min_confidence_for_entry",
        0.0,
        mode="execution",
    )
    assert value == 0.0


def test_create_strategy_accepts_g2():
    strategy = create_strategy("g2", {})
    assert strategy.name == "g2"
