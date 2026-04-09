from __future__ import annotations

from xtb_bot.tolerances import (
    FLOAT_COMPARISON_TOLERANCE,
    FLOAT_ROUNDING_TOLERANCE,
    float_gt,
    float_gte,
    float_is_close,
    float_is_zero,
    float_lt,
    float_lte,
)


def test_float_is_close_uses_named_default_tolerance():
    assert float_is_close(1.0, 1.0 + (FLOAT_COMPARISON_TOLERANCE * 0.5))
    assert not float_is_close(1.0, 1.0 + (FLOAT_COMPARISON_TOLERANCE * 10.0), rel_tol=0.0)


def test_float_is_close_defaults_to_zero_relative_tolerance():
    left = 1_000_000_000.0
    right = left + 0.5
    assert not float_is_close(left, right, abs_tol=0.0)
    assert float_is_close(left, right, rel_tol=1e-9, abs_tol=0.0)


def test_float_is_zero_uses_rounding_tolerance():
    assert float_is_zero(FLOAT_ROUNDING_TOLERANCE * 0.5)
    assert not float_is_zero(FLOAT_COMPARISON_TOLERANCE * 10.0)


def test_ordering_helpers_treat_close_values_as_equal():
    left = 10.0
    right = left + (FLOAT_COMPARISON_TOLERANCE * 0.5)
    assert not float_gt(right, left)
    assert float_gte(right, left)
    assert not float_lt(left, right)
    assert float_lte(left, right)


def test_ordering_helpers_preserve_strict_order_outside_tolerance():
    left = 10.0
    right = left + (FLOAT_COMPARISON_TOLERANCE * 10.0)
    assert float_gt(right, left)
    assert float_gte(right, left)
    assert float_lt(left, right)
    assert float_lte(left, right)
