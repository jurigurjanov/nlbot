from __future__ import annotations

import math


FLOAT_COMPARISON_TOLERANCE = 1e-9
FLOAT_ROUNDING_TOLERANCE = 1e-12
DEFAULT_RELATIVE_TOLERANCE = 0.0


def float_is_close(
    left: float,
    right: float,
    *,
    rel_tol: float = DEFAULT_RELATIVE_TOLERANCE,
    abs_tol: float = FLOAT_COMPARISON_TOLERANCE,
) -> bool:
    return math.isclose(
        left,
        right,
        rel_tol=rel_tol,
        abs_tol=abs_tol,
    )


def float_is_zero(
    value: float,
    *,
    abs_tol: float = FLOAT_ROUNDING_TOLERANCE,
) -> bool:
    return float_is_close(value, 0.0, rel_tol=0.0, abs_tol=abs_tol)


def float_gt(
    left: float,
    right: float,
    *,
    rel_tol: float = 0.0,
    abs_tol: float = FLOAT_COMPARISON_TOLERANCE,
) -> bool:
    return left > right and not float_is_close(
        left,
        right,
        rel_tol=rel_tol,
        abs_tol=abs_tol,
    )


def float_gte(
    left: float,
    right: float,
    *,
    rel_tol: float = 0.0,
    abs_tol: float = FLOAT_COMPARISON_TOLERANCE,
) -> bool:
    return left > right or float_is_close(
        left,
        right,
        rel_tol=rel_tol,
        abs_tol=abs_tol,
    )


def float_lt(
    left: float,
    right: float,
    *,
    rel_tol: float = 0.0,
    abs_tol: float = FLOAT_COMPARISON_TOLERANCE,
) -> bool:
    return left < right and not float_is_close(
        left,
        right,
        rel_tol=rel_tol,
        abs_tol=abs_tol,
    )


def float_lte(
    left: float,
    right: float,
    *,
    rel_tol: float = 0.0,
    abs_tol: float = FLOAT_COMPARISON_TOLERANCE,
) -> bool:
    return left < right or float_is_close(
        left,
        right,
        rel_tol=rel_tol,
        abs_tol=abs_tol,
    )
