from __future__ import annotations

import pytest

from xtb_bot.numeric import atr_wilder, efficiency_ratio, kama_last_two, kama_slope_atr, rsi_sma, session_vwap_bands


def test_rsi_sma_uses_all_samples_without_period():
    prices = [100.0, 101.0, 100.5, 101.5, 101.0, 102.0]
    baseline = rsi_sma(prices)
    assert 0.0 <= baseline <= 100.0


def test_rsi_sma_respects_period_when_provided():
    prices = [100.0, 101.0, 100.5, 101.5, 101.0, 102.0, 101.8, 102.3]
    all_samples = rsi_sma(prices)
    short_period = rsi_sma(prices, period=3)
    expected_short = rsi_sma(prices[-4:])
    assert short_period == pytest.approx(expected_short)
    assert short_period != pytest.approx(all_samples)


def test_atr_wilder_close_to_close_proxy():
    closes = [100.0, 101.0, 99.0, 100.0]
    atr = atr_wilder(closes, window=2)
    assert atr == pytest.approx(1.25)


def test_atr_wilder_supports_true_range_from_hlc_samples():
    hlc = [
        (101.0, 99.0, 100.0),
        (103.0, 99.0, 101.0),
        (104.0, 96.0, 99.0),
        (100.0, 95.0, 100.0),
    ]
    atr = atr_wilder(hlc, window=2)
    assert atr == pytest.approx(5.5)


def test_efficiency_ratio_is_low_for_chop_and_high_for_trend():
    chop = [100.0, 101.0, 100.0, 101.0, 100.0, 101.0]
    trend = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
    assert efficiency_ratio(chop, 5) == pytest.approx(0.2)
    assert efficiency_ratio(trend, 5) == pytest.approx(1.0)


def test_kama_last_two_tracks_directional_series():
    previous, current = kama_last_two([100.0, 101.0, 102.0, 103.0, 104.0, 105.0], 4, fast_window=2, slow_window=10)
    assert previous is not None
    assert current is not None
    assert current > previous


def test_kama_slope_atr_returns_positive_normalized_slope():
    slope = kama_slope_atr([100.0, 101.0, 102.0, 103.0, 104.0, 105.0], 4, atr=2.0, fast_window=2, slow_window=10)
    assert slope > 0.0


def test_session_vwap_bands_returns_weighted_levels():
    payload = session_vwap_bands([100.0, 102.0, 101.0], [10.0, 20.0, 10.0], band_multipliers=(1.0, 2.0))
    assert payload is not None
    assert payload["vwap"] == pytest.approx(101.25)
    assert payload["valid_samples"] == pytest.approx(3.0)
    assert payload["sigma"] > 0.0
    assert payload["volume_quality"] == pytest.approx(1.0)
    assert payload["upper_1"] > payload["vwap"]
    assert payload["lower_2"] < payload["vwap"]


def test_session_vwap_bands_flags_poor_volume_quality():
    payload = session_vwap_bands([100.0, 101.0, 102.0], [1e-9, 1e-9, 1e-9])
    assert payload is not None
    assert payload["volume_quality"] < 0.5


def test_session_vwap_bands_volume_quality_can_return_intermediate_values():
    payload = session_vwap_bands([100.0, 101.0, 102.0], [0.25, 0.25, 0.25])
    assert payload is not None
    assert payload["volume_quality"] == pytest.approx(0.25)
