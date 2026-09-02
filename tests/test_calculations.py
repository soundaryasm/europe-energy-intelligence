"""Tests for pure Silver calculation helpers (Spec 003)."""
import pytest

from src.transformations.calculations import energy_mwh, safe_percentage, weighted_average


def test_energy_mwh_hourly_interval():
    assert energy_mwh(power_mw=100.0, interval_hours=1.0) == 100.0


def test_energy_mwh_fifteen_minute_interval():
    assert energy_mwh(power_mw=100.0, interval_hours=0.25) == 25.0


def test_energy_mwh_thirty_minute_interval():
    assert energy_mwh(power_mw=100.0, interval_hours=0.5) == 50.0


def test_safe_percentage_normal_case():
    assert safe_percentage(25.0, 100.0) == 25.0


def test_safe_percentage_zero_denominator_returns_none_not_zero():
    assert safe_percentage(10.0, 0.0) is None


def test_safe_percentage_none_inputs_return_none():
    assert safe_percentage(None, 100.0) is None
    assert safe_percentage(10.0, None) is None


def test_safe_percentage_full_generation_is_100_percent():
    assert safe_percentage(50.0, 50.0) == 100.0


def test_weighted_average_uniform_weights_equals_plain_average():
    result = weighted_average([(10.0, 1.0), (20.0, 1.0), (30.0, 1.0)])
    assert result == pytest.approx(20.0)


def test_weighted_average_mixed_interval_durations():
    # One 60-minute interval at price 100, two 30-minute intervals at 50 and 60
    # -> (100*1 + 50*0.5 + 60*0.5) / (1 + 0.5 + 0.5) = 155/2 = 77.5
    result = weighted_average([(100.0, 1.0), (50.0, 0.5), (60.0, 0.5)])
    assert result == pytest.approx(77.5)


def test_weighted_average_negative_prices_are_not_excluded():
    result = weighted_average([(-10.0, 1.0), (10.0, 1.0)])
    assert result == pytest.approx(0.0)


def test_weighted_average_all_zero_weight_returns_none():
    assert weighted_average([(10.0, 0.0), (20.0, 0.0)]) is None


def test_weighted_average_skips_none_entries():
    result = weighted_average([(None, 1.0), (10.0, 1.0)])
    assert result == pytest.approx(10.0)


def test_weighted_average_empty_input_returns_none():
    assert weighted_average([]) is None
