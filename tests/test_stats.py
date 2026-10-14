from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.ui import stats


class TestPercentileOf:
    def test_higher_is_better_uses_mid_rank(self):
        assert stats.percentile_of(pd.Series([1, 2, 3, 4]), 4) == 87.5

    def test_lower_is_better_is_the_exact_mirror(self):
        assert stats.percentile_of(pd.Series([1, 2, 3, 4]), 4, higher_is_better=False) == 12.5
        assert stats.percentile_of(pd.Series([1, 2, 3, 4]), 1, higher_is_better=False) == 87.5

    def test_nan_rows_are_left_out_of_the_denominator(self):
        assert stats.percentile_of(pd.Series([1, 2, np.nan, 3, 4]), 4) == 87.5

    def test_ties_count_half(self):
        s = pd.Series([1, 2, 2, 3])
        assert stats.percentile_of(s, 2) == 50.0
        assert stats.percentile_of(s, 2, higher_is_better=False) == 50.0

    def test_all_tied_sits_in_the_middle(self):
        # most players have 0 red cards: 0 must not rank near the bottom
        assert stats.percentile_of(pd.Series([0, 0, 0, 0]), 0, higher_is_better=False) == 50.0

    def test_values_outside_the_pool(self):
        s = pd.Series([1, 2, 3, 4])
        assert stats.percentile_of(s, 10) == 100.0
        assert stats.percentile_of(s, 0) == 0.0

    @pytest.mark.parametrize("value", [None, np.nan, pd.NA])
    def test_missing_value_returns_none(self, value):
        assert stats.percentile_of(pd.Series([1, 2, 3]), value) is None

    def test_empty_or_all_nan_pool_returns_none(self):
        assert stats.percentile_of(pd.Series([], dtype=float), 1) is None
        assert stats.percentile_of(pd.Series([np.nan, np.nan]), 1) is None

    def test_nullable_int_column(self):
        s = pd.Series([1, 2, None, 4], dtype="Int64")
        assert stats.percentile_of(s, 4) == pytest.approx(250 / 3)

    def test_accepts_a_plain_list(self):
        assert stats.percentile_of([1, 2, 3, 4], 4) == 87.5


@pytest.mark.parametrize(
    "pct, index, label",
    [
        (0, 0, "Poor"), (24.9, 0, "Poor"),
        (25, 1, "Below avg"), (49.9, 1, "Below avg"),
        (50, 2, "Above avg"), (74.9, 2, "Above avg"),
        (75, 3, "Elite"), (100, 3, "Elite"),
    ],
)
def test_band(pct, index, label):
    assert stats.band(pct) == stats.Band(index, label)


@pytest.mark.parametrize(
    "n, text",
    [
        (1, "1st"), (2, "2nd"), (3, "3rd"), (4, "4th"), (11, "11th"), (12, "12th"),
        (13, "13th"), (21, "21st"), (22, "22nd"), (23, "23rd"), (101, "101st"),
        (111, "111th"), (112, "112th"), (0, "0th"),
    ],
)
def test_ordinal(n, text):
    assert stats.ordinal(n) == text


@pytest.mark.parametrize(
    "pct, text",
    [(87.4, "87th"), (87.5, "88th"), (21.5, "22nd"), (50, "50th"),
     (99.8, "99th"), (100, "99th"), (0.2, "1st"), (0, "1st")],
)
def test_pct_label_rounds_half_up_and_clamps(pct, text):
    assert stats.pct_label(pct) == text


class TestFmtNumber:
    def test_int_has_thousands_separator(self):
        assert stats.fmt_number(2536, "int") == "2,536"
        assert stats.fmt_number(2536.6, "int") == "2,537"

    def test_decimals(self):
        assert stats.fmt_number(19.24, "dec1") == "19.2"
        assert stats.fmt_number(1234.56, "dec1") == "1,234.6"
        assert stats.fmt_number(0.678, "dec2") == "0.68"

    def test_pct(self):
        assert stats.fmt_number(41.27, "pct") == "41.3%"

    @pytest.mark.parametrize("value", [None, np.nan, pd.NA])
    def test_missing_is_an_em_dash(self, value):
        assert stats.fmt_number(value, "int") == stats.MISSING == "—"

    def test_unknown_format_raises(self):
        with pytest.raises(ValueError):
            stats.fmt_number(1, "money")


@pytest.mark.parametrize(
    "total, nineties, expected",
    [(10, 20, 0.5), (10, 0, None), (None, 20, None), (10, None, None), (np.nan, 20, None)],
)
def test_per90(total, nineties, expected):
    assert stats.per90(total, nineties) == expected


@pytest.mark.parametrize(
    "season, short",
    [("2022-2023", "2022-23"), ("2019-2020", "2019-20"), ("2023", "2023"), ("All", "All")],
)
def test_short_season(season, short):
    assert stats.short_season(season) == short


@pytest.mark.parametrize(
    "name, text",
    [("Mohamed Salah", "MS"), ("Virgil van Dijk", "VD"), ("Alisson", "A"),
     ("N'Golo Kanté", "NK"), ("   ", "?")],
)
def test_initials(name, text):
    assert stats.initials(name) == text


@pytest.mark.parametrize("text, expected", [("Playing time", "playing_time"), ("Shooting", "shooting"),
                                            ("Passing & Assisting", "passing_assisting")])
def test_slug(text, expected):
    assert stats.slug(text) == expected


def test_is_missing_and_to_float():
    assert stats.is_missing(None) and stats.is_missing(np.nan) and stats.is_missing(pd.NA)
    assert not stats.is_missing(0) and not stats.is_missing("x") and not stats.is_missing([1, 2])
    assert stats.to_float("3.5") == 3.5
    assert stats.to_float("abc") is None
    assert stats.to_float(np.int64(4)) == 4.0
