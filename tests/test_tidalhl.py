# Test cases for Tidal High and Lows
import pytest
import os
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal
from numpy.testing import assert_array_almost_equal, assert_array_equal
from pydsm.functions import tidalhl


@pytest.fixture
def tidal_signal_with_gaps():
    """
    A simple tidal signal fixture
    """
    df = pd.read_csv(
        os.path.join(os.path.dirname(__file__), "data", "tidal_signal_with_gaps.csv"),
        parse_dates=[0],
        index_col=0,
        dtype="float",
    )
    dfh = pd.read_csv(
        os.path.join(
            os.path.dirname(__file__), "data", "tidal_signal_with_gaps_highs.csv"
        ),
        parse_dates=[0],
        index_col=0,
        dtype="float",
    )
    dfl = pd.read_csv(
        os.path.join(
            os.path.dirname(__file__), "data", "tidal_signal_with_gaps_lows.csv"
        ),
        parse_dates=[0],
        index_col=0,
        dtype="float",
    )
    return df, dfh, dfl


@pytest.fixture
def tidal_signal_with_gaps_doubled_shifted(tidal_signal_with_gaps):
    """
    A values doubled and shifted signal (25 minutes) based on tidal_signal_with_gaps
    """
    df, dfh, dfl = tidal_signal_with_gaps
    df2 = 2 * df  # increase amplitude to twice
    # resample @ min resolution and shift by 25 mins
    df2s = df2.resample("T").interpolate().shift(25, freq="T")
    # resample back at original resolution
    df2s = df2s.resample(tidal_signal_with_gaps.index.freq).interpolate()
    return df2s


@pytest.fixture
def tidal_signal_with_disturbances():
    """
    tidal signal with disturbances
    """
    df = pd.read_csv(
        os.path.join(
            os.path.dirname(__file__), "data", "tidal_signal_with_disturbances.csv"
        ),
        parse_dates=[0],
        index_col=0,
        dtype="float",
    )
    dfh = pd.read_csv(
        os.path.join(
            os.path.dirname(__file__),
            "data",
            "tidal_signal_with_disturbances_highs.csv",
        ),
        parse_dates=[0],
        index_col=0,
        dtype="float",
    )
    dfl = pd.read_csv(
        os.path.join(
            os.path.dirname(__file__), "data", "tidal_signal_with_disturbances_lows.csv"
        ),
        parse_dates=[0],
        index_col=0,
        dtype="float",
    )
    return df, dfh, dfl


def _plot_highs_lows(dfh, dfl, df):
    import matplotlib.pyplot as plt

    plt.plot(dfh, "g^", label="highs")
    plt.plot(dfl, "r+", label="lows")
    plt.plot(df, label="data")
    plt.show()


def test_signal_with_gaps(tidal_signal_with_gaps):
    df, dfh_expected, dfl_expected = tidal_signal_with_gaps
    dfh, dfl = tidalhl.get_tidal_hl_rolling(df)
    # _plot_highs_lows(dfh,dfl,df)
    assert_array_almost_equal(dfh.values, dfh_expected.values)
    assert_array_equal(dfh.index.values, dfh_expected.index.values)
    assert_array_almost_equal(dfl.values, dfl_expected.values)
    assert_array_equal(dfl.index.values, dfl_expected.index.values)


def test_signal_with_disturbances(tidal_signal_with_disturbances):
    df, dfh_expected, dfl_expected = tidal_signal_with_disturbances
    dfh, dfl = tidalhl.get_tidal_hl_rolling(df)
    # _plot_highs_lows(dfh,dfl,df)
    assert_array_almost_equal(dfh.values, dfh_expected.values)
    assert_array_equal(dfh.index.values, dfh_expected.index.values)
    assert_array_almost_equal(dfl.values, dfl_expected.values)
    assert_array_equal(dfl.index.values, dfl_expected.index.values)


def test_amplitude_with_gaps(tidal_signal_with_gaps):
    df, dfh, dfl = tidal_signal_with_gaps
    dfamp = tidalhl.get_tidal_amplitude(dfh, dfl)
    dfh2, dfl2 = 2 * dfh, 2 * dfl
    dfamp2 = tidalhl.get_tidal_amplitude(dfh2, dfl2)
    dfdiff = tidalhl.get_tidal_amplitude_diff(dfamp, dfamp2)
    assert_array_almost_equal(2 * dfamp.values, dfamp2.values)
