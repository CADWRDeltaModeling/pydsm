# Test cases for Tidal High and Lows
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal
from numpy.testing import assert_array_almost_equal, assert_array_equal
from pydsm.functions import tidalhl

@pytest.fixture
def tidal_signal_with_gaps():
    '''
    A simple tidal signal fixture
    '''
    df=pd.read_csv('tidal_signal_with_gaps.csv',parse_dates=[0],index_col=0,dtype='float')
    dfh=pd.read_csv('tidal_signal_with_gaps_highs.csv',parse_dates=[0],index_col=0,dtype='float')
    dfl=pd.read_csv('tidal_signal_with_gaps_lows.csv',parse_dates=[0],index_col=0,dtype='float')
    return df,dfh,dfl

@pytest.fixture
def tidal_signal_with_disturbances():
    '''
    tidal signal with disturbances
    '''
    df=pd.read_csv('tidal_signal_with_disturbances.csv',parse_dates=[0],index_col=0,dtype='float')
    dfh=pd.read_csv('tidal_signal_with_disturbances_highs.csv',parse_dates=[0],index_col=0,dtype='float')
    dfl=pd.read_csv('tidal_signal_with_disturbances_lows.csv',parse_dates=[0],index_col=0,dtype='float')
    return df,dfh,dfl

def _plot_highs_lows(dfh,dfl,df):
    import matplotlib.pyplot as plt
    plt.plot(dfh,'g^',label='highs')
    plt.plot(dfl,'r+',label='lows')
    plt.plot(df,label='data')
    plt.show()

def test_signal_with_gaps(tidal_signal_with_gaps):
    df,dfh_expected,dfl_expected=tidal_signal_with_gaps
    dfh,dfl=tidalhl.get_tidal_hl_rolling(df)
    #_plot_highs_lows(dfh,dfl,df)
    assert_array_almost_equal(dfh.values,dfh_expected.values)
    assert_array_equal(dfh.index.values,dfh_expected.index.values)
    assert_array_almost_equal(dfl.values,dfl_expected.values)
    assert_array_equal(dfl.index.values,dfl_expected.index.values)

def test_signal_with_disturbances(tidal_signal_with_disturbances):
    df,dfh_expected,dfl_expected=tidal_signal_with_disturbances
    dfh,dfl=tidalhl.get_tidal_hl_rolling(df)
    #_plot_highs_lows(dfh,dfl,df)
    assert_array_almost_equal(dfh.values,dfh_expected.values)
    assert_array_equal(dfh.index.values,dfh_expected.index.values)
    assert_array_almost_equal(dfl.values,dfl_expected.values)
    assert_array_equal(dfl.index.values,dfl_expected.index.values)
