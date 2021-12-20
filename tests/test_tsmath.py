import pytest
import pydsm
import numpy as np
import pandas as pd
from pydsm.functions import tsmath

@pytest.fixture(scope="module",params=['ts','period'])
def linear_timeseries(request):
    '''
    A simple increasing time series to use for averaging functions
    ```
    01JAN2000 0100 - 0
    01JAN2000 0200 - 1
    ....
    01JAN2000 2200 - 21
    01JAN2000 2300 - 22
    02JAN2000 0000 - 23
    ```
    '''
    nvals=100
    ts = pd.DataFrame(np.arange(0,nvals), columns=['values'], index=pd.date_range(start='01JAN2000 0100',periods=nvals,freq='H'))
    if request.param=='period':
        return ts.to_period()
    else:
        return ts

def test_per_aver(linear_timeseries):
    tsavg=tsmath.per_aver(linear_timeseries)
    assert tsavg.loc['01JAN2000'].values[0] == sum(range(0,24))/24

def test_per_max(linear_timeseries):
    tsmax=tsmath.per_max(linear_timeseries)
    assert tsmax.loc['01JAN2000'].values[0] == 23

def test_per_min(linear_timeseries):
    tsmin=tsmath.per_min(linear_timeseries)
    assert tsmin.loc['01JAN2000'].values[0] == 0

def test_mse(linear_timeseries):
    assert 0 == tsmath.mse(linear_timeseries.iloc[:,0],linear_timeseries.iloc[:,0])

def test_rmse(linear_timeseries):
    assert 0 == tsmath.rmse(linear_timeseries.iloc[:,0],linear_timeseries.iloc[:,0])

def test_nse(linear_timeseries):
    assert 1 == tsmath.nash_sutcliffe(linear_timeseries.iloc[:,0,],linear_timeseries.iloc[:,0])

def test_percent_bias(linear_timeseries):
    assert 0 == tsmath.percent_bias(linear_timeseries.iloc[:,0],linear_timeseries.iloc[:,0])

def test_mean_error(linear_timeseries):
    assert 0 == tsmath.mean_error(linear_timeseries.iloc[:,0], linear_timeseries.iloc[:,0])