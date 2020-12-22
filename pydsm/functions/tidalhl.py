import numpy as np
import pandas as pd
import numba
from vtools.functions.filter import cosine_lanczos


def get_smoothed_resampled(df, cutoff_period='2H', resample_period='1T', interpolate_method='pchip'):
    """Resample the dataframe (indexed by time) to the regular period of resample_period using the interpolate method

    Furthermore the cosine lanczos filter is used with a cutoff_period to smooth the signal to remove high frequency noise

    Args:

        df (DataFrame): A single column dataframe indexed by datetime

        cutoff_period (str, optional): cutoff period for cosine lanczos filter. Defaults to '2H'.

        resample_period (str, optional): Resample to regular period. Defaults to '1T'.

        interpolate_method (str, optional): interpolation for resampling. Defaults to 'pchip'.

    Returns:

        DataFrame: smoothed and resampled dataframe indexed by datetime
    """
    dfb = df.resample(resample_period).fillna(method='backfill')
    df = df.resample(resample_period).interpolate(method=interpolate_method)
    df[dfb.iloc[:, 0].isna()] = np.nan
    return cosine_lanczos(df, cutoff_period)


@numba.jit(nopython=True)
def lmax(arr):
    '''Local maximum: Returns value only when centered on maximum
    '''
    idx = np.argmax(arr)
    if idx == len(arr)/2:
        return arr[idx]
    else:
        return np.NaN


@numba.jit(nopython=True)
def lmin(arr):
    '''Local minimum: Returns value only when centered on minimum
    '''
    idx = np.argmin(arr)
    if idx == len(arr)/2:
        return arr[idx]
    else:
        return np.NaN


def periods_per_window(moving_window_size: str, period_str: str) -> int:
    """Number of period size in moving window

    Args:

        moving_window_size (str): moving window size as a string e.g 7H for 7 hour

        period_str (str): period as str e.g. 1T for 1 min

    Returns:

        int: number of periods in the moving window rounded to an integer
    """

    return int(pd.Timedelta(moving_window_size)/pd.to_timedelta(pd.tseries.frequencies.to_offset(period_str)))


def tidal_highs(df, moving_window_size='7H'):
    """Tidal highs (could be upto two highs in a 25 hr period)

    Args:

        df (DataFrame): a time series with a regular frequency

        moving_window_size (str, optional): moving window size to look for highs within. Defaults to '7H'.

    Returns:

        DataFrame: an irregular time series with highs at resolution of df.index
    """
    period_str = df.index.freqstr
    periods = periods_per_window(moving_window_size, period_str)
    dfmax = df.rolling(moving_window_size, min_periods=periods).apply(lmax, raw=True)
    dfmax = dfmax.shift(periods=-(periods//2-1))
    dfmax = dfmax.dropna()
    dfmax.columns = ['max']
    return dfmax


def tidal_lows(df, moving_window_size='7H'):
    """Tidal lows (could be upto two lows in a 25 hr period)

    Args:

        df (DataFrame): a time series with a regular frequency

        moving_window_size (str, optional): moving window size to look for lows within. Defaults to '7H'.

    Returns:

        DataFrame: an irregular time series with lows at resolution of df.index
    """
    period_str = df.index.freqstr
    periods = periods_per_window(moving_window_size, period_str)
    dfmin = df.rolling(moving_window_size, min_periods=periods).apply(lmin, raw=True)
    dfmin = dfmin.shift(periods=-(periods//2-1))
    dfmin = dfmin.dropna()
    dfmin.columns = ['min']
    return dfmin


def get_tidal_hl(df, cutoff_period='2H', resample_period='1T', interpolate_method='pchip', moving_window_size='7H'):
    """Get Tidal highs and lows

    Args:

        df (DataFrame): A single column dataframe indexed by datetime

        cutoff_period (str, optional): cutoff period for cosine lanczos filter. Defaults to '2H'.

        resample_period (str, optional): Resample to regular period. Defaults to '1T'.

        interpolate_method (str, optional): interpolation for resampling. Defaults to 'pchip'.

        moving_window_size (str, optional): moving window size to look for lows within. Defaults to '7H'.

    Returns:

        tuple of DataFrame: Tidal high and tidal low time series 
    """
    dfs = get_smoothed_resampled(df, cutoff_period, resample_period, interpolate_method)
    return tidal_highs(dfs), tidal_lows(dfs)


get_tidal_hl_rolling = get_tidal_hl  # for older refs. #FIXME


def get_tidal_amplitude(dfh, dfl):
    """Tidal amplitude given tidal highs and lows

    Args:

        dfh (DataFrame): Tidal highs time series

        dfl (DataFrame): Tidal lows time series

    Returns:

        DataFrame: Amplitude timeseries, at the times of the low following the high being used for amplitude calculation
    """
    dfamp = pd.concat([dfh, dfl], axis=1)
    dfamp = dfamp[['min']].dropna().join(dfamp[['max']].ffill())
    return pd.DataFrame(dfamp['max']-dfamp['min'], columns=['amplitude'])


def get_value_diff(df, percent_diff=False):
    '''
    Get the difference of values of each element in the dataframe
    The times in the dataframe may or may not coincide as this is a slice of irregularly sampled time series
    On any error, the returned value is np.nan
    '''
    try:
        arr = [df[c].dropna() for c in df.columns]
        if percent_diff:
            value_diff = 100.0 * (arr[0].values[0]-arr[1].values[0])/arr[1].values[0]
        else:
            value_diff = arr[0].values[0]-arr[1].values[0]
        return value_diff
    except:
        return np.nan


def get_tidal_amplitude_diff(dfamp1, dfamp2, percent_diff=False):
    """Get the difference of values within +/- 4H of values in the two amplitude arrays

    Args:

        dfamp1 (DataFrame): Amplitude time series

        dfamp2 (DataFrame): Amplitude time series

        percent_diff (bool, optional): If true do percent diff. Defaults to False.

    Returns:

        DataFrame: Difference dfamp1-dfamp2 or % Difference (dfamp1-dfamp2)/dfamp2*100  for values within +/- 4H of each other
    """
    dfamp = pd.concat([dfamp1, dfamp2], axis=1).dropna(how='all')
    dfamp.columns = ['2', '1']
    tdelta = '4H'
    sliceamp = [slice(t-pd.to_timedelta(tdelta), t+pd.to_timedelta(tdelta)) for t in dfamp.index]
    ampdiff = [get_value_diff(dfamp[sl], percent_diff) for sl in sliceamp]
    return pd.DataFrame(ampdiff, index=dfamp.index)


def get_index_diff(df):
    '''
    Get the difference of index values of each element in the dataframe
    The times in the dataframe may or may not coincide
    The difference is in Timedelta and is converted to minutes 
    On any error, the returned value is np.nan
    '''
    try:
        arr = [df[c].dropna() for c in df.columns]
        tidal_phase_diff = (arr[0].index[0]-arr[1].index[0]).total_seconds()/60.
        return tidal_phase_diff
    except:
        return np.nan


def get_tidal_phase_diff(dfh2, dfl2, dfh1, dfl1):
    """Calculates the phase difference between df2 and df1 tidal highs and lows

    Scans +/- 4 hours in df1 to get the highs and lows in that windows for df2 to 
    get the tidal highs and lows at the times of df1


    Args:

        dfh2 (DataFrame): Timeseries of tidal highs

        dfl2 (DataFrame): Timeseries of tidal lows

        dfh1 (DataFrame): Timeseries of tidal highs

        dfl1 (DataFRame): Timeseries of tidal lows

    Returns:

        DataFrame: Phase difference (dfh2-dfh1) and (dfl2-dfl1) in minutes
    """
    '''
    '''
    tdelta = '4H'
    sliceh1 = [slice(t-pd.to_timedelta(tdelta), t+pd.to_timedelta(tdelta)) for t in dfh1.index]
    slicel1 = [slice(t-pd.to_timedelta(tdelta), t+pd.to_timedelta(tdelta)) for t in dfl1.index]
    dfh21 = pd.concat([dfh2, dfh1], axis=1)
    dfh21.columns = ['2', '1']
    dfl21 = pd.concat([dfl2, dfl1], axis=1)
    dfl21.columns = ['2', '1']
    high_phase_diff, low_phase_diff = [get_index_diff(dfh21[sl]) for sl in sliceh1], [
        get_index_diff(dfl21[sl]) for sl in slicel1]
    merged_diff = pd.merge(pd.DataFrame(high_phase_diff, index=dfh1.index), pd.DataFrame(
        low_phase_diff, index=dfl1.index), how='outer', left_index=True, right_index=True)
    return merged_diff.iloc[:, 0].fillna(merged_diff.iloc[:, 1])


def get_tidal_hl_zerocrossing(df, round_to='1T'):
    '''
    Finds the tidal high and low times using zero crossings of the first derivative. 

    This works for all situations but is not robust in the face of noise and perturbations in the signal
    '''
    zc, zi = zerocross(df)
    if round_to:
        zc = pd.to_datetime(zc).round(round_to)
    return zc


def zerocross(df):
    '''
    Calculates the gradient of the time series and identifies locations where gradient changes sign
    Returns the time rounded to nearest minute where the zero crossing happens (based on linear derivative assumption)
    '''
    diffdfv = pd.Series(np.gradient(df[df.columns[0]].values), index=df.index)
    indi = np.where((np.diff(np.sign(diffdfv))) & (diffdfv[1:] != 0))[0]
    # Find the zero crossing by linear interpolation
    zdb = diffdfv[indi].index
    zda = diffdfv[indi+1].index
    x = diffdfv.index
    y = diffdfv.values
    dx = x[indi+1] - x[indi]
    dy = y[indi+1] - y[indi]
    zc = -y[indi] * (dx/dy) + x[indi]
    return zc, indi

##---- FUNCTIONS CACHED BELOW THIS LINE PERHAPS TO USE LATER? ---#


def where_changed(df):
    '''
    '''
    diff = np.diff(df[df.columns[0]].values)
    wdiff = np.where(diff != 0)[0]
    wdiff = np.insert(wdiff, 0, 0)  # insert the first value i.e. zero index
    return df.iloc[wdiff+1, :]


def where_same(dfg, df):
    '''
    return dfg only where its value is the same as df for the same time stamps
    i.e. the interesection locations with df
    '''
    dfall = pd.concat([dfg, df], axis=1)
    return dfall[dfall.iloc[:, 0] == dfall.iloc[:, 1]].iloc[:, 0]


def limit_to_indices(df, si, ei):
    return df[(df.index > si) & (df.index < ei)]


def filter_where_na(df, dfb):
    '''
    remove values in df where dfb has na values
    '''
    dfx = dfb.loc[df.index]
    return df.loc[dfx.dropna().index, :]
