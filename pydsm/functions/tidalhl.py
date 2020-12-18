import numpy as np
import pandas as pd
from vtools.functions.filter import cosine_lanczos, godin


def get_unique_max(dfhl):
    v = np.NINF
    rv = None
    for r in dfhl.iterrows():
        if np.isnan(r[1]['min']):
            if r[1]['max'] > v:
                v = r[1]['max']
                rv = (r[0], v)
        else:
            if rv:
                yield rv
                v = np.NINF
                rv = None
    if rv:
        yield rv


def get_unique_min(dfhl):
    v = np.PINF
    rv = None
    for r in dfhl.iterrows():
        if np.isnan(r[1]['max']):
            if r[1]['min'] < v:
                v = r[1]['min']
                rv = (r[0], v)
        else:
            if rv:
                yield rv
                v = np.PINF
                rv = None
    if rv:
        yield rv


def build_unique_highs(dfhl):
    dfhu = pd.DataFrame.from_records(get_unique_max(dfhl), index=0)
    dfhu = dfhu.drop(columns=[0])
    dfhu.columns = ['max']
    dfhu.index.name = 'time'
    return dfhu


def build_unique_lows(dfhl):
    dflu = pd.DataFrame.from_records(get_unique_min(dfhl), index=0)
    dflu = dflu.drop(columns=[0])
    dflu.columns = ['min']
    dflu.index.name = 'time'
    return dflu


def ffill_zero_gradient(df):
    """Forward fill with zero gradients

    Args:
        df (DataFrame): with a single column

    Returns:
        DataFrame: filled with the locations of the zero gradient
    """
    dfg = np.gradient(df[df.columns[0]])
    zerog = np.where(dfg[1:] == 0)[0]
    return df.iloc[zerog].resample(df.index.freq).ffill()


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


def get_tidal_hl_rolling(df, cutoff_period='2H', resample_period='1T', interpolate_method='pchip', moving_window_size='7H'):
    '''
    df is a pandas DataFrame with time as index and a single column of values
    Filter with cutoff_period (default='2H'), resampled with resample_period='1T' with interpolation method='pchip'
    and then moving window (max/min) of size window_size='7H'
    returns a tuple of tidal high and tidal low time series 
    '''
    dfb = df.resample(resample_period).fillna(method='backfill')
    df = df.resample(resample_period).interpolate(method=interpolate_method)
    df[dfb.iloc[:, 0].isna()] = np.nan
    df = cosine_lanczos(df, cutoff_period)
    dfmax = df.rolling(moving_window_size).max()
    dfmax.columns = ['max']
    dfmin = df.rolling(moving_window_size).min()
    dfmin.columns = ['min']
    dfmax = ffill_zero_gradient(dfmax)
    dfmin = ffill_zero_gradient(dfmin)
    si = df.first_valid_index()
    ei = df.last_valid_index()
    dfh, dfl = limit_to_indices(where_same(dfmax, df), si, ei), limit_to_indices(
        where_same(dfmin, df), si, ei)
    dfh, dfl = dfh.to_frame(), dfl.to_frame()
    #
    dfhl = pd.concat([dfh, dfl], axis=0)
    dfhl = dfhl.sort_index()
    dfh, dfl = build_unique_highs(dfhl), build_unique_lows(dfhl)
    # remove highs and lows where original series has missing values
    dfh, dfl = filter_where_na(dfh, dfb), filter_where_na(dfl, dfb)
    return dfh, dfl


def get_tidal_hl_zerocrossing(df, round_to='1T'):
    '''
    Finds the tidal high and low times
    '''
    # ddf=np.gradient(df[df.columns[0]].values)
    # zc,zi=zerocross1d(df.index.astype(np.int64),ddf,getIndices=True)
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


def zerocross1d(x, y, getIndices=False):
    """
      Find the zero crossing points in 1d data.

      Find the zero crossing events in a discrete data set.
      Linear interpolation is used to determine the actual
      locations of the zero crossing between two data points
      showing a change in sign. Data point which are zero
      are counted in as zero crossings if a sign change occurs
      across them. Note that the first and last data point will
      not be considered whether or not they are zero. 

      Parameters
      ----------
      x, y : arrays
          Ordinate and abscissa data values.
      getIndices : boolean, optional
          If True, also the indicies of the points preceding
          the zero crossing event will be returned. Defeualt is
          False.

      Returns
      -------
      xvals : array
          The locations of the zero crossing events determined
          by linear interpolation on the data.
      indices : array, optional
          The indices of the points preceding the zero crossing
          events. Only returned if `getIndices` is set True.
    """

    # Check sorting of x-values
    if np.any((x[1:] - x[0:-1]) <= 0.0):
        raise("The x-values must be sorted in ascending order!")
    # Indices of points *before* zero-crossing
    indi = np.where(y[1:]*y[0:-1] < 0.0)[0]

    # Find the zero crossing by linear interpolation
    dx = x[indi+1] - x[indi]
    dy = y[indi+1] - y[indi]
    zc = -y[indi] * (dx/dy) + x[indi]

    # What about the points, which are actually zero
    zi = np.where(y == 0.0)[0]
    # Do nothing about the first and last point should they
    # be zero
    zi = zi[np.where((zi > 0) & (zi < x.size-1))]
    # Select those point, where zero is crossed (sign change
    # across the point)
    zi = zi[np.where(y[zi-1]*y[zi+1] < 0.0)]

    # Concatenate indices
    zzindi = np.concatenate((indi, zi))
    # Concatenate zc and locations corresponding to zi
    zz = np.concatenate((zc, x[zi]))

    # Sort by x-value
    sind = np.argsort(zz)
    zz, zzindi = zz[sind], zzindi[sind]

    if not getIndices:
        return zz
    else:
        return zz, zzindi


def get_tidal_amplitude(dfh, dfl):
    '''
    Returns a dataframe with amplitude at the times of the low following the high being used for amplitude calculation
    '''
    dfamp = pd.concat([dfh, dfl], axis=1)
    dfamp = dfamp[['min']].dropna().join(dfamp[['max']].ffill())
    return pd.DataFrame(dfamp['max']-dfamp['min'], columns=['amplitude'])


def get_tidal_amplitude_diff(dfamp1, dfamp2, percent_diff=False):
    '''
    Get the difference of values within +/- 4H of values in the two amplitude arrays
    '''
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


def get_tidal_phase_diff(dfh2, dfl2, dfh1, dfl1):
    '''
    return the phase difference between df2 and df1 tidal highs and lows
    scans +/- 4 hours in df1 to get the highs and lows in that windows for df2 to 
    get the tidal highs and lows at the times of df1
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


def get_mean(arr):
    return np.nanmean(arr)


def match_next(dfh1, dfh2):
    dfh = pd.concat([dfh1, dfh2], axis=1)
    dfh.columns = ['1', '2']
    # resample and fill 4 hours in either direction
    dfh1 = dfh[['1']].resample('1T').asfreq().ffill(limit=240).bfill(limit=240)
    # sample at index and drop na to get values within 4 hours of each other
    dfh = dfh1.loc[dfh[['1']].index].join(dfh[['2']]).dropna()
    return dfh


def match_high_lows(dfh1, dfl1, dfh2, dfl2):
    dfh = match_next(dfh1, dfh2)
    dfh.columns = ['1', '2']
    dfl = match_next(dfl1, dfl2)
    dfl.columns = ['1', '2']
    return match_next(dfh1, dfh2), match_next(dfl1, dfl2)
