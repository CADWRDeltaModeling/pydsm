'''
Timeseries operation
Conform to HEC-convention.
'''
import pandas as pd

def resample_hec_style(df, interval='D'):
    '''
    Resampling of time series in DataFrame provided for the interval (see Pandas resample for codes)
    In addition to conform to HEC-conventions the resampling is done with closed="right" and label="right"
    see pandas resample documentation to understand these arguments
    '''
    return df.resample(interval, closed='right', label='right')
def per_aver(df,interval='D'):
    '''
    PermissionErroriod averages of the time series in DataFrame provided for the interval (see Pandas resample for codes)
    In addition to conform to HEC-conventions the resampling is done with closed="right" and label="right"
    '''
    return resample_hec_style(df,interval).mean()
def per_max(df, interval='D'):
    return resample_hec_style(df,interval).max()
def per_min(df, interval='D'):
    return resample_hec_style(df, interval).min()