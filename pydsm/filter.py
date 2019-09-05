import pandas as pd
import numpy as np
def generate_godin_fir(timeinterval):
    '''
    generate godin filter impulse response for given timeinterval
    timeinterval could be anything that pd.Timedelta can accept
    '''
    mins=pd.Timedelta(timeinterval).seconds/60
    wts24=np.zeros(round(24*60/mins))
    wts24[:]=1/wts24.size
    tidal_period=round(24.75*60/mins)
    if tidal_period%2==0: tidal_period=tidal_period+1
    wts25=np.zeros(tidal_period)
    wts25[:]=1.0/wts25.size
    return np.convolve(wts25,np.convolve(wts24,wts24))
def godin_filter(df,timeinterval='15min'):
    '''
    Uses FIR filter and convolves with the data frame values
    return godin filtered values for data frame values
    '''
    godin_ir=generate_godin_fir(df.index.freq)
    assert len(df.columns) == 1
    dfg=pd.DataFrame(np.convolve(df.iloc[:,0].values,godin_ir,mode='same'), columns=df.columns, index = df.index)
    return dfg
#
