'''
TAFM is an acronym for a unit of flow. It stands for thousand acre-feet per month. This is converted to CFS i.e. cubic feet per second

'''
import pandas as pd

def tafm2cfs(start:str='01OCT1920', nyears:int=100) -> pd.Series:
    """
    Conversion for flow from TAFM (thousand acre-feet per month) to CFS (cubic feet per second)
    This method generates a pandas Series of montlhy indexed value of conversion factors
    
    :: 

    (1000 ->T)*43560(AF(acre)->ft^3)/(ndaysinmonth*(24*60*60)) (seconds in a day)

    This can be used to multiply pandas Series with TAFM units and get pandas Series with CFS units

    Parameters
    ----------
    start : str, optional
        start month and year to generate the series, by default '01OCT1920'
    nyears : int, optional
        number of years from start, by default 100

    Returns
    -------
    pandas Series 
        series index by monthly values starting at "start" with converstion factors for TAFM to CFS
    """    
    s1=pd.Series(0, pd.period_range(start=start,periods=nyears*12,freq='M'))
    s1=s1.reset_index()
    s1['daysinmonth']=s1['index'].dt.daysinmonth
    s1=s1.set_index('index').loc[:,'daysinmonth']
    taf2cfs=(1000.0*43560.)/(s1*24*60*60)# (1000 ->T)*43560(AF(acre)->ft^3)/(ndaysinmonth*(24*60*60)) (seconds in a day)
    taf2cfs.name='TAFM2CFS'
    return taf2cfs

def cfs2tafm(start:str='01OCT1920', nyears:int=100) -> pd.Series:
    """
    Conversion factors for flow from CFS to TAFM. See :py:func:`tafm2cfs`

    This is the inverse of the conversion factors from :py:func:`tafm2cfs`

    PParameters
    ----------
    start : str, optional
        start month and year to generate the series, by default '01OCT1920'
    nyears : int, optional
        number of years from start, by default 100

    Returns
    -------
    pandas Series 
        series index by monthly values starting at "start" with converstion factors for CFS to TAFM
    """    
    cfs2taf = 1/tafm2cfs(start, nyears)
    cfs2taf.name='CFS2TAFM'
    return cfs2taf
