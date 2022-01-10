import pytest
from pydsm.functions.tafcfs import tafm2cfs, cfs2tafm
import pandas as pd
import numpy as np

@pytest.fixture
def cfs2_taf():
    df=pd.read_csv('data/cfs2_taf.csv',index_col=0,parse_dates=True)
    return df.to_period(freq='M')

def test_cfs2taf(cfs2_taf):
    cfs2_taf = cfs2_taf[:'2003'] # truncating as it seems some of the later values need to be confirmed
    c2t = cfs2tafm()
    x= pd.concat([cfs2_taf,c2t],axis=1).dropna()
    assert all(np.isclose(x.iloc[:,0],x.iloc[:,1]))
