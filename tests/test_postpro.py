from numpy.core.fromnumeric import var
import pydsm
from pydsm import postpro
import pytest
import numpy as np

def test_postprocessor():
    study = postpro.Study('observed', 'data/sample_obs_small.dss')
    loc=postpro.Location('GrantLineCanal','GLC','Grant Line Canal in South Delta')
    vtype=postpro.VarType('ec','mmhos/cm')
    p=postpro.PostProcessor(study,loc,vtype)
    p.process()
    assert p.df is not None
    p.store_processed()
    p2=postpro.PostProcessor(study,loc,vtype)
    p2.load_processed()
    #assert_frame_equal(p.df, p2.df,check_names=False, check_column_type=False, check_like=False)