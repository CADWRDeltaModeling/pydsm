import pyhecdss
import pydsm.filter
import pytest

def test_godin():
    '''
    Test for godin filter
    '''
    d=pyhecdss.DSSFile('justoneone.dss')
    catdf=d.read_catalog()
    plist=d.get_pathnames(catdf[catdf.F=='TEST'])
    ts,u,p=d.read_rts(plist[0])
    tsg=pydsm.filter.godin_filter(ts)
    plist2=d.get_pathnames(catdf[catdf.F=='TEST-VTOOLS-GODIN'])
    tsg_vtools,u,p=d.read_rts(plist2[0])
    pytest.approx(tsg_vtools['05JAN1990':'15FEB1990'].values,tsg['05JAN1990':'15FEB1990'].values)
