import h5py
from pydsm import dsm2h5
def test_model_type():
    """ Test for hydro or qual identification based on reading the groups of the .h5 file"""
    with h5py.File('data/historical_v82.h5','r') as h5f:
        assert dsm2h5.get_model(h5f) == 'hydro'
    with h5py.File('data/historical_v82_ec.h5','r') as h5f:
        assert dsm2h5.get_model(h5f) == 'qual'
