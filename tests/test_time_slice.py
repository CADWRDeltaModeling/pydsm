'''
Test function that does time slicing
'''
from pydsm.hydroh5 import HydroH5

def test_slice():
    s=HydroH5._convert_time_to_table_slice('02JAN1930','03JAN1930','1H','01JAN1930',96)
    assert slice(24,48,1) == s

