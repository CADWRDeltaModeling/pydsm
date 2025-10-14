import h5py
import os
from pydsm import dsm2h5


def test_model_type():
    """Test for hydro or qual identification based on reading the groups of the .h5 file.

    Uses test data residing under tests/data to avoid relying on the working directory.
    """
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    hydro_file = os.path.join(data_dir, "historical_v82.h5")
    qual_file = os.path.join(data_dir, "historical_v82_ec.h5")
    with h5py.File(hydro_file, "r") as h5f:
        assert dsm2h5.get_model(h5f) == "hydro"
    with h5py.File(qual_file, "r") as h5f:
        assert dsm2h5.get_model(h5f) == "qual"
