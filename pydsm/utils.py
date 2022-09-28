import pydsm
from pydsm import hydroh5
from pydsm.input import read_input, write_input
import h5py

def update_hydro_tidefile_with_inp(hydro_tidefile, input_file, tidefile_path='/hydro/input/channel', table_name='CHANNEL'):
    """
    update hydro tidefile with specified table read from input file

    Parameters
    ----------
    hydro_tidefile : str
        DSM2 hydro output tidefile
    input_file : str
        .inp files containing the table (see table_name below)
    tidefile_path : str, optional
        path to table inside the tidefile, by default '/hydro/input/channel'
    table_name : str, optional
        table name to be read from the .inp file, by default 'CHANNEL'
    """
    tables = read_input(input_file)
    dfchi=tables[table_name]
    with h5py.File(hydro_tidefile,'r+') as h5:
        h5table = h5.get(tidefile_path)
        h5table[()] = [tuple(x) for x in dfchi.to_numpy()]

