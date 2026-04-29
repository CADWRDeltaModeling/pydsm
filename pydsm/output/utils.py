from pydsm.output import hydroh5
from pydsm.input import read_input, write_input
import h5py
import sys


def write_csv_with_meta(filepath, df, meta, header=None):
    """
    Write a DataFrame to CSV with leading comment lines containing metadata.

    Comment lines are prefixed with ``#`` so they can be skipped when reading
    back with ``pd.read_csv(..., comment='#')``.

    Parameters
    ----------
    filepath : str
        Destination CSV path.
    df : pandas.DataFrame or pandas.Series
        Data to write.
    meta : dict
        Ordered key/value pairs written as ``# key: value`` lines.
    header : list of str | None
        Column header override passed to ``to_csv``.
    """
    with open(filepath, "w", newline='') as f:
        for key, value in meta.items():
            f.write(f"# {key}: {value}\n")
        df.to_csv(f, header=header if header is not None else True)

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

