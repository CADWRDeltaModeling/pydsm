import numpy as np
import pandas as pd
'''
Common utility functions for interacting with h5py. Used by hydro and qual h5 readers
'''


def list_table_paths(h5, group_paths):
    """
    returns a list of paths to tables (h5py.DataSets)
    """
    table_paths = []
    # incase both hydro and qual output to the same file (some ovewriting possible?)
    group_paths = ['/hydro/input', '/hydro/data',
                   '/hydro/geometry', 'input', 'output']
    for path in group_paths:
        g = h5.get(path)
        if not g:
            continue
        for key in g.keys():
            table_paths.append(path+'/'+key)
    return table_paths


def read_compound_table(h5, tpath):
    '''
    reads a compound table from the open h5 handle and table path tpath
    returns a DataFrame after converting them to strings (stripped)
    '''
    x = h5.get(tpath)
    result = pd.DataFrame(
        [
            [v[name].astype(np.str).strip() for name in v.dtype.names] for v in x],
        columns=[name for name in x[0].dtype.names
                 ]
    )
    return result

def strip(df):
    '''
    strips the object types of a DataFrame df
    '''
    df_obj=df.select_dtypes(['object'])
    df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
    return df
