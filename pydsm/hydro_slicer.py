import pandas as pd
import numpy as np
import h5py

import click


def get_start_time(tbl):
    return tbl.attrs['start_time'][0].decode('utf-8')


def get_time_interval(tbl):
    return tbl.attrs['interval'][0].decode('utf-8')


def get_slice_indices(tbl, stime, etime):
    dindex = pd.date_range(start=get_start_time(
        tbl), freq=get_time_interval(tbl), periods=tbl.shape[0])
    dfindex = pd.DataFrame(np.arange(tbl.shape[0]), index=dindex)
    return tuple(dfindex[stime:etime].iloc[[0, -1]][0].values)


def slice_table(tbl, stime, etime):
    bi, ei = get_slice_indices(tbl, stime, etime)
    return tbl[slice(bi, ei)]


def copy_attrs_table(ntbl, tbl):
    for a in tbl.attrs:
        ntbl.attrs[a] = tbl.attrs[a]

def copy_path(path,fromhf,tohf):
    for tname in fromhf[path]:
        tpath = '%s/%s' % (path,tname)
        tbl = fromhf[tpath]
        tohf[tpath] = fromhf[tpath][:]
        ntbl = tohf[tpath]
        copy_attrs_table(ntbl, tbl)

#infile='historical_v8.h5'
#outfile='historical_sliced.h5'
#stime = '1990-01-10'
#etime = '1990-01-15'
@click.command()
@click.argument('infile')
@click.argument('outfile')
@click.argument('stime')
@click.argument('etime')
def slice_hydro(infile, outfile, stime, etime):
    """Slices hydro tidefile and writes out a hydro tidefile

    Args:

        INFILE (str): Input hydro tidefile

        OUTFILE (str): Output hydro tidefile

        STIME (str): Datetime string, e.g. 1990-01-10

        ETIME (str): Datetime string, e.g. 1990-01-15
    """    
    with h5py.File(infile,'r') as hf:
        with h5py.File(outfile, 'w-') as nhf:
            for tname in hf['/hydro/data']:
                tpath = '/hydro/data/%s' % tname
                tbl = hf[tpath]
                nhf[tpath] = slice_table(tbl, stime, etime)
                ntbl = nhf[tpath]
                copy_attrs_table(ntbl, tbl)
                ntbl.attrs['start_time'] = bytes(str(pd.to_datetime(stime)), 'utf-8')
            copy_path('/hydro/geometry',hf,nhf)
            copy_path('/hydro/input',hf,nhf)