import os
import pydsm
from pydsm.input import parser
import warnings
import pandas as pd
import logging
import pydsm.hydroh5
from vtools.functions.filter import cosine_lanczos
import pyhecdss
pyhecdss.set_message_level(0)


def sum_dict(data_dict):
    """
    Sum of all the time series
    checks for non-dataframes to add
    converts period index to timestamp for add

    Parameters
    ----------
    data_dict : dictionary of data names to DataFrame

    Returns
    -------
    DataFrame
        sum of all the data in the dictionary resample to daily means
    """
    sum = 0.0
    for k in data_dict:
        data = data_dict[k]
        if isinstance(data, pd.DataFrame):
            if isinstance(data.index, pd.PeriodIndex):
                #warnings.warn('Converting %s to timestamp'%k)
                data.index = data.index.to_timestamp()
            if data.index.freqstr != 'D':
                data = data.resample('D').mean()
            sum = sum+data.iloc[:, 0]
        else:
            sum = sum+data
    return sum


def read_flows(table, base_dir):
    """
    Builds a dictionary of {name: timeseries data} from a table that has atleast the following columns: NAME, FILE, PATH.

    The data is either
     * Timeseries data retrieved from a FILE ( DSS format) and PATH (DSS pathname)
     * Constant data when FILE is 'constant' & PATH is the value

    Parameters
    ----------
    table : DataFrame
        table containing name and file and path of data
    base_dir : str
        directory used as based to construct full file path name of file in table

    Returns
    -------
    a dictionary of data indexed by NAME with SIGN from the FILE and PATH indicated in each row of the table

    """
    data_dict = {}
    grouped_by_file = table.groupby('FILE')
    for fname, group_info in grouped_by_file:
        logging.info(f'reading {base_dir}/{fname}')
        if fname == 'constant':
            for _, r in group_info.iterrows():
                rts, units, _ = float(r.PATH), 'cfs', 'INST-VAL'
                data_dict[r.NAME] = rts*float(r.SIGN)
        else:
            fpath = os.path.normpath(f'{base_dir}/{fname}')
            matches = pyhecdss.get_ts(fpath, *[r.PATH for _, r in group_info.iterrows()])
            for m, r in zip(matches, [r for _, r in group_info.iterrows()]):
                rts, units, _ = m
                if units.casefold() != 'cfs'.casefold():
                    warnings.warn('%s::%s::%s -- Units expected are cfs, got : %s' %
                                  (r.NAME, fname, r.PATH, units))
                data_dict[r.NAME] = rts*float(r.SIGN)
    return data_dict


def get_output_dir(tables):
    envvar = tables['ENVVAR']
    return envvar[envvar.NAME == 'DSM2OUTPUTDIR']['VALUE'].values[0]


def get_dsm2_dir(echo_file, tables):
    output_dir = get_output_dir(tables)
    output_rel_path = '../'*len(os.path.normpath(output_dir).split(os.path.sep))
    dsm2_dir = os.path.normpath(os.path.join(os.path.dirname(echo_file), output_rel_path))
    return dsm2_dir


def calculate_net_flow(echo_file):
    """
    Calculate net sum of flows at a daily resampling of all boundary flow and exports and diversions. 
    Each flow in DSM2 has a sign value and those are accounted for in this calculation

    Loosely speaking this would be equivalent of NDOI (Net Delta Outflow Index) for DSM2 applied to the Delta. 

    A more precise interpretation would require to remove certain source/sink terms e.g. Suisun Marsh

    Parameters
    ----------
    dsm2_dir : str
        directory containing the DSM2 main input file

    Returns
    -------
    DataFrame
        net sum of all boundary flows
    """
    logging.getLogger().setLevel(level=logging.INFO)
    sum_flows = net_flows_by_filename(echo_file)
    return sum([sum_flows[k] for k in sum_flows])


def net_flows_by_filename(echo_file):
    """
    calculate net flows by filename (.dss) from the input specified in the echo file

    Each flow in DSM2 is specified as a file (.dss) and pathname (/A/B/C//E/F) (or optionally constant) with a sign
    in the following tables :-

    BOUNDARY_FLOW
    SOURCE_FLOW
    SOURCE_FLOW_RESERVOIR
    INPUT_TRANSFER_FLOW

    These tables are combined and then grouped by the filename and the resulting matches are summed (for efficiency)


    Parameters
    ----------
    echo_file : str
        echo filename from DSM2 Hydro setup

    Returns
    -------
    dict
        DSS filename to summed flow read from that file
    """    
    tables = read_echo_file(echo_file)
    dsm2_dir = get_dsm2_dir(echo_file, tables)
    # Tables for inflows, sources/sinks and transfers
    bflow = tables['BOUNDARY_FLOW']
    sflow = tables['SOURCE_FLOW']
    srflow = tables['SOURCE_FLOW_RESERVOIR']
    trflow = tables['INPUT_TRANSFER_FLOW']
    dflows = pd.concat([tables[name] for name in ['BOUNDARY_FLOW', 'SOURCE_FLOW',
                       'SOURCE_FLOW_RESERVOIR', 'INPUT_TRANSFER_FLOW']])
    return {fname: sum_dict(read_flows(dfrows.assign(**{'FILE': fname}), dsm2_dir))
               for fname, dfrows in dflows.groupby('FILE')}


def read_echo_file(echo_file):
    """
    reads echo file and returns a dictionary of table names and their associated data in DataFrame(s)

    Parameters
    ----------
    echo_file : str
        echo file from DSM2 output

    Returns
    -------
    dict
         a dictionary of table names and their associated data in DataFrame(s)
    """
    with open(echo_file, 'r') as file:
        tables = parser.parse(file.read())
    return tables


def get_tidal_outflow(h5file, channel_id):
    """
    Read the h5 file for flow in channel_id
    Filter out the tidal signal using cosine lanczos (preferred) or godin filter
    Also resample to daily flow (period average to daily)

    One use of this is to retrieve the Net Delta Outflow from DSM2 output. For that 
    the channel_id is 441.

    Returns
    -------
    DataFrame
        Tidally filtered daily averaged flow at channel_id downstream
    """
    hydro = pydsm.hydroh5.HydroH5(h5file)
    cflow = hydro.get_channel_flow(channel_id, 'downstream')
    tcflow = cosine_lanczos(cflow, cutoff_period='40h')
    tcflowd = tcflow.resample('D').mean()
    return tcflowd
