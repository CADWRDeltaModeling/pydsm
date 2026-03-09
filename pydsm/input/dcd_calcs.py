import pandas as pd
import pyhecdss as dss


def get_bpart_pattern():
    return '[0-9]+|BBID'


def get_dss_data(file, path_pattern):
    """
    returns a data frame of all the time series that match the path_pattern

    Parameters
    ----------
    file : str
        dss filename
    path_pattern : str
        path pattern of /A/B/C//E/F/ parts, e.g. ///FLOW//// will match any C part of FLOW 

    Returns
    -------
    DataFrame
        pandas DataFrame with columns of each data matched
    """
    data = dss.get_matching_ts(file, path_pattern)
    df = pd.concat([r[0] for r in data], axis=1, join='inner')
    return df


def sum(dfflows):
    ''' sum over the columns '''
    return dfflows.sum(axis=1)


def get_dcd_flows(file, bpart_pattern='[0-9]+|BBID', seepage=True, epart='1DAY'):
    """
    gets the diversion, drainage and seepage flows as three data frames

    Parameters
    ----------
    file : str
        DSS filename for DCD or SMCD output
    bpart_pattern : str, optional
        pattern to match for BPART of file, by default '[0-9]+|BBID'
    seepage : bool, optional
        read seepage if True, by default True

    Returns
    -------
    tuple
        returns a tuple of diversion flows, drainage flows and seepage flows for all nodes
    """
    div_flows = get_dss_data(file, f'//({bpart_pattern})/DIV-FLOW//{epart}//')
    drain_flows = get_dss_data(file, f'//({bpart_pattern})/DRAIN-FLOW//{epart}//')
    if seepage:
        seep_flows = get_dss_data(file, f'//({bpart_pattern})/SEEP-FLOW//{epart}//')
    if seepage:
        return div_flows, drain_flows, seep_flows
    else:
        return div_flows, drain_flows


def calculate_netcd(div_flows, drain_flows, seep_flows=None):
    '''
    calculate the total net channel depletions for all the div_flows, drain_flows and seep_flows
    netcd = div_flows - drain_flows +seep_flows

    if seep_flows is None then its calculated as div_flows - drain_flows
    '''
    if seep_flows is None:
        return sum(div_flows) - sum(drain_flows)
    else:
        return sum(div_flows) + sum(seep_flows) - sum(drain_flows)
