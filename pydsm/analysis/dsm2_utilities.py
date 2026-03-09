import pandas as pd
import numpy as np

from pydsm.analysis.dssutils import get_dss_data
import pyhecdss

def calc_dcu(dcd_filename, write_outfile=None):
    """calculate Delta Consumptive Use from DICULFILE inputs from DSM2 setup
    
    Parameters
    ----------
    dcd_filename: str | Path 
    
        name of boundary flow DSM2 input file in DSS format

    write_outfile: str | Path | None
    
        name of output file to write Net Delta Ouflow (df_ndo) out to

    Returns
    -------
    cu_total_dcd : pd.DataFrame
        Data with calculation of Delta Consumptive Use from boundary inputs
    
    """
    
    div_seep_dcd_c_part_dss_filename_dict = {'DIV-FLOW': dcd_filename, 'SEEP-FLOW': dcd_filename}
    drain_dcd_c_part_dss_filename_dict = {'DRAIN-FLOW': dcd_filename}

    df_div_seep_dcd = get_dss_data(div_seep_dcd_c_part_dss_filename_dict, 'c_part', filter_b_part_numeric=True)
    df_drain_dcd = get_dss_data(drain_dcd_c_part_dss_filename_dict, 'c_part', filter_b_part_numeric=True)

    df_div_seep_dcd['dcd_divseep_total']=df_div_seep_dcd[df_div_seep_dcd.columns].sum(axis=1)
    df_drain_dcd['dcd_drain_total']=df_drain_dcd[df_drain_dcd.columns].sum(axis=1)

    cu_total_dcd = pd.merge(df_div_seep_dcd, df_drain_dcd, how='left', left_index=True, right_index=True)

    cu_total_dcd['cu_total'] = cu_total_dcd['dcd_divseep_total'] - cu_total_dcd['dcd_drain_total']

    cu_total_dcd = cu_total_dcd[['cu_total']]

    if write_outfile:
        cu_total_dcd.to_csv(write_outfile, index=True, float_format="%.2f")

    return cu_total_dcd


def calc_ndo(flow_filename, dcd_filename, write_outfile=None):
    """calculate Net Delta Outflow using the BNDRYINPUT and DICULFILE inputs from DSM2 setup
    
    Parameters
    ----------
    flow_filename: str | Path 
    
        name of boundary flow DSM2 input file in DSS format

    dcd_filename: str | Path 
    
        name of boundary flow DSM2 input file in DSS format

    write_outfile: str | Path | None
    
        name of output file to write Net Delta Ouflow (df_ndo) out to

    Returns
    -------
    df_ndo : pd.DataFrame
        Data with calculation of Net Delta Outflow from boundary inputs
    
    """

    cu_total_dcd = calc_dcu(dcd_filename)

    b_part_dss_filename_dict = {'RSAC155': flow_filename, 
                                'RSAN112': flow_filename,
                                'BYOLO040': flow_filename, 
                                'RMKL070': flow_filename, 
                                'RCSM075': flow_filename, 
                                'RCAL009': flow_filename, 
                                'SLBAR002': flow_filename, 
                                'CHSWP003': flow_filename, 
                                'CHDMC004': flow_filename, 
                                'CHVCT001': flow_filename, 
                                'ROLD034': flow_filename, 
                                'CHCCC006': flow_filename}
    df_ndo = get_dss_data(b_part_dss_filename_dict, 'b_part')
    df_ndo = pd.merge(df_ndo, cu_total_dcd, how='left', left_index=True, right_index=True)

    positive_flows = ['RSAC155', 'BYOLO040', 'RMKL070', 'RCSM075', 'RCAL009']
    negative_flows = ['SLBAR002', 'CHSWP003', 'CHDMC004', 'CHVCT001', 'ROLD034', 'CHCCC006', 'cu_total']

    df_ndo['ndo'] = df_ndo[positive_flows].sum(axis=1) - df_ndo[negative_flows].sum(axis=1)
    df_ndo = df_ndo[['ndo']]
    
    if write_outfile:
        df_ndo.to_csv(write_outfile, index=True, float_format="%.2f")

    return df_ndo
