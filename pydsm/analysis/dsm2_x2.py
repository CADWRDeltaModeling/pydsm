import pandas as pd
from vtools.functions.filter import cosine_lanczos

from pydsm.analysis.dssutils import get_dss_data

from vtools.functions.unit_conversions import ec_psu_25c

def find_x2(transect, thresh=2., convert_km=0.001, distance_bias=0.0):
    """ 
    Given a series transect, indexed by distance, locate a distance with value is close to thresh
    """

    midpoints = (transect.index.to_series() + transect.index.to_series().shift(1))
    index_new = pd.concat((transect.index.to_series(), 
                           midpoints),axis=0).sort_values().iloc[0:-1]
    row_new = transect.reindex(index_new).interpolate().sort_values()

    index_x2 = row_new.searchsorted(2.) # identify location of X2
    index_x2 = index_x2.clip(max=len(row_new) - 1)
    x2 = row_new.index[index_x2.clip(max=len(row_new) - 1)]
    final_x2 = (x2 * convert_km - distance_bias)

    return final_x2

def calc_x2_from_dss(dss_file, names, epart_int='1HOUR'):

    b_part_dss_filename_dict = {str(name):dss_file for name in names}
    b_part_c_part_dict = {str(name):'EC' for name in names}
    b_part_e_part_dict = {str(name):epart_int for name in names}

    ec_df = get_dss_data(b_part_dss_filename_dict, 'b_part', \
        primary_part_c_part_dict=b_part_c_part_dict, primary_part_e_part_dict=b_part_e_part_dict)
    
    for col_ec in ec_df.columns:
        ec_df[col_ec] = ec_psu_25c(ec_df.loc[:, col_ec])

    x2_df = pd.DataFrame(index=ec_df.index, columns=['x2'])

    for index, row in ec_df.iterrows():
        row.index = row.index.astype(float)
        x2_df.loc[index,'x2'] = find_x2(row)

    return x2_df
