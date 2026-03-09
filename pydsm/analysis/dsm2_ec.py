# script by Lily Tomkovic to create DSM2 boundary data from the meta-latinhypercube cases' datasets
# takes input yaml (format in progress to be somewhat forward compatible with eventual format of repo)
# produces boundary inputs for DSM2 within existing folders

import pandas as pd
import numpy as np

from pydsm.analysis.dssutils import get_dss_data
import pyhecdss

from pydsm.input import dsm2_config as config
from pydsm.analysis.dsm2_utilities import calc_ndo
from mrzecest.ec_boundary import ec_est
import string

import datetime as dt
import os


def dsm2_ec_est(config_infile, ec_est_dss_outfile='', f_part_out='', write_dss=True):
    """estimate martinez EC from DSM2 setup files
    
    Parameters
    ----------
    config_infile: str | Path 
    
        name of config.inp file for simulation to be estimated. points to the stage, boundary, and DICU files necessary for calculation

    ec_est_dss_outfile: str | Path
    
        name of output file to write estimated EC out to

    f_part_out: str

        name of DSS out F Part

    write_dss: bool

        trigger to write DSS output to the ec_est_dss_outfile path

    Returns
    -------
    mrzecest : pd.DataFrame
        Data with calculation of EC estimate at Martinez from DSM2 boundary inputs for simulation
    
    """

    config.setConfigVars(config_infile) # load the configuration parameters

    start = pd.to_datetime(f'{config.getAttr("START_DATE")} 0000', format="%d%b%Y %H%M")
    end = pd.to_datetime(f'{config.getAttr("END_DATE")} 0000', format="%d%b%Y %H%M")

    print(f"\t\tCalculating boundary salinity for the period {start.strftime('%m-%d-%Y')} to {end.strftime('%m-%d-%Y')}")
    
    stage_bdy_filename = os.path.normpath(os.path.join(os.getcwd(),config.getAttr('STAGE_SOURCE_FILE'))).replace("\\", "/")
    flow_filename = os.path.normpath(os.path.join(os.getcwd(),config.getAttr('BNDRYINPUT'))).replace("\\", "/")
    dcd_filename = os.path.normpath(os.path.join(os.getcwd(),config.getAttr('DICUFILE'))).replace("\\", "/")

    df_ndo = calc_ndo(flow_filename, dcd_filename)

    b_part_dss_filename_dict={'RSAC054': stage_bdy_filename}
    b_part_c_part_dict={'RSAC054': 'STAGE'}
    df_mtz_stage = get_dss_data(b_part_dss_filename_dict, 'b_part', primary_part_c_part_dict=b_part_c_part_dict, daily_avg=False)

    # parameters from estimation
    log10beta = 10.217 # x[0] from ec_boundary_fit_gee.py printout
    npow = 0.461 # x[1] from ec_boundary_fit_gee.py printout
    area_coef = -6127433509.04 # x[2] from ec_boundary_fit_gee.py printout
    energy_coef = 1495.91 # x[3] from ec_boundary_fit_gee.py printout
    beta0 = 1.6828 # from const coef result 
    beta1 = -23.0735 * 1e-3 # from gnpow coef result 
    filter_k0 = 6 # from fitting_config.yaml
    filt_coefs = np.array([0.111, 0.896, -0.606, 0.678, -0.745, -0.416, -0.046, 
                        1.161, 0.321, -1.069, 0.515, -0.965, 0.576]) * 1e-3 # z{n} from output coefs
    filter_dt = pd.Timedelta('3h') # from fitting_config.yaml
    so = 20000. # hardwired in ec_boundary_fit_gee.py
    sb = 200. # hardwired in ec_boundary_fit_gee.py

    mrzecest = ec_est(df_ndo, df_mtz_stage, start, end,
                    area_coef,energy_coef,
                    log10beta,
                    beta0, beta1, npow, filter_k0,
                    filt_coefs, filter_dt,
                    so, sb)
    
    if write_dss:
        
        if not os.path.exists(ec_est_dss_outfile) and write_dss:
            dumdss = pyhecdss.DSSFile(ec_est_dss_outfile, create_new=True) # create the file if writing out to DSS

        unit_part = "uS/CM"
        pathname_out = f"/FILL+CHAN/RSAC054/EC/{start.strftime('%d%b%Y')}/1HOUR/{f_part_out}/"
        ptype = 'INST-VAL'
        mrzecest.index = mrzecest.index.to_timestamp()

        # write to DSS file
        with pyhecdss.DSSFile(ec_est_dss_outfile) as d_out:
            if mrzecest.index.freq is None:
                mrzecest.index.freq = pd.infer_freq(mrzecest.index)
            # write regular output to DSS file
            d_out.write_rts(pathname_out, mrzecest, unit_part, ptype)

    return mrzecest
