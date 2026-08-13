import os
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

import click
import pyhecdss
import yaml


import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")




_DEFAULT_CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gate_config.yaml")

def update_dss(dssoutfile, dss_path, process_data):
    for i in pyhecdss.get_ts(dssoutfile,f'{dss_path}'):
            original_dss_data = i.data
            units = i.units
            pathname = original_dss_data.columns[0]
            period_type = i.period_type
    
    process_data.columns = [pathname]
    
    # Determine overlap and extension periods
    original_start = original_dss_data.index[0]
    original_end = original_dss_data.index[-1]
    
    # Overlap portion: where process_data overlaps with existing data
    overwrite_mask = (process_data.index >= original_start) & (process_data.index <= original_end)
    overwrite_data = process_data.loc[overwrite_mask]
    
    # Extension portion: where process_data goes beyond existing data's end
    extension_mask = process_data.index > original_end
    extension_data = process_data.loc[extension_mask]
    
    if not overwrite_data.empty:
        logging.info(f"Overwrote {len(overwrite_data)} values in {pathname} "
                     f"from {overwrite_data.index[0]} to {overwrite_data.index[-1]}")
    
    if not extension_data.empty:
        logging.info(f"Extended {pathname} with {len(extension_data)} values "
                     f"from {extension_data.index[0]} to {extension_data.index[-1]}")
    
    if overwrite_data.empty and extension_data.empty:
        logging.warning(f"No overlapping or extension data found for {pathname} "
                        f"in the period {process_data.index[0]} to {process_data.index[-1]}")
    
    # timestamps don't line up exactly between the two irregular series, so drop any
    # original rows within process_data's time span entirely and splice process_data's
    # own rows in, rather than trying to align/merge on matching index labels
    proc_start, proc_end = process_data.index[0], process_data.index[-1]
    kept_original = original_dss_data.loc[
        (original_dss_data.index < proc_start) | (original_dss_data.index > proc_end)
    ]
    updated_data = pd.concat([kept_original, process_data]).sort_index()
    
    # pd.concat drops the DatetimeIndex freq; restore it so write_rts can infer the DSS E-part
    updated_data.index.freq = original_dss_data.index.freq or pd.infer_freq(updated_data.index)
    # Write updated data back to the output DSS file
    with pyhecdss.DSSFile(dssoutfile) as d:
           d.write_its(pathname, updated_data, units, period_type)
    return

def update_smscg_flashboards_boadlock_dss(sdate, edate, dss_path_boatlock, dss_path_flashboard, processed_file, dss_outfile, column_name):

    # Load the processed CSV file
    processed_data = pd.read_csv(processed_file,index_col=0,comment='#',parse_dates=True)
    processed_data = processed_data.loc[sdate:edate][column_name]
    processed_data['flashboards_op'] = np.nan
    processed_data['boatlock_op'] = np.nan
    if processed_data.empty:
        logging.warning(f"No data found in the processed CSV file for the specified date range: {sdate} to {edate}")
        return
    processed_data.loc[processed_data['flashboards'] == 'IN', 'flashboards_op'] = 1
    processed_data.loc[processed_data['flashboards'] == 'IN', 'boatlock_op'] = 0
    processed_data.loc[processed_data['flashboards'] == 'OUT', 'flashboards_op'] = 0
    processed_data.loc[processed_data['flashboards'] == 'OUT', 'boatlock_op'] = 1

    update_dss(dss_outfile, dss_path_boatlock, processed_data[['boatlock_op']])
    logging.info(f"Updated DSM2 SMSCG Gate DSS for boatlock at {dss_path_boatlock}")
    update_dss(dss_outfile, dss_path_flashboard, processed_data[['flashboards_op']])
    logging.info(f"Updated DSM2 SMSCG Gate DSS for flashboards at {dss_path_flashboard}")

def update_smscg_radial_dss(sdate, edate, dss_path_radial_op, 
                            dss_path_radial_to, dss_path_radial_from, processed_file, dss_outfile, column_name):
    # Load the processed CSV file
    processed_data = pd.read_csv(processed_file,index_col=0,comment='#',parse_dates=True)
    processed_data = processed_data.loc[sdate:edate][column_name]

    processed_data['radial_op'] = np.nan
    processed_data['radial_to'] = np.nan
    processed_data['radial_from'] = np.nan
    if processed_data.empty:
        logging.warning(f"No data found in the processed CSV file for the specified date range: {sdate} to {edate}")
        return

    gate_state_map = {'Open': 1, 'Tidal': 0.5, 'Closed': 0.2}
    for gate_col in ['gate_1', 'gate_2', 'gate_3']:
        if gate_col in processed_data.columns:
            processed_data[gate_col] = processed_data[gate_col].map(gate_state_map)
    processed_data['gate_sum'] = processed_data[['gate_1', 'gate_2', 'gate_3']].sum(axis=1)
    #combination of O-O-O
    processed_data.loc[round(processed_data['gate_sum'],1) == 3, 'radial_to'] = 1
    processed_data.loc[round(processed_data['gate_sum'],1) == 3, 'radial_from'] = 1
    processed_data.loc[round(processed_data['gate_sum'],1) == 3, 'radial_op'] = 1
    #combination of O-O-CL
    processed_data.loc[round(processed_data['gate_sum'],1) == 2.2, 'radial_to'] = 0.67
    processed_data.loc[round(processed_data['gate_sum'],1) == 2.2, 'radial_from'] = 0.67
    processed_data.loc[round(processed_data['gate_sum'],1) == 2.2, 'radial_op'] = 1
    #combination of O-CL-CL
    processed_data.loc[round(processed_data['gate_sum'],1) == 1.4, 'radial_to'] = 0.33
    processed_data.loc[round(processed_data['gate_sum'],1) == 1.4, 'radial_from'] = 0.33
    processed_data.loc[round(processed_data['gate_sum'],1) == 1.4, 'radial_op'] = 1
    #combination of CL-CL-CL
    processed_data.loc[round(processed_data['gate_sum'],1) == 0.6, 'radial_to'] = 0
    processed_data.loc[round(processed_data['gate_sum'],1) == 0.6, 'radial_from'] = 0
    processed_data.loc[round(processed_data['gate_sum'],1) == 0.6, 'radial_op'] = 0
    #combinarion of O-O-T
    processed_data.loc[round(processed_data['gate_sum'],1) == 2.5, 'radial_to'] = 1
    processed_data.loc[round(processed_data['gate_sum'],1) == 2.5, 'radial_from'] = 0.67
    processed_data.loc[round(processed_data['gate_sum'],1) == 2.5, 'radial_op'] = -10
    #combination of O-T-T
    processed_data.loc[round(processed_data['gate_sum'],1) == 2, 'radial_to'] = 1
    processed_data.loc[round(processed_data['gate_sum'],1) == 2, 'radial_from'] = 0.33
    processed_data.loc[round(processed_data['gate_sum'],1) == 2, 'radial_op'] = -10
    #combination of T-T-T
    processed_data.loc[round(processed_data['gate_sum'],1) == 1.5, 'radial_to'] = 1
    processed_data.loc[round(processed_data['gate_sum'],1) == 1.5, 'radial_from'] = 0
    processed_data.loc[round(processed_data['gate_sum'],1) == 1.5, 'radial_op'] = -10
    #combination of CL-CL-T
    processed_data.loc[round(processed_data['gate_sum'],1) == 0.9, 'radial_to'] = 0.33
    processed_data.loc[round(processed_data['gate_sum'],1) == 0.9, 'radial_from'] = 0
    processed_data.loc[round(processed_data['gate_sum'],1) == 0.9, 'radial_op'] = -10
    #combination of CL-T-T
    processed_data.loc[round(processed_data['gate_sum'],1) == 1.2, 'radial_to'] = 0.67
    processed_data.loc[round(processed_data['gate_sum'],1) == 1.2, 'radial_from'] = 0
    processed_data.loc[round(processed_data['gate_sum'],1) == 1.2, 'radial_op'] = -10
    #combination of CL-O-T
    processed_data.loc[round(processed_data['gate_sum'],1) == 1.7, 'radial_to'] = 0.67
    processed_data.loc[round(processed_data['gate_sum'],1) == 1.7, 'radial_from'] = 0.33
    processed_data.loc[round(processed_data['gate_sum'],1) == 1.7, 'radial_op'] = -10

    update_dss(dss_outfile, dss_path_radial_op, processed_data[['radial_op']])
    logging.info(f"Updated DSM2 SMSCG Gate DSS for radial operation at {dss_path_radial_op}")
    update_dss(dss_outfile, dss_path_radial_to, processed_data[['radial_to']])
    logging.info(f"Updated DSM2 SMSCG Gate DSS for radial to at {dss_path_radial_to}")
    update_dss(dss_outfile, dss_path_radial_from, processed_data[['radial_from']])
    logging.info(f"Updated DSM2 SMSCG Gate DSS for radial from at {dss_path_radial_from}")

     

@click.command()
@click.option("--config", "config_file", type=click.Path(exists=True, dir_okay=False),
              default=_DEFAULT_CONFIG_FILE, show_default=True,
              help="Path to the YAML config file.")
def main(config_file):
    """Update DSM2 Gate DSS record(s) from processed CSV files.

    If SITE is omitted, all sites in the config file are updated.
    """
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)
    ccf_config_name = ['smscg']
    for s in ccf_config_name:
        site_cfg = config[s]
        update_smscg_flashboards_boadlock_dss(site_cfg['start'], site_cfg['end'], site_cfg['dss_path_boatlock'],
                                         site_cfg['dss_path_flashboard'], site_cfg['processed_file'], site_cfg['outfile'], site_cfg['column_name'])
        update_smscg_radial_dss(site_cfg['start'], site_cfg['end'], site_cfg['dss_path_radial_op'],
                                site_cfg['dss_path_radial_to'], site_cfg['dss_path_radial_from'],
                                site_cfg['processed_file'], site_cfg['outfile'], site_cfg['column_name'])



if __name__ == "__main__":
    main()