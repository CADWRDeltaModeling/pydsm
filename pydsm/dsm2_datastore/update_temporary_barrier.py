# placeholder of updating dsm2 temporary barrier using schism th file
import os
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

import click
import pyhecdss
import yaml
from pydsm.dsm2_datastore.update_smscg import update_dss
from schimpy.th_io import read_th


import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")


_DEFAULT_CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gate_config.yaml")


def update_with_m_to_ft(start, end, dss_path, processed_file, outfile, column_name):
    processed_data = read_th(processed_file).loc[start:end]
    processed_th = processed_data[[column_name]]
    processed_data['dsm2'] = processed_th * 3.28084  # convert meters to feet

    update_dss(outfile, dss_path, processed_data[['dsm2']])


def update_no_convert(start, end, dss_path,processed_file, outfile, column_name):
    processed_data = read_th(processed_file).loc[start:end]
    processed_th = processed_data[[column_name]]

    processed_data['dsm2'] = processed_th

    update_dss(outfile, dss_path, processed_data[['dsm2']])

def update_mid_r_op_down(start, end, dss_path, processed_file1, processed_file2, outfile, column_name):
    processed_data1 = read_th(processed_file1).loc[start:end]
    processed_data2 = read_th(processed_file2).loc[start:end]
    processed_th1 = processed_data1[[column_name]]
    processed_th2 = processed_data2[[column_name]]

    processed_data = (processed_th1 + processed_th2)/2
    processed_data = processed_data.dropna() #remove rows with NaN values which occurs when either of the input files has missing data

    processed_data['dsm2'] = processed_data

    update_dss(outfile, dss_path, processed_data[['dsm2']])


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
    glc_config_name = ['glc']
    mid_r_config_name = ['mid_r']
    old_r_tracy_config_name = ['old_r_tracy']
    for s in glc_config_name:
        site_cfg = config[s]
        update_with_m_to_ft(site_cfg['start'], site_cfg['end'],
                            site_cfg['dss_path_weir_elev'], site_cfg['processed_file'], site_cfg['outfile'], site_cfg['column_name'][1])
        logging.info(f"Updated DSS for GLC weir elevation: {site_cfg['dss_path_weir_elev']}")
        update_with_m_to_ft(site_cfg['start'], site_cfg['end'],
                            site_cfg['dss_path_weir_width'], site_cfg['processed_file'], site_cfg['outfile'], site_cfg['column_name'][2])
        logging.info(f"Updated DSS for GLC weir width: {site_cfg['dss_path_weir_width']}")
        update_no_convert(site_cfg['start'], site_cfg['end'], site_cfg['dss_path_weir_install'],
                                site_cfg['processed_file'], site_cfg['outfile'], site_cfg['column_name'][0])
        logging.info(f"Updated DSS for GLC Install: {site_cfg['dss_path_weir_install']}")
        update_no_convert(site_cfg['start'], site_cfg['end'], site_cfg['dss_path_pipe_op'],
                                site_cfg['processed_file'], site_cfg['outfile'], site_cfg['column_name'][3])
        logging.info(f"Updated DSS for GLC Pipe Op: {site_cfg['dss_path_pipe_op']}")

    for s in mid_r_config_name:
        site_cfg = config[s]
        update_no_convert(site_cfg['start'], site_cfg['end'],
                                site_cfg['dss_path_install'], site_cfg['processed_file1'], site_cfg['outfile'], site_cfg['column_name'][0])
        logging.info(f"Updated DSS for MID_R Install: {site_cfg['dss_path_install']}")
        update_with_m_to_ft(site_cfg['start'], site_cfg['end'],
                                site_cfg['dss_path_elev'], site_cfg['processed_file1'], site_cfg['outfile'], site_cfg['column_name'][1])
        logging.info(f"Updated DSS for MID_R Elev: {site_cfg['dss_path_elev']}")
        update_mid_r_op_down(site_cfg['start'], site_cfg['end'],
                                site_cfg['dss_path_op_down'], site_cfg['processed_file2'], site_cfg['processed_file3'], site_cfg['outfile'], site_cfg['column_name'][2])
        logging.info(f"Updated DSS for MID_R Op Down: {site_cfg['dss_path_op_down']}")

    for s in old_r_tracy_config_name:
        site_cfg = config[s]
        update_no_convert(site_cfg['start'], site_cfg['end'],
                                site_cfg['dss_path_install'], site_cfg['processed_file'], site_cfg['outfile'], site_cfg['column_name'][0])
        logging.info(f"Updated DSS for OLD_R_TRACY Install: {site_cfg['dss_path_install']}")
        update_no_convert(site_cfg['start'], site_cfg['end'],
                                site_cfg['dss_path_op_down'], site_cfg['processed_file'], site_cfg['outfile'], site_cfg['column_name'][1])
        logging.info(f"Updated DSS for OLD_R_TRACY Op Down: {site_cfg['dss_path_op_down']}")


if __name__ == "__main__":
    main()