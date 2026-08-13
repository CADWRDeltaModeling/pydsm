import os
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from dms_datastore.read_multi import read_ts_repo

import click
import pyhecdss
import yaml


import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")


def update_dcc_dss(sdate, edate, dss_path, processed_file, dss_outfile, column_name):
    process_data = pd.read_csv(processed_file, comment='#', index_col=0, parse_dates=True).loc[sdate:edate]
    process_data = process_data[[column_name]]
    process_data['dsm2_val'] = np.where(
        process_data[column_name].astype(str).str.strip().str.lower() == 'closed', 0, 2
    )
    process_data = process_data[['dsm2_val']]

    if process_data.empty:
       raise ValueError(f"No data found for the period {sdate} to {edate} in file {processed_file}")
    for i in pyhecdss.get_ts(dss_outfile,f'{dss_path}'):
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
                             f"in the period {sdate} to {edate}")

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
    with pyhecdss.DSSFile(dss_outfile) as d:
        d.write_its(pathname, updated_data, units, period_type)
    return



_DEFAULT_CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gate_config.yaml")


@click.command()
@click.option("--config", "config_file", type=click.Path(exists=True, dir_okay=False),
              default=_DEFAULT_CONFIG_FILE, show_default=True,
              help="Path to the YAML config file.")
@click.argument("site", required=False)
def main(config_file, site):
    """Update DSM2 Gate DSS record(s) from processed CSV files.

    If SITE is omitted, all sites in the config file are updated.
    """
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    dcc_config_name = ['dcc']
    for s in dcc_config_name:
        site_cfg = config[s]
        update_dcc_dss(site_cfg['start'], site_cfg['end'], site_cfg['dss_path'],
                       site_cfg['processed_file'], site_cfg['outfile'], site_cfg['column_name'])
        logging.info(f"Updated DSM2 Gate DSS for site {s}")


if __name__ == "__main__":
    main()