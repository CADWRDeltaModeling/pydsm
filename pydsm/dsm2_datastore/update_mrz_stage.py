import os
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

import click
import pyhecdss
import yaml
from dms_datastore.read_multi import read_ts_repo


import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")

def update_mrz_stage_dss(sdate, edate, dss_path, station_id, dss_outfile):

    process_stage_data = read_ts_repo(station_id, variable='elev', start=sdate, end=edate, repo="processed")
    if process_stage_data.empty:
       raise ValueError(f"No stage data found in the processed repo for station {station_id} for the period {sdate} to {edate}")
    for i in pyhecdss.get_ts(dss_outfile,f'{dss_path}'):
        original_dss_data = i.data
        units = i.units
        pathname = original_dss_data.columns[0]
        period_type = i.period_type

    process_stage_data.columns = [pathname]
    correction_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mrz_anh_setup_correction.csv")
    correction_data = pd.read_csv(correction_file, comment='#', index_col=0, parse_dates=True).loc[sdate:edate]
    correction_data.columns = [pathname]

    dsm2_stage_data = process_stage_data + correction_data

    # Determine overlap and extension periods
    original_start = original_dss_data.index[0]
    original_end = original_dss_data.index[-1]

     # Overwrite portion: where dsm2_stage_data overlaps with existing data
    overwrite_mask = (dsm2_stage_data.index >= original_start) & (dsm2_stage_data.index <= original_end)
    overwrite_data = dsm2_stage_data.loc[overwrite_mask]

    # Extension portion: where dsm2_stage_data goes beyond existing data's end
    extension_mask = dsm2_stage_data.index > original_end
    extension_data = dsm2_stage_data.loc[extension_mask]

    updated_data = original_dss_data.copy()

    if not overwrite_data.empty:
        updated_data.loc[overwrite_data.index] = overwrite_data.values
        logging.info(f"Overwrote {len(overwrite_data)} values in {pathname} "
                         f"from {overwrite_data.index[0]} to {overwrite_data.index[-1]}")

    if not extension_data.empty:
        updated_data = pd.concat([updated_data, extension_data])
        logging.info(f"Extended {pathname} with {len(extension_data)} values "
                         f"from {extension_data.index[0]} to {extension_data.index[-1]}")

    if overwrite_data.empty and extension_data.empty:
        logging.warning(f"No overlapping or extension data found for {pathname} "
                             f"in the period {sdate} to {edate}")

    # pd.concat drops the DatetimeIndex freq; restore it so write_rts can infer the DSS E-part
    updated_data.index.freq = original_dss_data.index.freq or pd.infer_freq(updated_data.index)
    # Write updated data back to the output DSS file
    with pyhecdss.DSSFile(dss_outfile) as d:
        d.write_rts(pathname, updated_data, units, period_type)


_DEFAULT_CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mrz_config.yaml")


@click.command()
@click.option("--config", "config_file", type=click.Path(exists=True, dir_okay=False),
              default=_DEFAULT_CONFIG_FILE, show_default=True,
              help="Path to the YAML config file.")
@click.argument("site", required=False)
def main(config_file, site):
    """Update MRZ stage DSS record(s) from processed CSV files.

    If SITE is omitted, all sites in the config file are updated.
    """
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)
    # only top-level entries that are site configs (dicts), not global scalars like start_time/end_time/flow_bc_dss
    sites_available = [k for k, v in config.items() if isinstance(v, dict)]
    if site and site not in sites_available:
        raise click.BadParameter(f"'{site}' must be one of {sites_available}", param_hint="'SITE'")

    sites = [site] if site else sites_available
    for s in sites:
        site_cfg = config[s]
        update_mrz_stage_dss(site_cfg['start'], site_cfg['end'], site_cfg['dss_path'],
                              site_cfg['station_id'], site_cfg['outfile'])
        logging.info(f"Updated MRZ stage DSS for site {s}")


if __name__ == "__main__":
    main()