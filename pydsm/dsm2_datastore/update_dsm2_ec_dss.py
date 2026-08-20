import os
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from dms_datastore.read_multi import read_ts_repo

import click
import pyhecdss
import yaml
import logging



def update_dsm2_ec_dss(sdate, edate, dss_path, dss_outfile, station_id):

    process_ec_data = read_ts_repo(station_id = station_id, variable = 'ec', start= sdate, end = edate, repo = 'processed')
    process_ec_data = process_ec_data.resample('D').mean()  # Resample to daily mean
    if process_ec_data.empty:
       raise ValueError(f"No EC data found for station {station_id} for the period {sdate} to {edate}")
    for i in pyhecdss.get_ts(dss_outfile,f'{dss_path}'):
        original_dss_data = i.data
        units = i.units
        pathname = original_dss_data.columns[0]
        period_type = i.period_type

    process_ec_data.columns = [pathname]

    # Determine overlap and extension periods
    original_start = original_dss_data.index[0]
    original_end = original_dss_data.index[-1]

     # Overwrite portion: where process_flow_data overlaps with existing data
    overwrite_mask = (process_ec_data.index >= original_start) & (process_ec_data.index <= original_end)
    overwrite_data = process_ec_data.loc[overwrite_mask]

    # Extension portion: where process_flow_data goes beyond existing data's end
    extension_mask = process_ec_data.index > original_end
    extension_data = process_ec_data.loc[extension_mask]

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


_DEFAULT_CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ec_config.yaml")


@click.command()
@click.option("--config", "config_file", type=click.Path(exists=True, dir_okay=False),
              default=_DEFAULT_CONFIG_FILE, show_default=True,
              help="Path to the YAML config file.")
@click.argument("site", required=False)
def main(config_file, site):
    """Update DSM2 EC DSS record(s) from processed CSV files.

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
        update_dsm2_ec_dss(site_cfg['start'], site_cfg['end'], site_cfg['dss_path'],
                             site_cfg['outfile'], site_cfg['station_id'])
        logging.info(f"Updated DSM2 EC DSS for site {s}")


if __name__ == "__main__":
    main()
