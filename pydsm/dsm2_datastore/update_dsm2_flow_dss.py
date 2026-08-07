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


def update_dsm2_flow_dss(sdate, edate, dss_path, process_flow_file, dss_outfile):

    process_flow_data = pd.read_csv(process_flow_file, comment='#', index_col=0, parse_dates=True).loc[sdate:edate]
    if process_flow_data.empty:
       raise ValueError(f"No flow data found in {process_flow_file} for the period {sdate} to {edate}")
    for i in pyhecdss.get_ts(dss_outfile,f'{dss_path}'):
        original_dss_data = i.data
        units = i.units
        pathname = original_dss_data.columns[0]
        period_type = i.period_type

    process_flow_data.columns = [pathname]

    # Determine overlap and extension periods
    original_start = original_dss_data.index[0]
    original_end = original_dss_data.index[-1]

     # Overwrite portion: where process_flow_data overlaps with existing data
    overwrite_mask = (process_flow_data.index >= original_start) & (process_flow_data.index <= original_end)
    overwrite_data = process_flow_data.loc[overwrite_mask]

    # Extension portion: where process_flow_data goes beyond existing data's end
    extension_mask = process_flow_data.index > original_end
    extension_data = process_flow_data.loc[extension_mask]

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


_CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "flow_config.yaml")

with open(_CONFIG_FILE, "r") as f:
    _config = yaml.safe_load(f)

# only top-level entries that are site configs (dicts), not global scalars like start_time/end_time/flow_bc_dss
_SITES = [k for k, v in _config.items() if isinstance(v, dict)]


@click.command()
@click.argument("site", required=False, type=click.Choice(_SITES))
def main(site):
    """Update DSM2 flow boundary DSS record(s) from processed CSV files.

    If SITE is omitted, all sites in the config file are updated.
    """
    sites = [site] if site else _SITES
    for s in sites:
        site_cfg = _config[s]
        update_dsm2_flow_dss(site_cfg['start'], site_cfg['end'], site_cfg['dss_path'],
                              site_cfg['process_flow_file'], site_cfg['outfile'])
        logging.info(f"Updated DSM2 flow DSS for site {s}")


if __name__ == "__main__":
    main()




