import os
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from dms_datastore.read_multi import read_ts_repo
from .update_dsm2_flow_dss import update_dsm2_flow_dss
from .update_dsm2_ec_dss import update_dsm2_ec_dss

import click
import pyhecdss
import yaml
import logging



def update_dsm2_potw_dss(sdate, edate, dss_path, dss_outfile, station_id):
    update_dsm2_flow_dss(sdate, edate, dss_path, dss_outfile, station_id)
    dss_ec_path = dss_path.replace("FLOW", "EC")
    update_dsm2_ec_dss(sdate, edate, dss_ec_path, dss_outfile, station_id)


_DEFAULT_CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "potw_config.yaml")


@click.command()
@click.option("--config", "config_file", type=click.Path(exists=True, dir_okay=False),
              default=_DEFAULT_CONFIG_FILE, show_default=True,
              help="Path to the YAML config file.")
@click.argument("site", required=False)
def main(config_file, site):
    """Update DSM2 POTW DSS record(s) from processed CSV files.

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
        update_dsm2_potw_dss(site_cfg['start'], site_cfg['end'], site_cfg['dss_path'],
                             site_cfg['outfile'], site_cfg['station_id'])
        logging.info(f"Updated DSM2 POTW DSS for site {s}")


if __name__ == "__main__":
    main()
