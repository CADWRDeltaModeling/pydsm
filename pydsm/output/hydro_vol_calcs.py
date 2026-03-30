"""
Utility functions and CLI command for calculating DSM2 Hydro volumes.

Channel volume  = channel average cross-sectional area (ft²) × channel length (ft)
Reservoir volume = reservoir area (million ft²) × 1e6 × reservoir water height (ft)

Unit conversion factors (relative to cubic feet):
    cubic-feet  : 1.0
    acre-feet   : 1 / 43560
    maf         : 1 / (43560 × 1 000 000)
"""

import click
import sys
import pandas as pd

from pydsm.output.hydroh5 import HydroH5
from pydsm.output.utils import write_csv_with_meta

# ---------------------------------------------------------------------------
# Unit conversion
# ---------------------------------------------------------------------------

UNITS = {
    "cubic-feet": 1.0,
    "acre-feet": 1.0 / 43560.0,
    "maf": 1.0 / (43560.0 * 1e6),
}

UNIT_LABELS = {
    "cubic-feet": "ft³",
    "acre-feet": "acre-ft",
    "maf": "MAF",
}


def convert_volume(vol_cubic_feet, unit):
    """
    Convert a volume Series or DataFrame from cubic feet to the requested unit.

    Parameters
    ----------
    vol_cubic_feet : pandas.Series or pandas.DataFrame
    unit : str
        One of 'cubic-feet', 'acre-feet', 'maf'.

    Returns
    -------
    Same type as input, scaled to the requested unit.
    """
    factor = UNITS[unit]
    return vol_cubic_feet * factor


# ---------------------------------------------------------------------------
# Core calculation functions
# ---------------------------------------------------------------------------

def get_channel_volumes(hydro, channels=None, timewindow=None, unit="acre-feet"):
    """
    Calculate per-channel volumes as time series.

    Volume = channel average area (ft²) × channel length (ft), converted to *unit*.

    Parameters
    ----------
    hydro : HydroH5
        Open HydroH5 instance.
    channels : list of str | None
        Channel numbers to include. None or empty list means all channels.
    timewindow : str | None
        DSM2 style time window e.g. '01JAN2014 - 01JAN2015'.
    unit : str
        Output unit: 'cubic-feet', 'acre-feet', or 'maf'.

    Returns
    -------
    pandas.DataFrame
        Time-indexed DataFrame with one column per channel. Column names are
        channel numbers (str). Values are in *unit*.
    """
    chan_table = hydro.get_channels()  # chan_no already cast to str
    chan_lengths = chan_table.set_index("chan_no")["length"].astype(float)

    if not channels:
        channels = list(chan_lengths.index.astype(str))

    channels = [str(c) for c in channels]
    chan_lengths = chan_lengths.loc[channels]

    avg_areas = hydro.get_channel_avg_area(channels, timewindow)
    # avg_areas columns are channel numbers as strings
    avg_areas.columns = [c.split("-")[0] for c in avg_areas.columns]

    vol_cft = avg_areas.multiply(chan_lengths.values, axis=1)
    return convert_volume(vol_cft, unit)


def get_reservoir_volumes(hydro, reservoirs=None, timewindow=None, unit="acre-feet"):
    """
    Calculate per-reservoir volumes as time series.

    Volume = reservoir area (million ft²) × 1e6 × reservoir height (ft), converted to *unit*.

    Parameters
    ----------
    hydro : HydroH5
        Open HydroH5 instance.
    reservoirs : list of str | None
        Reservoir names to include. None or empty list means all reservoirs.
    timewindow : str | None
        DSM2 style time window e.g. '01JAN2014 - 01JAN2015'.
    unit : str
        Output unit: 'cubic-feet', 'acre-feet', or 'maf'.

    Returns
    -------
    pandas.DataFrame
        Time-indexed DataFrame with one column per reservoir. Values are in *unit*.
    """
    res_table = hydro.get_input_table("/hydro/input/reservoir")
    # areas stored in millions of square feet
    res_areas = res_table.set_index("name")["area"].astype(float) * 1e6

    if not reservoirs:
        reservoirs = list(res_areas.index.astype(str))

    reservoirs = [str(r) for r in reservoirs]
    res_areas = res_areas.loc[reservoirs]

    heights = hydro.get_reservoir_height(reservoirs, timewindow)
    heights.columns = [c for c in reservoirs]

    vol_cft = heights.multiply(res_areas.values, axis=1)
    return convert_volume(vol_cft, unit)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.command(name="calc-volumes")
@click.argument("hydrofile", type=click.Path(exists=True))
@click.option(
    "--timewindow",
    default=None,
    help='Time window, e.g. "01JAN2014 - 01JAN2015" (quoted on command line)',
)
@click.option(
    "--channel",
    multiple=True,
    help="Channel number to include (repeatable). Defaults to all channels.",
)
@click.option(
    "--channel-file",
    type=click.Path(exists=True),
    default=None,
    help="Text file with one channel number per line.",
)
@click.option(
    "--reservoir",
    multiple=True,
    help="Reservoir name to include (repeatable). Defaults to all reservoirs.",
)
@click.option(
    "--reservoir-file",
    type=click.Path(exists=True),
    default=None,
    help="Text file with one reservoir name per line.",
)
@click.option(
    "--unit",
    default="acre-feet",
    show_default=True,
    type=click.Choice(["cubic-feet", "acre-feet", "maf"], case_sensitive=False),
    help="Volume unit for output.",
)
@click.option(
    "--no-channels",
    is_flag=True,
    default=False,
    help="Skip channel volume calculation.",
)
@click.option(
    "--no-reservoirs",
    is_flag=True,
    default=False,
    help="Skip reservoir volume calculation.",
)
@click.option(
    "-o",
    "--output",
    default="volumes.csv",
    show_default=True,
    help="Output CSV file path.",
)
def calc_volumes_cmd(
    hydrofile, timewindow, channel, channel_file, reservoir, reservoir_file, unit, no_channels, no_reservoirs, output
):
    """Calculate DSM2 channel and/or reservoir volumes from a Hydro HDF5 tidefile.

    Volume is reported as a time series summed over the selected channels and
    reservoirs. Use --channel / --reservoir to restrict to specific items;
    omitting them includes everything in the file.

    \b
    Examples
    --------
    # All channels + reservoirs, monthly window, acre-feet (default unit)
    pydsm calc-volumes hist.h5 --timewindow "01JAN2014 - 01JAN2015"

    # Channel subset, MAF
    pydsm calc-volumes hist.h5 --channel 1 --channel 2 --unit maf -o chan_vols.csv

    # Reservoirs only
    pydsm calc-volumes hist.h5 --no-channels --unit maf -o res_vols.csv
    """
    unit = unit.lower()
    label = UNIT_LABELS[unit]

    chan_list = list(channel)
    if channel_file:
        with open(channel_file) as f:
            chan_list.extend(line.strip() for line in f if line.strip())

    res_list = list(reservoir)
    if reservoir_file:
        with open(reservoir_file) as f:
            res_list.extend(line.strip() for line in f if line.strip())

    hydro = HydroH5(hydrofile)

    result_parts = {}

    if not no_channels:
        try:
            chan_vols = get_channel_volumes(hydro, chan_list or None, timewindow, unit)
            result_parts["channel_volume"] = chan_vols.sum(axis=1)
            click.echo(
                f"Channels: {len(chan_vols.columns)} included, "
                f"mean total = {result_parts['channel_volume'].mean():.4g} {label}"
            )
        except Exception as e:
            raise click.ClickException(f"Failed to calculate channel volumes: {e}")

    if not no_reservoirs:
        try:
            res_vols = get_reservoir_volumes(hydro, res_list or None, timewindow, unit)
            result_parts["reservoir_volume"] = res_vols.sum(axis=1)
            click.echo(
                f"Reservoirs: {len(res_vols.columns)} included, "
                f"mean total = {result_parts['reservoir_volume'].mean():.4g} {label}"
            )
        except Exception as e:
            raise click.ClickException(f"Failed to calculate reservoir volumes: {e}")

    if not result_parts:
        raise click.ClickException("Nothing to calculate: both --no-channels and --no-reservoirs were set.")

    df_out = pd.DataFrame(result_parts)
    df_out["total_volume"] = df_out.sum(axis=1)
    df_out.index.name = "datetime"

    meta = {
        "command": " ".join(sys.argv),
        "hydrofile": hydrofile,
        "unit": label,
        "timewindow": timewindow or "all",
        "channels": ", ".join(chan_list) if chan_list else "all",
        "reservoirs": ", ".join(res_list) if res_list else ("none" if no_reservoirs else "all"),
    }
    write_csv_with_meta(output, df_out, meta)
    click.echo(f"Volumes ({label}) written to {output}")
