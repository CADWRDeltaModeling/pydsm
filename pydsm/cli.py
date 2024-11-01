import click
import sys

from pydsm.ptm_animator import ptm_animate
from pydsm.hydro_slicer import slice_hydro
from pydsm.postpro import load_location_file, load_location_table
from pydsm.functions import tsmath
from pydsm import dssutils
from pydsm import repeating_timeseries
from pydsm import create_cd_inp
from pydsm import extend_dss_ts


import pandas as pd
import pyhecdss as dss

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
def main():
    pass


@click.command()
@click.option(
    "-o",
    "--outfile",
    default="out.gz",
    help="path to output file (ends in .zip, .gz, .bz2 for compression), (.h5 for hdf5), (.dss for dss)",
)
@click.option(
    "--cpart", help="filter by cpart string match (e.g. EC for only loading EC)"
)
@click.option(
    "-godin",
    "--godin-filter",
    is_flag=True,
    default=False,
    help="apply godin filter before writing out",
)
@click.option(
    "-davg",
    "--daily-average",
    is_flag=True,
    default=False,
    help="average to daily values",
)
@click.option(
    "-dmax", "--daily-max", is_flag=True, default=False, help="maximum daily value"
)
@click.option(
    "-dmin", "--daily-min", is_flag=True, default=False, help="minimum daily value"
)
@click.option(
    "-mavg",
    "--monthly-average",
    is_flag=True,
    default=False,
    help="monthly average value",
)
@click.argument("dssfile", type=click.Path(exists=True))
def extract_dss(
    dssfile,
    outfile,
    cpart,
    godin_filter,
    daily_average,
    daily_max,
    daily_min,
    monthly_average,
):
    """
    Extract data from DSS file, optionally filtering it and writing to a pickle for quick future loads
    """
    dssutils.extract_dss(
        dssfile,
        outfile,
        cpart,
        godin_filter,
        daily_average,
        daily_max,
        daily_min,
        monthly_average,
    )


@click.command()
@click.option(
    "--cpart", help="filter by cpart string match (e.g. EC for only loading EC)"
)
@click.option(
    "--threshold", default=1e-3, help="Threshold to check for mean squared error"
)
@click.option(
    "--threshold-metric",
    default="rmse",
    type=click.Choice(
        [
            "mean_error",
            "nmean_error",
            "mse",
            "nmse",
            "rmse",
            "nrmse",
            "nash_sutcliffe",
            "percent_bias",
        ],
        case_sensitive=False,
    ),
)
@click.option(
    "--metricsfile",
    default="compare_dss_metrics_diff.csv",
    help="name of file to write out metrics differnce",
)
@click.option(
    "--time-window",
    default=None,
    help='ddMMMyyyy [HHmm] - ddMMMyyyy [HHmm], e.g. "01JAN1990 - 01OCT1991" (quoted on command line)',
)
@click.option(
    "--threshold-plots",
    default=False,
    type=click.BOOL,
    help="specify to output plots to html file for those matches that exceed the threshold metric",
)
@click.argument("dssfile1", type=click.Path(exists=True))
@click.argument("dssfile2", type=click.Path(exists=True))
def compare_dss(
    dssfile1,
    dssfile2,
    threshold=1e-3,
    threshold_metric="rmse",
    time_window=None,
    cpart=None,
    godin=False,
    metricsfile="compare_dss_metrics_diff.csv",
    threshold_plots=False,
):
    """
    Compares the dss files for common pathnames (B and C parts) and writes out various metrics to file
    Filtering for matching c parts
    and compare values with tolerance (default of 3 digits)
    """
    dssutils.compare_dss(
        dssfile1,
        dssfile2,
        threshold,
        threshold_metric,
        time_window,
        cpart,
        godin,
        metricsfile,
        threshold_plots,
    )


@click.command()
@click.argument("hydro_tidefile", type=click.Path(exists=True))
@click.argument("input_file", type=click.Path(exists=True))
@click.argument("tidefile_path", type=click.STRING, default="/hydro/input/channel")
@click.argument("table_name", type=click.STRING, default="CHANNEL")
def update_hydro_tidefile_with_inp(
    hydro_tidefile, input_file, tidefile_path, table_name
):
    from . import utils

    utils.update_hydro_tidefile_with_inp(
        hydro_tidefile, input_file, tidefile_path=tidefile_path, table_name=table_name
    )


# Command to create a repeating time series from a template year
@click.command(name="create")
@click.argument("datafile", type=click.Path(exists=True))
@click.option(
    "--input-file",
    type=click.Path(exists=True),
    required=True,
    help="Path to input CSV file",
)
@click.option(
    "--path", type=click.STRING, required=True, help="HECDSS path to write the data to"
)
@click.option("--units", type=click.STRING, required=True, help="Units for the data")
@click.option(
    "--period-type",
    type=click.STRING,
    required=False,
    default="PER-AVER",
    help="period type for the data",
)
def create_repeating(datafile, input_file, path, units, period_type="PER-AVER"):
    # Load the input CSV file
    df_template_year = pd.read_csv(
        input_file, parse_dates=["datetime"], index_col="datetime"
    )
    # Create the repeating time series
    df_repeating = repeating_timeseries.create_repeating_timeseries(
        df_template_year,
        start_year=df_template_year.index.year[0],
        end_year=pd.Timestamp.now().year,
    )
    # Write the data to the DSS file
    with dss.DSSFile(datafile, create_new=True) as dh:
        dh.write_rts(path, df_repeating, units, period_type)


# Command to extend a repeating time series to a given end year
@click.command(name="extend")
@click.argument("datafile", type=click.Path(exists=True))
@click.option(
    "--cpart", required=True, help="HECDSS cpart filter to the datasets in the DSS file"
)
@click.option(
    "--end-year", required=True, type=int, help="End year for the extended time series"
)
def extend_repeating(datafile, cpart, end_year):
    matches = dss.get_matching_ts(datafile, pathname=f"///{cpart}///")
    with dss.DSSFile(datafile, create_new=False) as dh:
        for df, u, t in matches:
            df_extended = repeating_timeseries.extend_repeating_timeseries(
                df, end_year=end_year
            )
            pathname = df.columns[0]
            dh.write_rts(pathname, df_extended, u, t)


# Command to create a DSM2 .inp file for a consumptive use DSS file
@click.command()
@click.argument(
    "dss_filename", type=click.Path(dir_okay=False, exists=True, readable=True)
)
@click.argument("dsm2_input_filename")
@click.argument("file_field_string")
def create_dsm2_input_for_cd(dss_filename, dsm2_input_filename, file_field_string):
    create_cd_inp.create_cd_inp(dss_filename, dsm2_input_filename, file_field_string)


# Create a group for the commands
@click.group(help="Commands to create and extend repeating time series")
def repeating():
    pass


@click.command()
@click.argument("from_file", type=click.Path(exists=True))
@click.argument("to_file", type=click.Path(exists=False))
def copy_all_dss(from_file, to_file):
    dssutils.copy_all(from_file, to_file)


@click.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.argument("output_file", type=click.Path(exists=False), required=False)
def pretty_print_input(input_file, output_file=None):
    from pydsm.input import read_input, pretty_print

    tables = read_input(input_file)
    if not output_file:
        fields = input_file.split(".inp")
        output_file = fields[0] + ".pretty.inp"
    append = False
    for table in tables:
        pretty_print(output_file, tables[table], tableName=table, append=append)
        append = True


# Add the commands to the group repeating
repeating.add_command(create_repeating)
repeating.add_command(extend_repeating)
# adding sub commands to main
main.add_command(repeating)
main.add_command(extract_dss)
main.add_command(compare_dss)
main.add_command(copy_all_dss)
main.add_command(ptm_animate)
main.add_command(slice_hydro)
main.add_command(update_hydro_tidefile_with_inp)
main.add_command(create_dsm2_input_for_cd)
main.add_command(pretty_print_input)
#
main.add_command(extend_dss_ts.extend_dss_ts)
if __name__ == "__main__":
    sys.exit(main())
