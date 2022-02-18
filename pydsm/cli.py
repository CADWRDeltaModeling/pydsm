import click
import sys

from pydsm.ptm_animator import ptm_animate
from pydsm.hydro_slicer import slice_hydro
from pydsm.postpro import load_location_file, load_location_table
from pydsm.functions import tsmath
from pydsm import dssutils

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

@click.group(context_settings=CONTEXT_SETTINGS)
def main():
    pass


@click.command()
@click.option("-o", "--outfile", default="out.gz", help="path to output file (ends in .zip, .gz, .bz2 for compression), (.h5 for hdf5), (.dss for dss)")
@click.option("--cpart", help="filter by cpart string match (e.g. EC for only loading EC)")
@click.option("-godin", "--godin-filter", is_flag=True, default=False, help="apply godin filter before writing out")
@click.option("-davg", "--daily-average", is_flag=True, default=False, help="average to daily values")
@click.option("-dmax", "--daily-max", is_flag=True, default=False, help="maximum daily value")
@click.option("-dmin", "--daily-min", is_flag=True, default=False, help="minimum daily value")
@click.option("-mavg", "--monthly-average", is_flag=True, default=False, help="monthly average value")
@click.argument("dssfile", type=click.Path(exists=True))
def extract_dss(dssfile, outfile, cpart, godin_filter, daily_average, daily_max, daily_min, monthly_average):
    '''
    Extract data from DSS file, optionally filtering it and writing to a pickle for quick future loads
    '''
    dssutils.extract_dss(dssfile, outfile, cpart, godin_filter, daily_average, daily_max, daily_min, monthly_average)

@click.command()
@click.option("--cpart", help="filter by cpart string match (e.g. EC for only loading EC)")
@click.option("--threshold", default=1e-3, help="Threshold to check for mean squared error")
@click.option('--threshold-metric', default='rmse',
              type=click.Choice(['mean_error', 'nmean_error', 'mse', 'nmse', 'rmse', 'nrmse', 'nash_sutcliffe', 'percent_bias'], case_sensitive=False))
@click.option("--metricsfile", default="compare_dss_metrics_diff.csv", help="name of file to write out metrics differnce")
@click.option("--time-window", default=None, help='ddMMMyyyy [HHmm] - ddMMMyyyy [HHmm], e.g. "01JAN1990 - 01OCT1991" (quoted on command line)')
@click.argument("dssfile1", type=click.Path(exists=True))
@click.argument("dssfile2", type=click.Path(exists=True))
def compare_dss(dssfile1, dssfile2, threshold=1e-3, threshold_metric='rmse', time_window=None, cpart=None, godin=False, metricsfile='compare_dss_metrics_diff.csv'):
    '''
    Compares the dss files for common pathnames (B and C parts) and writes out various metrics to file
    Filtering for matching c parts
    and compare values with tolerance (default of 3 digits)
    '''
    dssutils.compare_dss(dssfile1, dssfile2, threshold, threshold_metric, time_window, cpart, godin, metricsfile)

# adding sub commands to main
main.add_command(extract_dss)
main.add_command(compare_dss)
main.add_command(ptm_animate)
main.add_command(slice_hydro)
if __name__ == "__main__":
    sys.exit(main())
