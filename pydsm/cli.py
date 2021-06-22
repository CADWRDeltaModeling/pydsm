import sys
from time import time
import click
import pyhecdss
from vtools.functions import filter
import pandas as pd
import numpy as np

from pydsm.ptm_animator import ptm_animate
from pydsm.hydro_slicer import slice_hydro
from pydsm.postpro import load_location_file, load_location_table
from pydsm.functions import tsmath


@click.group()
def main():
    pass

def _build_column(columns, cpart_append, epart_replace=None):
    '''
    builds column name based on /A/B/C/D/E/F/ DSS pathname and
    replacing the cpart with existing cpart + cpart_append value
    '''
    def append_cpart(name):
        parts = name.split('/')
        parts[3] = parts[3]+cpart_append
        if epart_replace:
            parts[5] = epart_replace
        return '/'.join(parts)
    return [append_cpart(name) for name in columns]


def _restart_console_line():
    sys.stdout.write('\r')
    sys.stdout.flush()


def _extract_processing(df, godin_filter, daily_average, daily_max, daily_min, monthly_average):
    results = df
    results_monthly = None
    if godin_filter:
        results = filter.godin_filter(results)
    if daily_average:  # if godin filtered then replace that with daily averaged values
        tdf = results.resample('1D', closed='right', label='right').mean()
        tdf.columns = _build_column(df.columns, '-MEAN', '1DAY')
        results = tdf
    if daily_max:
        tdf = df.resample('1D', closed='right', label='right').max()
        tdf.columns = _build_column(df.columns, '-MAX', '1DAY')
        results = results.join(tdf, how='outer')
    if daily_min:
        tdf = df.resample('1D', closed='right', label='right').min()
        tdf.columns = _build_column(df.columns, '-MIN', '1DAY')
        results = results.join(tdf, how='outer')
    if monthly_average:
        results_monthly = df.resample('M', closed='right', label='right').mean()
        results_monthly.columns = _build_column(df.columns, '-MONTHLY-AVG', '1MON')
    return results, results_monthly


def _write_to_dss(od, rtg_daily, rtg_monthly, units, ptype='PER-VAL'):
    for i in range(len(rtg_daily.columns)):
        r = rtg_daily.iloc[:, i].to_frame()
        od.write_rts(r.columns[0], r, units, ptype)
    try:
        r = rtg_monthly.iloc[:, 0].to_frame()
        od.write_rts(r.columns[0], r, units, ptype)
    except Exception:
        pass


def _build_column(columns, cpart_append, epart_replace=None):
    '''
    builds column name based on /A/B/C/D/E/F/ DSS pathname and
    replacing the cpart with existing cpart + cpart_append value
    '''
    def append_cpart(name):
        parts = name.split('/')
        parts[3] = parts[3]+cpart_append
        if epart_replace:
            parts[5] = epart_replace
        return '/'.join(parts)
    return [append_cpart(name) for name in columns]


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
    pyhecdss.set_message_level(0)
    d = pyhecdss.DSSFile(dssfile)
    od = None
    if outfile.endswith('dss'):
        od = pyhecdss.DSSFile(outfile)
    catdf = d.read_catalog()
    catec = catdf[catdf.C == cpart]
    plist = d.get_pathnames(catec)
    if len(plist) == 0:
        print("No pathnames found in dssfile: %s for cpart=%s" %
              (dssfile, cpart))

    sys.stdout.write('@ %d / %d ==> Processing: %s' % (0, len(plist), plist[0]))
    r, u, p = d.read_rts(plist[0])
    results_daily, results_monthly = [], []
    rtg_daily, rtg_monthly = _extract_processing(
        r, godin_filter, daily_average, daily_max, daily_min, monthly_average)
    if od:
        _write_to_dss(od, rtg_daily, rtg_monthly, u)
    else:
        results_daily.append(rtg_daily)
        results_monthly.append(rtg_monthly)

    for index, p in enumerate(plist, start=1):
        _restart_console_line()
        sys.stdout.write('@ %d / %d ==> Processing: %s' % (index, len(plist), p))
        r, u, p = d.read_rts(p)
        rtg_daily, rtg_monthly = _extract_processing(
            r, godin_filter, daily_average, daily_max, daily_min, monthly_average)
        if od:
            _write_to_dss(od, rtg_daily, rtg_monthly, u)
        else:
            results_daily.append(rtg_daily)
            results_monthly.append(rtg_monthly)

    if od:
        print('Done writing to DSS: %s' % outfile)
        od.close()
    else:
        all_daily = pd.concat(results_daily, axis=1)
        all_monthly = pd.concat(results_monthly, axis=1)
        if outfile.endswith('zip') or outfile.endswith('bz2') or outfile.endswith('gzip'):
            all_daily.to_csv(outfile)
            all_monthly.to_csv(outfile)
        elif outfile.endswith('.h5'):
            all_daily.to_hdf(outfile, 'daily')
            all_monthly.to_hdf(outfile, 'monthly')
        elif outfile.endswith('dss'):
            od.close()
        else:
            print('Unknown type of file ending: %s' % outfile)
            all_daily.to_pickle(outfile)
            all_monthly.to_pickle(outfile)


@click.command()
@click.option("--cpart", help="filter by cpart string match (e.g. EC for only loading EC)")
@click.option("--threshold", default=1e-3, help="Threshold to check for mean squared error")
@click.option('--threshold-metric', default='rmse',
              type=click.Choice(['mean_error', 'mse', 'rmse', 'nash_sutcliffe', 'percent_bias'], case_sensitive=False))
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
    pyhecdss.set_message_level(0)
    with pyhecdss.DSSFile(dssfile1) as d1, pyhecdss.DSSFile(dssfile2) as d2:
        dc1 = d1.read_catalog()
        dc2 = d2.read_catalog()
        if cpart != None:
            dc1 = dc1[dc1.C == cpart]
            dc2 = dc2[dc2.C == cpart]
        # common B and C
        cc = dc1.merge(dc2, on=['B', 'C'])
        metrics = []
        sdate, edate = (None, None)
        if time_window:
            sdate, edate = (f.strip() for f in time_window.split("-"))
        for index, row in cc.iterrows():
            rowid = '%s/%s' % (row.loc['B'], row.loc['C'])
            print('Comparing %s' % rowid)
            p1 = d1.get_pathnames(dc1[(dc1.B == row.loc['B']) & (dc1.C == row.loc['C'])])
            df1, u1, p1 = d1.read_rts(p1[0], sdate, edate)
            p2 = d2.get_pathnames(dc2[(dc2.B == row.loc['B']) & (dc2.C == row.loc['C'])])
            df2, u2, p2 = d2.read_rts(p2[0], sdate, edate)
            series1 = df1.iloc[:, 0]
            series2 = df2.iloc[:, 0]
            metrics.append((rowid, tsmath.mean_error(series1, series2),
                            tsmath.mse(series1, series2),
                            tsmath.rmse(series1, series2),
                            tsmath.nash_sutcliffe(series1, series2),
                            tsmath.percent_bias(series1, series2)))
        dfmetrics = pd.DataFrame.from_records(
            metrics, columns=['name', 'mean_error', 'mse', 'rmse', 'nash_sutcliffe', 'percent_bias'])
        # -- display missing or unmatched pathnames
        missingc1 = dc1[(~dc1.B.isin(cc.B)) & (~dc1.C.isin(cc.C))]
        missingc2 = dc2[(~dc2.B.isin(cc.B)) & (~dc2.C.isin(cc.C))]
        if not missingc1.empty:
            print('No matches for FILE1: %s' % dssfile1)
            print(missingc1)
        if not missingc2.empty:
            print('No matches for FILE2: %s' % dssfile2)
            print(missingc2)
        print(dfmetrics)
        print('Writing out metrics to file: ', metricsfile)
        dfmetrics.to_csv(metricsfile)
        threshold_cond = dfmetrics[threshold_metric.lower()] > threshold
        if threshold_cond.any():
            print(f'Threshold {threshold} exceeded! See exceeded rows below')
            print(dfmetrics[threshold_cond])


# adding sub commands to main
main.add_command(extract_dss)
main.add_command(compare_dss)
main.add_command(ptm_animate)
main.add_command(slice_hydro)
if __name__ == "__main__":
    sys.exit(main())
