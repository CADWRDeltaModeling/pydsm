# Utility functions for working with dss files
import sys
from time import time
import pyhecdss
from vtools.functions import filter
import pandas as pd
import numpy as np

from pydsm.hydro_slicer import slice_hydro
from pydsm.postpro import load_location_file, load_location_table
from pydsm.functions import tsmath

from pydsm import lockutil

import hvplot.pandas
import holoviews as hv

def _restart_console_line():
    sys.stdout.write('\r')
    sys.stdout.flush()


def _to_periods(df):
    try:
        df.index = df.index.to_period()
    except:
        pass



def _to_periods(df):
    try:
        df.index = df.index.to_period()
    except:
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

def _extract_processing(df, godin_filter, daily_average, daily_max, daily_min, monthly_average):
    results = df
    results_monthly = None
    results_godin = None
    if godin_filter:
        results_godin = filter.godin(results)
    if daily_average:  # if godin filtered then replace that with daily averaged values
        tdf = tsmath.per_aver(df, 'D')
        _to_periods(tdf)
        tdf.columns = _build_column(df.columns, '-MEAN', '1DAY')
        results = tdf
    if daily_max:
        tdf = tsmath.per_max(df, 'D')
        _to_periods(tdf)
        tdf.columns = _build_column(df.columns, '-MAX', '1DAY')
        results = results.join(tdf, how='outer')
    if daily_min:
        tdf = tsmath.per_min(df, 'D')
        _to_periods(tdf)
        tdf.columns = _build_column(df.columns, '-MIN', '1DAY')
        results = results.join(tdf, how='outer')
    if monthly_average:
        results_monthly = tsmath.per_aver(df, 'M')
        _to_periods(results_monthly)
        results_monthly.columns = _build_column(df.columns, '-MONTHLY-AVG', '1MON')
    return results_godin, results, results_monthly


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

def copy_all(from_file,to_file):
    '''
    Copy all pathnames from one DSS file to another
    '''
    with pyhecdss.DSSFile(to_file, create_new=True) as dhout:
        with pyhecdss.DSSFile(from_file) as dhin:
            plist=dhin.get_pathnames(dhin.read_catalog())
            for p in plist:
                df,units,period_type=dhin.read_rts(p)
                dhout.write_rts(p,df,units,period_type)

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
    rtg_godin, rtg_daily, rtg_monthly = _extract_processing(
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
        rtg_godin, rtg_daily, rtg_monthly = _extract_processing(
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
        if monthly_average:
            all_monthly = pd.concat(results_monthly, axis=1)
        if outfile.endswith('zip') or outfile.endswith('bz2') or outfile.endswith('gzip'):
            compression_opts = dict(method='infer', archive_name=outfile.split('.')[0]+'.csv')
            all_daily.to_csv(outfile, compression=compression_opts)
            if monthly_average:
                compression_opts = dict(
                    method='infer', archive_name=outfile.split('.')[0]+'-monthly.csv')
                all_monthly.to_csv(outfile, mode='a', compression=compression_opts)
        elif outfile.endswith('.h5'):
            all_daily.to_hdf(outfile, 'daily')
            if monthly_average:
                all_monthly.to_hdf(outfile, 'monthly')
        elif outfile.endswith('dss'):
            od.close()
        else:
            print('Unknown type of file ending: %s' % outfile)
            all_daily.to_pickle(outfile)
            if monthly_average:
                all_monthly.to_pickle(outfile)

def _read_ts(dssh, pathname, sdate, edate):
    ''' read regular or irregular time series as indicated by pathname E (5th) part '''
    if pathname.split('/')[5].startswith('IR-'):
        return dssh.read_its(pathname, sdate, edate)
    else:
        return dssh.read_rts(pathname, sdate, edate)


def compare_dss(dssfile1, dssfile2, threshold=1e-3, threshold_metric='rmse', 
    time_window=None, cpart=None, godin=False, 
    metricsfile='compare_dss_metrics_diff.csv',
    threshold_plots=False):
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
        metrics_names = ['mean_error', 'nmean_error', 'mse', 'nmse', 'rmse', 'nrmse', 'nash_sutcliffe', 'percent_bias']
        if threshold_plots:
            plots_threshold_exceeds = []
        if time_window:
            sdate, edate = (f.strip() for f in time_window.split("-"))
        for index, row in cc.iterrows():
            rowid = '%s/%s' % (row.loc['B'], row.loc['C'])
            print('Comparing %s' % rowid)
            p1 = d1.get_pathnames(dc1[(dc1.B == row.loc['B']) & (dc1.C == row.loc['C'])])
            df1, u1, p1 = _read_ts(d1, p1[0], sdate, edate)
            p2 = d2.get_pathnames(dc2[(dc2.B == row.loc['B']) & (dc2.C == row.loc['C'])])
            df2, u2, p2 = _read_ts(d2, p2[0], sdate, edate)
            series1 = df1.iloc[:, 0]
            series2 = df2.iloc[:, 0]
            metrics.append((rowid, tsmath.mean_error(series1, series2), tsmath.nmean_error(series1, series2),
                            tsmath.mse(series1, series2), tsmath.nmse(series1, series2),
                            tsmath.rmse(series1, series2), tsmath.nrmse(series1, series2),
                            tsmath.nash_sutcliffe(series1, series2),
                            tsmath.percent_bias(series1, series2)))
            if threshold_plots:
                threshold_metric_value = metrics[-1][1+metrics_names.index(threshold_metric)]
                if threshold_metric_value > threshold:
                    plt = df1.hvplot(label=dssfile1)*df2.hvplot(label=dssfile2)
                    plt.opts(title=rowid)
                    plots_threshold_exceeds.append(plt)
        dfmetrics = pd.DataFrame.from_records(
            metrics, columns=['name', 'mean_error', 'nmean_error', 'mse', 'nmse', 'rmse', 'nrmse', 'nash_sutcliffe', 'percent_bias'])
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
        if threshold_plots:
            print('Saving plots for threshold exceeds')
            if len(plots_threshold_exceeds) > 0:
                hvplot.save(hv.Layout(plots_threshold_exceeds).cols(1), 'plots_threshold_exceeds.html')
        threshold_cond = dfmetrics[threshold_metric.lower()] > threshold
        if threshold_cond.any():
            print(f'Threshold {threshold} exceeded! See exceeded rows below')
            print(dfmetrics[threshold_cond])


@lockutil.do_with_lock(lockfile='my.test.lock', timeout=20, check_interval=5)
def do_catalog_with_lock(dssfile):
    '''
    Does catalog by first acquiring system wide lock on lockfile. 
    
    This is because HEC-DSS version 6 uses a single temporary catalog file across any process that is attempting a catalog on a DSS File
    '''
    with pyhecdss.DSSFile(dssfile) as dssh:
        dssh.do_catalog()
