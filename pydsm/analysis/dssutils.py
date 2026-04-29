# Utility functions for working with dss files
import sys
from time import time
import pyhecdss
from vtools.functions import filter
import pandas as pd
import numpy as np
import tqdm

from pydsm.output.hydro_slicer import slice_hydro
from .postpro import load_location_file, load_location_table
from pydsm.functions import tsmath

from . import lockutil

import hvplot.pandas
import holoviews as hv


def _restart_console_line():
    sys.stdout.write("\r")
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
    """
    builds column name based on /A/B/C/D/E/F/ DSS pathname and
    replacing the cpart with existing cpart + cpart_append value
    """

    def append_cpart(name):
        parts = name.split("/")
        parts[3] = parts[3] + cpart_append
        if epart_replace:
            parts[5] = epart_replace
        return "/".join(parts)

    return [append_cpart(name) for name in columns]


def _extract_processing(
    df, godin_filter, daily_average, daily_max, daily_min, monthly_average
):
    results = df
    results_monthly = None
    results_godin = None
    if godin_filter:
        results_godin = filter.godin(results)
    if daily_average:  # if godin filtered then replace that with daily averaged values
        tdf = tsmath.per_aver(df, "D")
        _to_periods(tdf)
        tdf.columns = _build_column(df.columns, "-MEAN", "1DAY")
        results = tdf
    if daily_max:
        tdf = tsmath.per_max(df, "D")
        _to_periods(tdf)
        tdf.columns = _build_column(df.columns, "-MAX", "1DAY")
        results = results.join(tdf, how="outer")
    if daily_min:
        tdf = tsmath.per_min(df, "D")
        _to_periods(tdf)
        tdf.columns = _build_column(df.columns, "-MIN", "1DAY")
        results = results.join(tdf, how="outer")
    if monthly_average:
        results_monthly = tsmath.per_aver(df, "M")
        _to_periods(results_monthly)
        results_monthly.columns = _build_column(df.columns, "-MONTHLY-AVG", "1MON")
    return results_godin, results, results_monthly


def _write_to_dss(od, rtg_daily, rtg_monthly, units, ptype="PER-VAL"):
    for i in range(len(rtg_daily.columns)):
        r = rtg_daily.iloc[:, i].to_frame()
        od.write_rts(r.columns[0], r, units, ptype)
    try:
        r = rtg_monthly.iloc[:, 0].to_frame()
        od.write_rts(r.columns[0], r, units, ptype)
    except Exception:
        pass


def _build_column(columns, cpart_append, epart_replace=None):
    """
    builds column name based on /A/B/C/D/E/F/ DSS pathname and
    replacing the cpart with existing cpart + cpart_append value
    """

    def append_cpart(name):
        parts = name.split("/")
        parts[3] = parts[3] + cpart_append
        if epart_replace:
            parts[5] = epart_replace
        return "/".join(parts)

    return [append_cpart(name) for name in columns]


def copy_all(from_file, to_file):
    """
    Copy all pathnames from one DSS file to another
    """
    with pyhecdss.DSSFile(to_file, create_new=True) as dhout:
        with pyhecdss.DSSFile(from_file) as dhin:
            plist = dhin.get_pathnames(dhin.read_catalog())
            for p in plist:
                df, units, period_type = dhin.read_rts(p)
                dhout.write_rts(p, df, units, period_type)


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
    pyhecdss.set_message_level(0)
    d = pyhecdss.DSSFile(dssfile)
    od = None
    if outfile.endswith("dss"):
        od = pyhecdss.DSSFile(outfile)
    catdf = d.read_catalog()
    catec = catdf[catdf.C == cpart]
    plist = d.get_pathnames(catec)
    if len(plist) == 0:
        print("No pathnames found in dssfile: %s for cpart=%s" % (dssfile, cpart))

    sys.stdout.write("@ %d / %d ==> Processing: %s" % (0, len(plist), plist[0]))
    r, u, p = d.read_rts(plist[0])
    results_daily, results_monthly = [], []
    rtg_godin, rtg_daily, rtg_monthly = _extract_processing(
        r, godin_filter, daily_average, daily_max, daily_min, monthly_average
    )
    if od:
        _write_to_dss(od, rtg_daily, rtg_monthly, u)
    else:
        results_daily.append(rtg_daily)
        results_monthly.append(rtg_monthly)

    for index, p in enumerate(plist, start=1):
        _restart_console_line()
        sys.stdout.write("@ %d / %d ==> Processing: %s" % (index, len(plist), p))
        r, u, p = d.read_rts(p)
        rtg_godin, rtg_daily, rtg_monthly = _extract_processing(
            r, godin_filter, daily_average, daily_max, daily_min, monthly_average
        )
        if od:
            _write_to_dss(od, rtg_daily, rtg_monthly, u)
        else:
            results_daily.append(rtg_daily)
            results_monthly.append(rtg_monthly)

    if od:
        print("Done writing to DSS: %s" % outfile)
        od.close()
    else:
        all_daily = pd.concat(results_daily, axis=1)
        if monthly_average:
            all_monthly = pd.concat(results_monthly, axis=1)
        if (
            outfile.endswith("zip")
            or outfile.endswith("bz2")
            or outfile.endswith("gzip")
        ):
            compression_opts = dict(
                method="infer", archive_name=outfile.split(".")[0] + ".csv"
            )
            all_daily.to_csv(outfile, compression=compression_opts)
            if monthly_average:
                compression_opts = dict(
                    method="infer", archive_name=outfile.split(".")[0] + "-monthly.csv"
                )
                all_monthly.to_csv(outfile, mode="a", compression=compression_opts)
        elif outfile.endswith(".h5"):
            all_daily.to_hdf(outfile, "daily")
            if monthly_average:
                all_monthly.to_hdf(outfile, "monthly")
        elif outfile.endswith("dss"):
            od.close()
        else:
            print("Unknown type of file ending: %s" % outfile)
            all_daily.to_pickle(outfile)
            if monthly_average:
                all_monthly.to_pickle(outfile)


def _read_ts(dssh, pathname, sdate, edate):
    """read regular or irregular time series as indicated by pathname E (5th) part"""
    if pathname.split("/")[5].startswith("IR-"):
        return dssh.read_its(pathname, sdate, edate)
    else:
        return dssh.read_rts(pathname, sdate, edate)


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
    pyhecdss.set_message_level(0)
    with pyhecdss.DSSFile(dssfile1) as d1, pyhecdss.DSSFile(dssfile2) as d2:
        dc1 = d1.read_catalog()
        dc2 = d2.read_catalog()
        if cpart != None:
            dc1 = dc1[dc1.C == cpart]
            dc2 = dc2[dc2.C == cpart]
        # common B and C
        cc = dc1.merge(dc2, on=["B", "C"])
        metrics = []
        sdate, edate = (None, None)
        metrics_names = [
            "mean_error",
            "nmean_error",
            "mse",
            "nmse",
            "rmse",
            "nrmse",
            "nash_sutcliffe",
            "percent_bias",
        ]
        if threshold_plots:
            plots_threshold_exceeds = []
        if time_window:
            sdate, edate = (f.strip() for f in time_window.split("-"))
        for index, row in cc.iterrows():
            rowid = "%s/%s" % (row.loc["B"], row.loc["C"])
            print("Comparing %s" % rowid)
            p1 = d1.get_pathnames(
                dc1[(dc1.B == row.loc["B"]) & (dc1.C == row.loc["C"])]
            )
            df1, u1, p1 = _read_ts(d1, p1[0], sdate, edate)
            p2 = d2.get_pathnames(
                dc2[(dc2.B == row.loc["B"]) & (dc2.C == row.loc["C"])]
            )
            df2, u2, p2 = _read_ts(d2, p2[0], sdate, edate)
            series1 = df1.iloc[:, 0]
            series2 = df2.iloc[:, 0]
            metrics.append(
                (
                    rowid,
                    tsmath.mean_error(series1, series2),
                    tsmath.nmean_error(series1, series2),
                    tsmath.mse(series1, series2),
                    tsmath.nmse(series1, series2),
                    tsmath.rmse(series1, series2),
                    tsmath.nrmse(series1, series2),
                    tsmath.nash_sutcliffe(series1, series2),
                    tsmath.percent_bias(series1, series2),
                )
            )
            if threshold_plots:
                threshold_metric_value = metrics[-1][
                    1 + metrics_names.index(threshold_metric)
                ]
                if threshold_metric_value > threshold:
                    plt = df1.hvplot(label=dssfile1) * df2.hvplot(label=dssfile2)
                    plt.opts(title=rowid)
                    plots_threshold_exceeds.append(plt)
        dfmetrics = pd.DataFrame.from_records(
            metrics,
            columns=[
                "name",
                "mean_error",
                "nmean_error",
                "mse",
                "nmse",
                "rmse",
                "nrmse",
                "nash_sutcliffe",
                "percent_bias",
            ],
        )
        # -- display missing or unmatched pathnames
        missingc1 = dc1[(~dc1.B.isin(cc.B)) & (~dc1.C.isin(cc.C))]
        missingc2 = dc2[(~dc2.B.isin(cc.B)) & (~dc2.C.isin(cc.C))]
        if not missingc1.empty:
            print("No matches for FILE1: %s" % dssfile1)
            print(missingc1)
        if not missingc2.empty:
            print("No matches for FILE2: %s" % dssfile2)
            print(missingc2)
        print(dfmetrics)
        print("Writing out metrics to file: ", metricsfile)
        dfmetrics.to_csv(metricsfile)
        if threshold_plots:
            print("Saving plots for threshold exceeds")
            if len(plots_threshold_exceeds) > 0:
                hvplot.save(
                    hv.Layout(plots_threshold_exceeds).cols(1),
                    "plots_threshold_exceeds.html",
                )
        threshold_cond = dfmetrics[threshold_metric.lower()] > threshold
        if threshold_cond.any():
            print(f"Threshold {threshold} exceeded! See exceeded rows below")
            print(dfmetrics[threshold_cond])


@lockutil.do_with_lock(lockfile="my.test.lock", timeout=20, check_interval=5)
def do_catalog_with_lock(dssfile):
    """
    Does catalog by first acquiring system wide lock on lockfile.

    This is because HEC-DSS version 6 uses a single temporary catalog file across any process that is attempting a catalog on a DSS File
    """
    with pyhecdss.DSSFile(dssfile) as dssh:
        dssh.do_catalog()


def csv_to_dss(
    csv_file,
    dss_file,
    index_col=0,
    apart="A",
    cpart="C",
    fpart="F",
    unit="UNK",
    period_type="INST-VAL",
    multiplier=1.0,
    resample_to="15min",
):
    """Convert a CSV file to a DSS file.

    Column names in the CSV header are used as the B-part of each DSS path.
    The index column (default 0) is parsed as the datetime index.
    D and E parts are inferred from the time series.
    """
    df = pd.read_csv(csv_file, index_col=index_col, parse_dates=True, comment='#')
    df = df * multiplier
    df = df.resample(resample_to).mean()
    with pyhecdss.DSSFile(dss_file, create_new=True) as f:
        for c in tqdm.tqdm(df.columns):
            ts = df[c].dropna()
            if ts.empty:
                print(f"Skipping {c!r} — all values are NaN")
                continue
            # dropna() drops the freq attribute; restore it so write_rts can
            # determine the E-part from the index frequency.
            if ts.index.freq is None and len(ts) > 1:
                inferred = pd.infer_freq(ts.index)
                if inferred is not None:
                    ts.index.freq = pd.tseries.frequencies.to_offset(inferred)
            pathname = f"/{apart}/{c}/{cpart}///{fpart}/"
            print("Writing to ", pathname)
            f.write_rts(pathname, ts, unit, period_type)
    print("Done")


def get_dss_data(
    primary_pathname_part_dss_filename_dict,
    primary_pathname_part,
    primary_part_c_part_dict=None,
    primary_part_e_part_dict=None,
    primary_part_f_part_dict=None,
    daily_avg=True,
    filter_b_part_numeric=False,
):
    """
    Read each dss time series from specified b part, c part, e part, and filename, and return a
    dataframe containing all time series as individual columns.

    primary_pathname_part (str):
        The 'primary pathname part' is the pathname part for which we will be extracting one or more
        time series. For example:
        1) if we want data for a specific list of stations, then 'b_part' will
           be the primary pathname part.
        2) If we want all div-flow, seep-flow, or drain-flow data, then 'c_part' will be the primary
           pathname part.
    primary_pathname_part_dss_filename_dict (dict): key=primary part value, value=DSS filename
    primary_part_c_part_dict (dict): key=primary part, value=c_part to use for filtering
    primary_part_e_part_dict (dict): key=primary part, value=e_part to use for filtering
    primary_part_f_part_dict (dict): key=primary part, value=f_part to use for filtering
    daily_avg (bool, optional): if true and data are not daily, only daily averaged data will be returned
    filter_b_part_numeric (bool, optional): if true, remove any columns for which b part in dss path header is not numeric
    """
    print("==============================================================")
    return_df = None
    for pp in primary_pathname_part_dss_filename_dict:
        dss_filename = primary_pathname_part_dss_filename_dict[pp]
        b_part = None
        c_part = None
        e_part = None
        f_part = None
        if primary_pathname_part == "b_part":
            b_part = pp
            c_part = (
                primary_part_c_part_dict[pp]
                if primary_part_c_part_dict is not None
                else None
            )
            e_part = (
                primary_part_e_part_dict[pp]
                if primary_part_e_part_dict is not None
                else None
            )
            f_part = (
                primary_part_f_part_dict[pp]
                if (
                    primary_part_f_part_dict is not None
                    and b_part in primary_part_f_part_dict
                )
                else None
            )
            print(
                "bcef="
                + str(b_part)
                + ","
                + str(c_part)
                + ","
                + str(e_part)
                + ","
                + str(f_part)
            )
        elif primary_pathname_part == "c_part":
            c_part = pp
        else:
            raise ValueError(
                f"primary_pathname_part must be 'b_part' or 'c_part', got: {primary_pathname_part!r}"
            )

        with pyhecdss.DSSFile(dss_filename) as d:
            catdf = d.read_catalog()
            filtered_df = None
            if b_part is not None:
                filtered_df = (
                    filtered_df[catdf.B == b_part]
                    if filtered_df is not None
                    else catdf[catdf.B == b_part]
                )
            if c_part is not None:
                filtered_df = (
                    filtered_df[catdf.C == c_part]
                    if filtered_df is not None
                    else catdf[catdf.C == c_part]
                )
            if e_part is not None:
                filtered_df = (
                    filtered_df[catdf.E == e_part]
                    if filtered_df is not None
                    else catdf[catdf.E == e_part]
                )
            if f_part is not None:
                filtered_df = (
                    filtered_df[catdf.F == f_part]
                    if filtered_df is not None
                    else catdf[catdf.F == f_part]
                )
            if filter_b_part_numeric:
                filtered_df = filtered_df[catdf.B.str.isnumeric()]
            path_list = d.get_pathnames(filtered_df)
            for p in path_list:
                df = None
                if d.parse_pathname_epart(p).startswith("IR-"):
                    df, units, ptype = d.read_its(p)
                else:
                    df, units, ptype = d.read_rts(p)
                time_interval_str = p.split("/")[5]
                if daily_avg and "1DAY" not in time_interval_str:
                    print("daily averaging")
                    df = tsmath.per_aver(df, "1D")
                if isinstance(df.index, pd.core.indexes.datetimes.DatetimeIndex):
                    df.index = df.index.to_period()
                if primary_pathname_part == "b_part":
                    df.columns = [pp]
                print("path=" + p)
                return_df = (
                    df
                    if return_df is None
                    else pd.merge(
                        return_df, df, how="left", left_index=True, right_index=True
                    )
                )
        print("==============================================================")

    return return_df
