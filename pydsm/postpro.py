"""
This module is to postprocess data for use in visualization and in calculating metrics

"""

import click
import collections
import contextlib
import logging
import sys

import numpy as np
import pandas as pd
import scipy.stats as stats

import pyhecdss
from pandas.core.frame import DataFrame
from vtools.functions.filter import godin
from pydsm.functions import tidalhl
import os
from os.path import exists
import diskcache

Study = collections.namedtuple("Study", ["name", "dssfile"])
# time_window_exclusion_list is a comma separated list of timewindows
# threshold_value is a value, above and below which data will be removed, to create plots/metrics
Location = collections.namedtuple(
    "Location",
    ["name", "bpart", "description", "time_window_exclusion_list", "threshold_value"],
)
VarType = collections.namedtuple("VarType", ["name", "units"])


def convert_index_to_timestamps(df):
    if isinstance(df.index, pd.core.indexes.datetimes.DatetimeIndex):
        return
    df.index = df.index.astype("datetime64[ns]")
    df.index.freq = pd.infer_freq(df.index)  # if possible


def resample_with_interpolation(
    df: DataFrame, time_interval="15min", interpolation_method="linear"
) -> DataFrame:
    """Resampling to the time_interval and using the interpolation method

    Args:
        df: DataFrame with time index
        time_interval (str, optional): [time interval for resampling]. Defaults to '15min'.
        interpolation_method (str, optional): [interpolation method for resampling]. Defaults to 'linear'.

    Returns:
        DataFrame: merged data frame in order of listing (indexed by time) and regularly sampled at time_interval
    """
    return df.resample(time_interval).interpolate(method=interpolation_method)


def merge(df_list: list) -> DataFrame:
    result = df_list[0]
    convert_index_to_timestamps(result)  # inplace change of index
    for df in df_list[1:]:
        convert_index_to_timestamps(df)  # inplace change of index
        df.columns = result.columns  # Assume the same column names for merging
        result = result.combine_first(df)
    return result


def calc_number_nan_filled(df: DataFrame, filled_df: DataFrame) -> int:
    """
    calculates number of NaN values filled between df and filled_df

    Args:
        df (DataFrame): dataframe
        filled_df (DataFrame): dataframe after filling with some method

    Returns:
         int: number of nans filled = number of NaNs {before - after}
    """
    num_nans_before_filling = df.isna().sum()[0]
    num_nans_after_filling = filled_df.isna().sum()[0]
    return num_nans_before_filling - num_nans_after_filling


def fill_in(df: DataFrame, max_fillin_gap: int, fillin_method: str) -> DataFrame:
    """fill in DataFrame

    Args:
        df (DataFrame): A dataframe with a time index of a frequency
        max_fillin_gap (int): Maximum consecutive values to fill
        fillin_method (str): Fill in method,e.g. linear

    Returns:
        DataFrame: A filled dataframe
    """
    if max_fillin_gap <= 0:
        raise "Invalid argument max_fillin_gap should be greater than zero"
    return df.interpolate(method=fillin_method, axis=0, limit=max_fillin_gap)


def get_dirname(fname):
    return os.path.dirname(os.path.normpath(fname))


def get_filename(fname):
    return os.path.basename(os.path.normpath(fname))


def get_cache_dir(fname):
    return os.path.join(get_dirname(fname), ".cache-postpro-" + get_filename(fname))


# ------------ METRICS CALCULATOR FUNCTIONS ---------#


# ----------- POST PROCESS and CACHE RESULTS --------#
# Cache original time series, godin filtered, tidal high, low and amplitude calculations and differences in phase with observed time series.


class PostProCache:
    """Post process cache: For loading and storing using diskcache"""

    IRR_E_PART = "IR-MONTH"  # The epart to use for irregular time series

    def __init__(self, fname):
        """Cache filename based on input DSS filename (i.e. ends in .dss)"""
        self.fname = fname
        self.cache = diskcache.Cache(get_cache_dir(fname), size_limit=1e11)

    def clear(self):
        self.cache.clear()

    def is_rts(df):
        return hasattr(df.index, "freqstr") and df.index.freqstr is not None

    def get_cache_key(self, bpart, cpart, epart):
        return f"/{bpart.upper()}/{cpart.upper()}/{epart.upper()}/"

    def store(self, df, units, bpart, cpart, epart):
        if type(df) is str:
            key = self.get_cache_key(bpart, cpart, epart)
            self.cache[key] = (df, units.upper(), "ERROR")
        else:
            if df.empty:
                return
            key = self.get_cache_key(bpart, cpart, epart)
            self.cache[key] = (df, units.upper(), "INST-VAL")

    def load(self, bpart, cpart, epart):
        # logging.info('postpro.PostProCache.load: bpart, cpart, epart, fpart='+bpart+','+cpart+','+epart+','+fpart)
        df = None
        units = ""
        ptype = ""
        key = self.get_cache_key(bpart, cpart, epart)
        if key in self.cache:
            df, units, ptype = self.cache[key]
        return df, units, ptype


class PostProcessor:
    """Post processor contains the operations to be applied to the input HEC-DSS file"""

    TIME_INTERVAL = "15min"
    CPART_SUFFIX_MAP = {}

    def __init__(
        self,
        study: Study,
        location: Location,
        vartype: VarType,
        subtract=False,
        ratio=False,
    ):
        self.study = study
        self.location = location
        self.vartype = vartype
        self.cache = PostProCache(self.study.dssfile)
        self.subtract = subtract
        self.ratio = ratio
        self._set_default_ops()

    def _set_default_ops(self):
        self.do_resampling_with_merging = False
        self.do_filling_in = False
        self.do_scaling = False

    def do_resample_with_merge(self, time_interval):
        self.do_resampling_with_merging = True
        self.resampling_interval = time_interval

    def do_fill_in(self, max_fillin_gap=3, fillin_method="linear"):
        self.do_filling_in = True
        self.max_fillin_gap = max_fillin_gap
        self.fillin_method = fillin_method

    def do_scale(self, scale):
        self.do_scaling = True
        self.scale = scale

    def append_dss_path_part(self, pathname, path_part_appendage, path_part="b_part"):
        parts = pathname.split("/")
        parts.pop()
        parts.pop(0)
        if path_part != "d_part":
            dss_parts_list = [
                "a_part",
                "b_part",
                "c_part",
                "d_part",
                "e_part",
                "f_part",
            ]
            i = dss_parts_list.index(path_part)
            parts[i] += path_part_appendage
        return "/%s/%s/%s/%s/%s/%s/" % tuple(parts)

    def get_part_from_dss_path(self, pathname, path_part="b_part"):
        parts = pathname.split("/")
        dss_parts_list = ["a_part", "b_part", "c_part", "d_part", "e_part", "f_part"]
        i = dss_parts_list.index(path_part)
        return parts[i]

    def _read_ts(self):
        """
        Reads data from DSS, and returns a time series dataframe.
        Sometimes we need to subtract two flow time series. For example, cross-delta flow=RSAC128-RSAC123 and SDC-GES. \
        If data type is flow, and if pathname not found in file, check to see if there is a minus sign. \
        if there is, split the b part on the minus sign, and see if pathnames with each part exist. \
        if yes, then subtract the two time series and return the result. 
        returns dataframe or the error message if the file is not found.
        """
        if not os.path.exists(self.study.dssfile) and not os.path.isfile(
            self.study.dssfile
        ):
            error_message = (
                "Error in postpro._read_ts: DSS file not found: " + self.study.dssfile
            )
            return error_message
        pathname = "//%s/%s////" % (self.location.bpart, self.vartype.name)
        return_df = None
        if not self.subtract and not self.ratio:
            generator = pyhecdss.get_ts(self.study.dssfile, pathname.upper())
            with contextlib.closing(generator) as dfgen:
                if self.do_resampling_with_merging:
                    dflist = [
                        df.data.resample(PostProcessor.TIME_INTERVAL).asfreq()
                        for df in dfgen
                    ]
                    if len(dflist) <= 0:
                        error_message = (
                            "Error in postpro._read_ts: dflist has len 0, after trying to read pathname: "
                            + pathname.upper()
                        )
                        return error_message
                    else:
                        return_df = merge(dflist)
                else:
                    return_df = next(dfgen).data
                    convert_index_to_timestamps(return_df)  # inplace change of index
        elif self.subtract or self.ratio:
            # read in >1 time series, and subtract them. Column names of dataframes are set to 'data' because when
            # subtracting, column names must match. new_dss_path is used for the column name of the result, and will typically have
            # a bpart that is 'SDC-GES' or 'RSAC128-RSAC123'
            return_df = None
            df_index = 0
            bpart_separator = None
            if "_div_" in self.location.bpart:
                bpart_separator = "_div_"
            elif "-" in self.location.bpart:
                bpart_separator = "-"
            if (self.vartype.name.lower() == "flow") and (
                bpart_separator in self.location.bpart
            ):
                parts = pathname.upper().split("/")
                bpart_parts = parts[2].split(bpart_separator.upper())

                new_dss_path = None
                for bp in bpart_parts:
                    pathname = "//%s/%s////" % (bp, self.vartype.name)
                    generator = pyhecdss.get_ts(self.study.dssfile, pathname.upper())
                    with contextlib.closing(generator) as dfgen:
                        dflist = [
                            df.data.resample(PostProcessor.TIME_INTERVAL).asfreq()
                            for df in dfgen
                        ]
                        if df_index == 0:
                            return_df = merge(dflist)
                            new_dss_path = return_df.columns[0]
                            return_df.columns = ["data"]
                        else:
                            return_df.columns = ["data"]
                            new_df = merge(dflist)
                            new_dss_path = self.append_dss_path_part(
                                new_dss_path,
                                bpart_separator
                                + self.get_part_from_dss_path(new_df.columns[0]),
                            )
                            new_df.columns = ["data"]
                            if self.subtract:
                                return_df = return_df.subtract(new_df)
                            elif self.ratio:
                                error_message = "Error in postpro._read_ts: ratio not implemented yet"
                                return error_message
                    df_index += 1
                return_df.columns = [new_dss_path]
        return return_df

    def process(self):
        df = self._read_ts()
        if isinstance(df, str):
            logging.info("Error in postpro.process: " + df)
            self.error_message = df
            return
        if df is None:
            return
        if self.do_filling_in:
            df = fill_in(df, self.max_fillin_gap, self.fillin_method)

        if self.do_scaling:
            df = df.mul(self.scale)

        self.df = df

        self.gdf = godin(self.df)
        self.high, self.low = tidalhl.get_tidal_hl_rolling(self.df)
        self.amp = tidalhl.get_tidal_amplitude(self.high, self.low)

    def clear_refs(self):
        self.df = self.gdf = self.high = self.low = self.amp = None

    def _store(self, df, cpart_suffix="", epart=TIME_INTERVAL):
        self.cache.store(
            df,
            self.vartype.units,
            self.location.name,
            self.vartype.name + cpart_suffix,
            epart,
        )

    def _load(self, cpart_suffix="", epart=TIME_INTERVAL, timewindow=""):
        return_series = None
        try:
            series, _, _ = self.cache.load(
                self.location.name,
                self.vartype.name + cpart_suffix,
                epart,
            )
            if isinstance(series, str):
                return_series = series
            elif series is not None:
                if timewindow != "":
                    start, end = timewindow.split("-")
                    start = pd.Timestamp(start)
                    end = pd.Timestamp(end)
                    return_series = series.loc[start:end]
            else:
                return_series = series
        except StopIteration as e:
            logging.exception("pydsm.postpro.PostProCache.load: no data found")
        return return_series

    def __repr__(self):
        return "PostProCache(location=%s, vartype=%s, study=%s, cache=%s)" % (
            self.location,
            self.vartype,
            self.study,
            self.cache,
        )

    def __str__(self):
        return repr(self)

    def store_processed(self):
        if not hasattr(self, "df"):
            self._store(self.error_message, "-ERROR")
            logging.warning(
                "pydsm.postpro.PostProCache.store_processed: no data to store: "
                + str(self)
            )
            return True
        else:
            self._store("", "-ERROR")
        self._store(self.df)
        self._store(self.gdf, "-GODIN")
        self._store(self.high, "-HIGH", PostProCache.IRR_E_PART)
        self._store(self.low, "-LOW", PostProCache.IRR_E_PART)
        self._store(self.amp, "-AMP", PostProCache.IRR_E_PART)
        return True

    def load_processed(self, timewindow="", invert_series=False):
        """
        invert_series (bool): if true, all data will be multiplied by -1. This is needed
          when observed data and model use opposite sign conventions.
        """
        self.error_message = self._load("-ERROR")
        if self.error_message is None:
            return False  # legacy to load older cache data, FIXME: remove this later
        if len(self.error_message) > 0:
            return True
        self.df = self._load(cpart_suffix="", timewindow=timewindow)
        self.gdf = self._load(cpart_suffix="-GODIN", timewindow=timewindow)
        self.high = self._load(
            cpart_suffix="-HIGH", epart=PostProCache.IRR_E_PART, timewindow=timewindow
        )
        self.low = self._load(
            cpart_suffix="-LOW", epart=PostProCache.IRR_E_PART, timewindow=timewindow
        )
        self.amp = self._load(
            cpart_suffix="-AMP", epart=PostProCache.IRR_E_PART, timewindow=timewindow
        )
        success = False
        if (
            self.df is not None
            and self.gdf is not None
            and self.high is not None
            and self.low is not None
            and self.amp is not None
            and len(self.df) > 0
            and len(self.gdf) > 0
            and len(self.high) > 0
            and len(self.low) > 0
            and len(self.amp) > 0
        ):
            success = True
            if invert_series:
                self.df = -self.df
                self.gdf = -self.gdf
                self.high = -self.high
                self.low = -self.low
                self.amp = -self.amp

        return success

    def process_diff(self, other):
        if self.amp is None or other.amp is None:
            self.amp_diff = None
            self.amp_diff_pct = None
        else:
            self.amp_diff = tidalhl.get_tidal_amplitude_diff(self.amp, other.amp)
            self.amp_diff_pct = tidalhl.get_tidal_amplitude_diff(
                self.amp, other.amp, percent_diff=True
            )
            self.phase_diff = tidalhl.get_tidal_phase_diff(
                self.high, self.low, other.high, other.low
            )

    def store_diff(self):
        self._store(self.amp_diff, "-AMP-DIFF", PostProCache.IRR_E_PART)
        self._store(self.amp_diff_pct, "-AMP-DIFF-PCT", PostProCache.IRR_E_PART)
        self._store(self.phase_diff, "-PHASE-DIFF", PostProCache.IRR_E_PART)

    def load_diff(self, timewindow=""):
        self.amp_diff = self._load("-AMP-DIFF", PostProCache.IRR_E_PART, timewindow)
        self.amp_diff_pct = self._load(
            "-AMP-DIFF-PCT", PostProCache.IRR_E_PART, timewindow
        )
        self.phase_diff = self._load("-PHASE-DIFF", PostProCache.IRR_E_PART, timewindow)


def load_location_table(loc_name_file: str):
    """Loads locations from the table.

    dsm2_id,obs_station_id,station_name,Elevation,lat,lon,...
    SSS,SSS,Steamboat Slough,10,38.285,-121.587,...

    Args:
        loc_name_file (str): Path to file as string

    Returns:
        DataFrame: a data frame from the table
    """
    dfnames = pd.read_csv(loc_name_file, comment="#", skiprows=0, encoding="latin1")
    dfnames = dfnames.fillna("")
    dfnames.index = dfnames["dsm2_id"]
    dfnames.columns = dfnames.columns.str.strip()
    return dfnames


def load_location_file(locationfile, gate_data=False):
    # def load_location_file(locationfile):
    df = load_location_table(locationfile)
    columns_to_keep = [
        "dsm2_id",
        "obs_station_id",
        "station_name",
        "subtract",
        "time_window_exclusion_list",
        "threshold_value",
        "ratio",
        "lat",
        "lon",
    ]
    new_column_names = [
        "Name",
        "BPart",
        "Description",
        "subtract",
        "time_window_exclusion_list",
        "threshold_value",
        "ratio",
        "Latitude",
        "Longitude",
    ]
    if gate_data:
        columns_to_keep = ["dsm2_id", "obs_station_id", "station_name"]
        new_column_names = ["Name", "BPart", "Description"]
    # optionally allow overriding of VARTYPE, by specifying a vartype for each dataset in the calibration_<constituent>_stations.csv file.
    # This is needed for DSM2 rim flow input files, which have cparts such as FLOW-DIVERSION and FLOW-EXPORT
    if "OBS VARTYPE" in df.columns:
        columns_to_keep.append("OBS VARTYPE")
        new_column_names.append("OBS_VARTYPE")
    if "MODEL VARTYPE" in df.columns:
        columns_to_keep.append("MODEL VARTYPE")
        new_column_names.append("MODEL_VARTYPE")
    dfloc = None
    try:
        if "subtract" not in df.columns:
            columns_to_keep.remove("subtract")
            new_column_names.remove("subtract")
        if "ratio" not in df.columns:
            columns_to_keep.remove("ratio")
            new_column_names.remove("ratio")
        dfloc = df[columns_to_keep]
        dfloc.columns = new_column_names
    except:
        logging.info(
            "****************************************************************************************************"
        )
        logging.info(
            "error in pydsm.postpro.load_location_file: location file must have the following fields:"
        )
        logging.info(str(columns_to_keep))
        logging.info(
            "****************************************************************************************************"
        )
    return dfloc


# -------- HELPER FUNCTIONS ----------#


def build_processors(dssfile, locationfile, vartype, units, study_name, observed=False):
    dfloc = load_location_file(locationfile)
    processors = []
    for index, row in dfloc.iterrows():
        # if obs or model vartype specified, override specified vartype
        # this is necessary for files such as DSM2 input DSS files, which have FLOW, FLOW-DIVERSION, and FLOW-EXPORT
        if observed:
            if "OBS_VARTYPE" in dfloc.columns:
                vartype = row["OBS_VARTYPE"]
        else:
            if "MODEL_VARTYPE" in dfloc.columns:
                vartype = row["MODEL_VARTYPE"]
        subtract = False
        ratio = False
        if (
            "subtract" in row
            and row["subtract"].lower() in ["yes", "true"]
            and "-" in row["Name"]
            and "-" in row["BPart"]
        ):
            subtract = True
        if (
            "ratio" in row
            and row["ratio"].lower() in ["yes", "true"]
            and "_div_" in row["Name"]
            and "_div_" in row["BPart"]
        ):
            ratio = True
        processor = PostProcessor(
            Study(study_name, dssfile),
            Location(
                row["Name"],
                row["BPart"] if observed else row["Name"],
                row["Description"],
                row["time_window_exclusion_list"],
                row["threshold_value"],
            ),
            VarType(vartype, units),
            subtract=subtract,
            ratio=ratio,
        )
        if observed:
            processor.do_resample_with_merge("15min")
            processor.do_fill_in()
        processors.append(processor)
    return processors


def run_processor(processor, store=True, clear=True):
    logging.info("Running %s/%s" % (processor.location.name, processor.vartype.name))
    processed = False
    try:
        processor.process()
        processed = True
    except Exception as ex:
        logging.info("exception caught in run_processor: " + str(ex))
        logging.exception(
            f"Failed to process {processor.location.name}/{processor.vartype.name}: "
        )
    if processed:
        if store:
            logging.info(
                "Storing %s/%s" % (processor.location.name, processor.vartype.name)
            )
            processor.store_processed()
        if clear:
            logging.info(
                "Clearing %s/%s" % (processor.location.name, processor.vartype.name)
            )
            processor.clear_refs()
        logging.info("Done %s/%s" % (processor.location.name, processor.vartype.name))


#


@click.command()
@click.argument("dssfile", type=click.Path(exists=True))
@click.argument("locationfile", type=click.Path(exists=True))
@click.option(
    "--vartype",
    type=click.STRING,
    help="Name of variable type, e.g. FLOW, STAGE, EC, TEMP",
)
@click.option(
    "--units",
    type=click.STRING,
    help="Units for variable type, e.g. cfs, feet, mmhos/cm",
)
@click.option(
    "--study_name",
    type=click.STRING,
    help="study name, also used for F-part of post processed dssfile",
)
@click.option(
    "--observed",
    is_flag=True,
    default=False,
    help="if dss file is observed data (resampling, merging, filling, scaling may be needed)",
)
def postpro(dssfile, locationfile, vartype, units, study_name, observed=False):
    """
    Postprocess dssfile with locations defined in locationfile for vartype.
    Args:
        dssfile: DSS file (input)
        locationfile: Location file (.csv) with location names and descriptions
        vartype ([type]): [description]
        units ([type]): [description]
        study_name ([type]): [description]
        observed (bool, optional): [description]. Defaults to False.
    """
    dfloc = load_location_file(locationfile)
    for index, row in dfloc.iterrows():
        processor = PostProcessor(
            Study(study_name, dssfile),
            Location(row["Name"], row["BPart"], row["Description"]),
            VarType(vartype, units),
        )
        if observed:  # TODO: Could be pushed out to a processing instruction file
            processor.do_resample_with_merge("15min")
            processor.do_fill_in()
        run_processor(processor, store=True, clear=False)


def postpro_diff(study1, study2, locationfile, vartype, units):

    logging.info("postpro_diff: study1, study2=" + study1 + "," + study2)

    dfloc = load_location_file(locationfile)
    for index, row in dfloc.iterrows():
        p1 = PostProcessor(
            study1,
            Location(row["Name"], row["BPart"], row["Description"]),
            VarType(vartype, units),
        )
        p2 = PostProcessor(
            study2,
            Location(row["Name"], row["BPart"], row["Description"]),
            VarType(vartype, units),
        )
        p1.load_processed()
        p2.load_processed()
        p1.process_diff(p2)
        p1.store_diff()
