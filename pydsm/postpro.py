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
from os.path import exists


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


# ------------ METRICS CALCULATOR FUNCTIONS ---------#


# ----------- POST PROCESS and CACHE RESULTS --------#
# Cache original time series, godin filtered, tidal high, low and amplitude calculations and differences in phase with observed time series.


class PostProCache:
    """Post process cache: For loading and storing to a HEC-DSS cache filename
    The cache filename is determined by adding "_calib_postpro" to the filename"""

    A_PART = "POSTPRO"  # The apart to be used when storing to DSS format
    IRR_E_PART = "IR-MONTH"  # The epart to use for irregular time series

    def __init__(self, fname):
        """Cache filename based on input DSS filename (i.e. ends in .dss)"""
        self.fname = PostProCache.postpro_filename(fname)
        self.dssh = pyhecdss.DSSFile(self.fname)
        self.dss_catalog = self.dssh.read_catalog()
        self.dss_catalog["Path"] = self.dss_catalog.apply(
            lambda r: f'{r["B"]}/{r["C"]}', axis=1
        )

    def __del__(self):
        self.dssh.close()

    def postpro_filename(fname: str, ext="dss") -> str:
        """Returns the name of the postpro cache filename based on this filename"""
        return fname.split(".dss")[0] + "_calib_postpro.%s" % ext

    def is_rts(df):
        return hasattr(df.index, "freqstr") and df.index.freqstr is not None

    def store(self, df, units, bpart, cpart, epart, fpart):
        with pyhecdss.DSSFile(self.fname, create_new=True) as dh:
            if df.empty:
                print(
                    "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
                )
                print(
                    f"Empty dataframe in pydsm.postpro.PostProCache.store({bpart}/{cpart}/{epart}/{fpart}) while trying to store data."
                )
                print(
                    "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
                )
                return
            if PostProCache.is_rts(df):
                dh.write_rts(
                    "/%s/%s/%s//%s/%s/"
                    % (
                        PostProCache.A_PART,
                        bpart.upper(),
                        cpart.upper(),
                        epart.upper(),
                        fpart.upper(),
                    ),
                    df,
                    units.upper(),
                    "INST-VAL",
                )
            else:
                dh.write_its(
                    "/%s/%s/%s//%s/%s/"
                    % (
                        PostProCache.A_PART,
                        bpart.upper(),
                        cpart.upper(),
                        PostProCache.IRR_E_PART,
                        fpart.upper(),
                    ),
                    df,
                    units.upper(),
                    "INST-VAL",
                )

    def load(self, bpart, cpart, dpart):
        return_series = None
        dss_path = "/%s/%s/%s/%s///" % (
            PostProCache.A_PART,
            bpart.upper(),
            cpart.upper(),
            dpart.upper(),
        )
        try:
            # return_series = next(pyhecdss.get_ts(self.fname, dss_path))
            matching = self.dss_catalog[
                self.dss_catalog["Path"] == f"{bpart.upper()}/{cpart.upper()}"
            ]
            if len(matching) == 0:
                raise StopIteration
            pathname = self.dssh.get_pathnames(matching)[0]
            if pathname.split("/")[5].startswith("IR-"):
                return_series = self.dssh.read_its(pathname)
            else:
                return_series = self.dssh.read_rts(pathname)
        except StopIteration as e:
            logging.exception(
                "pydsm.postpro.PostProCache.load: no data found for "
                + self.fname
                + ","
                + dss_path
            )
            if exists(self.fname):
                logging.exception(
                    "DSS file found, but data not found in file. DSS File, DSS path="
                    + self.fname
                    + ","
                    + dss_path
                )
            else:
                logging.exception("DSS file not found:" + self.fname)
        return return_series


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
        returns dataframe
        """
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
                        print(
                            "****************************************************************************************************************"
                        )
                        print(
                            "Error in postpro._read_ts: dflist has len 0, after trying to read pathname: "
                            + pathname.upper()
                        )
                        print(
                            "****************************************************************************************************************"
                        )
                        return_df = None
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
                print("bpart_parts=" + str(bpart_parts))

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
                                # this is not the place to do this, because we need godin
                                # first remove all rows if either value is nan
                                # ratio_df = return_df.merge(new_df, how='inner')
                                # print('ratio_df='+str(ratio_df.head()))
                                # ratio_df = ratio_df[ratio_df.notna()]
                                # numerator_col_name = ratio_df.columns[0]
                                # denominator_col_name = ratio_df.columns[1]
                                # ratio_df['ratio'] = ratio_df[numerator_col_name] / ratio_df[denominator_col_name]
                                # # return_df = return_df.divide(new_df)
                                # return_df = ratio_df[denominator_col_name]
                                # do nothing for now.
                                return_df = None
                    df_index += 1
                return_df.columns = [new_dss_path]
        return return_df

    def process(self):
        df = self._read_ts()
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
            self.study.name,
        )

    def _load(self, cpart_suffix="", timewindow=""):
        return_series = None
        try:
            series = self.cache.load(
                self.location.name, self.vartype.name + cpart_suffix, timewindow
            )
            if series is not None:
                return_series = series.data
        except StopIteration as e:
            logging.exception("pydsm.postpro.PostProCache.load: no data found")
        # except AttributeError as e:
        #     print('ERROR: location, vartype='+str(self.location)+','+str(self.vartype))
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
            logging.warning(
                "pydsm.postpro.PostProCache.store_processed: no data to store: "
                + str(self)
            )
            return
        self._store(self.df)
        self._store(self.gdf, "-GODIN")
        self._store(self.high, "-HIGH", PostProCache.IRR_E_PART)
        self._store(self.low, "-LOW", PostProCache.IRR_E_PART)
        self._store(self.amp, "-AMP", PostProCache.IRR_E_PART)

    def load_processed(self, timewindow=""):
        self.df = self._load(cpart_suffix="", timewindow=timewindow)
        self.gdf = self._load(cpart_suffix="-GODIN", timewindow=timewindow)
        self.high = self._load(cpart_suffix="-HIGH", timewindow=timewindow)
        self.low = self._load(cpart_suffix="-LOW", timewindow=timewindow)
        self.amp = self._load(cpart_suffix="-AMP", timewindow=timewindow)
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
        return success

    def process_diff(self, other):
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
        self.amp_diff = self._load("-AMP-DIFF", timewindow)
        self.amp_diff_pct = self._load("-AMP-DIFF-PCT", timewindow)
        self.phase_diff = self._load("-PHASE-DIFF", timewindow)


def load_location_table(loc_name_file: str):
    """Loads locations from the table.

    DSM2 ID,CDEC ID,Station Name,Elevation,Latitude,Longitude,...
    SSS,SSS,Steamboat Slough,10,38.285,-121.587,...

    Args:
        loc_name_file (str): Path to file as string

    Returns:
        DataFrame: a data frame from the table
    """
    dfnames = pd.read_csv(loc_name_file, comment="#", skiprows=0, encoding="latin1")
    dfnames = dfnames.fillna("")
    dfnames.index = dfnames["DSM2 ID"]
    dfnames.columns = dfnames.columns.str.strip()
    return dfnames


def load_location_file(locationfile, gate_data=False):
    # def load_location_file(locationfile):
    df = load_location_table(locationfile)
    columns_to_keep = [
        "DSM2 ID",
        "CDEC ID",
        "Station Name",
        "subtract",
        "time_window_exclusion_list",
        "threshold_value",
        "ratio",
        "Latitude",
        "Longitude",
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
        columns_to_keep = ["DSM2 ID", "CDEC ID", "Station Name"]
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
        print(
            "error in pydsm.postpro.load_location_file: location file must have the following fields:"
        )
        print(str(columns_to_keep))
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
    print("Running %s/%s" % (processor.location.name, processor.vartype.name))
    processed = False
    try:
        processor.process()
        processed = True
    except Exception as ex:
        print("exception caught in run_processor: " + str(ex))
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
