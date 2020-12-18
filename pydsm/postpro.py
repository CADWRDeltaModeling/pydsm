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

Location = collections.namedtuple('Location', ['name', 'bpart', 'description'])
VarType = collections.namedtuple('VarType', ['name', 'units'])


def convert_index_to_timestamps(df):
    if isinstance(df.index, pd.core.indexes.datetimes.DatetimeIndex):
        return
    df.index = df.index.astype('datetime64[ns]')
    df.index.freq = pd.infer_freq(df.index)  # if possible


def resample_with_interpolation(df: DataFrame, time_interval='15MIN', interpolation_method='linear') -> DataFrame:
    """Resampling to the time_interval and using the interpolation method

    Args:
        df: DataFrame with time index
        time_interval (str, optional): [time interval for resampling]. Defaults to '15MIN'.
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
    return num_nans_before_filling-num_nans_after_filling


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

#------------ METRICS CALCULATOR FUNCTIONS ---------#


#----------- POST PROCESS and CACHE RESULTS --------#
# Cache original time series, godin filtered, tidal high, low and amplitude calculations and differences in phase with observed time series.


class PostProCache:
    """Post process cache: For loading and storing to a HEC-DSS cache filename
    The cache filename is determined by adding "_calib_postpro" to the filename"""
    A_PART = 'POSTPRO'  # The apart to be used when storing to DSS format
    IRR_E_PART = 'IR-MONTH'  # The epart to use for irregular time series

    def postpro_filename(fname: str, ext='dss') -> str:
        '''Returns the name of the postpro cache filename based on this filename'''
        return fname.split('.dss')[0]+'_calib_postpro.%s' % ext

    def is_rts(df):
        return df.index.freqstr is not None

    def __init__(self, fname):
        '''Cache filename based on input DSS filename (i.e. ends in .dss)'''
        self.fname = PostProCache.postpro_filename(fname)

    def store(self, df, units, bpart, cpart, epart, fpart):
        with pyhecdss.DSSFile(self.fname) as dh:
            if PostProCache.is_rts(df):
                dh.write_rts('/%s/%s/%s//%s/%s/' % (PostProCache.A_PART, bpart.upper(), cpart.upper(), epart.upper(), fpart.upper()),
                             df, units.upper(), 'INST-VAL')
            else:
                dh.write_its('/%s/%s/%s//%s/%s/' %
                             (PostProCache.A_PART, bpart.upper(), cpart.upper(),
                              PostProCache.IRR_E_PART, fpart.upper()),
                             df, units.upper(), 'INST-VAL')

    def load(self, bpart, cpart, dpart):
        return next(pyhecdss.get_ts(self.fname, '/%s/%s/%s/%s///' %
                                    (PostProCache.A_PART, bpart.upper(), cpart.upper(), dpart.upper())))


class PostProcessor:
    '''Post processor contains the operations to be applied to the input HEC-DSS file'''

    TIME_INTERVAL = '15MIN'
    CPART_SUFFIX_MAP = {}

    def __init__(self, fname: str, location: Location, vartype: VarType, study_name: str):
        self.fname = fname
        self.location = location
        self.vartype = vartype
        self.study_name = study_name
        self.cache = PostProCache(self.fname)
        self._set_default_ops()

    def _set_default_ops(self):
        self.do_resampling_with_merging = False
        self.do_filling_in = False
        self.do_scaling = False

    def do_resample_with_merge(self, time_interval):
        self.do_resampling_with_merging = True
        self.resampling_interval = time_interval

    def do_fill_in(self, max_fillin_gap=3, fillin_method='linear'):
        self.do_filling_in = True
        self.max_fillin_gap = max_fillin_gap
        self.fillin_method = fillin_method

    def do_scale(self, scale):
        self.do_scaling = True
        self.scale = scale

    def _read_ts(self):
        pathname = '//%s/%s////' % (self.location.bpart, self.vartype.name)
        return pyhecdss.get_ts(self.fname, pathname.upper())

    def process(self):
        dfgen = self._read_ts()
        if self.do_resampling_with_merging:
            dflist = [resample_with_interpolation(
                df.data, time_interval=PostProcessor.TIME_INTERVAL) for df in contextlib.closing(dfgen)]
            df = merge(dflist)
        else:
            df = next(dfgen).data
            convert_index_to_timestamps(df)  # inplace change of index

        if self.do_filling_in:
            df = fill_in(df, self.max_fillin_gap, self.fillin_method)

        if self.do_scaling:
            df = df.mul(self.scale)

        self.df = df

        self.gdf = godin(self.df)
        self.high, self.low = tidalhl.get_tidal_hl_rolling(self.df)
        self.amp = tidalhl.get_tidal_amplitude(self.high, self.low)
    
    def clear_refs(self):
        self.df=self.gdf=self.high=self.low=self.amp=None

    def _store(self, df, cpart_suffix='', epart=TIME_INTERVAL):
        self.cache.store(df, self.vartype.units, self.location.name, self.vartype.name + cpart_suffix,
                         epart, self.study_name)

    def _load(self, cpart_suffix='', timewindow=''):
        return self.cache.load(self.location.name, self.vartype.name+cpart_suffix, timewindow).data

    def store_processed(self):
        self._store(self.df)
        self._store(self.gdf, '-GODIN')
        self._store(self.high, '-HIGH', PostProCache.IRR_E_PART)
        self._store(self.low, '-LOW', PostProCache.IRR_E_PART)
        self._store(self.amp, '-AMP', PostProCache.IRR_E_PART)

    def load_processed(self, timewindow=''):
        self.df = self._load(cpart_suffix='', timewindow=timewindow)
        self.gdf = self._load(cpart_suffix='-GODIN', timewindow=timewindow)
        self.high = self._load(cpart_suffix='-HIGH', timewindow=timewindow)
        self.low = self._load(cpart_suffix='-LOW', timewindow=timewindow)
        self.amp = self._load(cpart_suffix='-AMP', timewindow=timewindow)

    def process_diff(self, other):
        self.amp_diff = tidalhl.get_tidal_amplitude_diff(self.amp, other.amp)
        self.amp_diff_pct = tidalhl.get_tidal_amplitude_diff(self.amp, other.amp, percent_diff=True)
        self.phase_diff = tidalhl.get_tidal_phase_diff(self.high, self.low, other.high, other.low)

    def store_diff(self):
        self._store(self.amp_diff, '-AMP-DIFF', PostProCache.IRR_E_PART)
        self._store(self.amp_diff_pct, '-AMP-DIFF-PCT', PostProCache.IRR_E_PART)
        self._store(self.phase_diff, '-PHASE-DIFF', PostProCache.IRR_E_PART)

    def load_diff(self, timewindow=''):
        self.amp_diff = self._load('-AMP-DIFF', timewindow)
        self.amp_diff_pct = self._load('-AMP-DIFF-PCT', timewindow)
        self.phase_diff = self._load('-PHASE-DIFF', timewindow)


def load_location_table(loc_name_file: str):
    """Loads locations from the table. 

    DSM2 ID,CDEC ID,Station Name,Elevation,Latitude,Longitude,...
    SSS,SSS,Steamboat Slough,10,38.285,-121.587,...

    Args:
        loc_name_file (str): Path to file as string

    Returns:
        DataFrame: a data frame from the table
    """
    dfnames = pd.read_csv(loc_name_file, comment='#', skiprows=0, encoding='latin1')
    dfnames = dfnames.fillna('')
    dfnames.index = dfnames['DSM2 ID']
    dfnames.columns = dfnames.columns.str.strip()
    return dfnames


def load_location_file(locationfile):
    df = load_location_table(locationfile)
    dfloc = df[['DSM2 ID', 'CDEC ID', 'Station Name', 'Latitude', 'Longitude']]
    dfloc.columns = ['Name', 'BPart', 'Description', 'Latitude', 'Longitude']
    return dfloc

#-------- HELPER FUNCTIONS ----------#


def build_processors(dssfile, locationfile, vartype, units, study_name, observed=False):
    dfloc = load_location_file(locationfile)
    processors = []
    for index, row in dfloc.iterrows():
        processor = PostProcessor(dssfile,
                                  Location(row['Name'],
                                           row['BPart'] if observed else row['Name'],
                                           row['Description']),
                                  VarType(vartype, units), study_name)
        if observed:  # TODO: Could be pushed out to a processing instruction file
            processor.do_resample_with_merge('15MIN')
            processor.do_fill_in()
            # TODO: Should be pushed out to an external file
            if vartype == 'FLOW' and row['Name'] == 'VCU':
                processor.do_scale(-1)
        processors.append(processor)
    return processors


def run_processor(processor):
    logging.info('Running %s/%s'%(processor.location.name,processor.vartype.name))
    print('Running %s/%s'%(processor.location.name,processor.vartype.name))
    processor.process()
    logging.info('Storing %s/%s'%(processor.location.name,processor.vartype.name))
    processor.store_processed()
    processor.clear_refs()
    logging.info('Done %s/%s'%(processor.location.name,processor.vartype.name))

#
@click.command()
@click.argument('dssfile', type=click.Path(exists=True))
@click.argument('locationfile', type=click.Path(exists=True))
@click.option('--vartype', type=click.STRING, help='Name of variable type, e.g. FLOW, STAGE, EC')
@click.option('--units', type=click.STRING, help='Units for variable type, e.g. cfs, feet, mmhos/cm')
@click.option('--study_name', type=click.STRING, help='study name, also used for F-part of post processed dssfile')
@click.option('--observed', is_flag=True, default=False, help='if dss file is observed data (resampling, merging, filling, scaling may be needed)')
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
        processor = PostProcessor(dssfile, Location(
            row['Name'], row['BPart'], row['Description']), VarType(vartype, units), study_name)
        if observed:  # TODO: Could be pushed out to a processing instruction file
            processor.do_resample_with_merge('15MIN')
            processor.do_fill_in()
            if vartype == 'FLOW' and row['Name'] == 'VCU':
                processor.do_scale(-1)
        processor.process()
        processor.store_processed()


def postpro_diff(dssfile1, dssfile2, locationfile, vartype, units, study_name1, study_name2):
    dfloc = load_location_file(locationfile)
    for index, row in dfloc.iterrows():
        p1 = PostProcessor(dssfile1, Location(
            row['Name'], row['BPart'], row['Description']), VarType(vartype, units), study_name1)
        p2 = PostProcessor(dssfile2, Location(
            row['Name'], row['BPart'], row['Description']), VarType(vartype, units), study_name2)
        p1.load_processed()
        p2.load_processed()
        p1.process_diff(p2)
        p1.store_diff()
