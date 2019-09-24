'''
Hydro H5 reader. 

check to make sure it has hydro data
get list of channels 
get list of reservoirs
get time series data as pandas DataFrame -
    for given list of channels 
    for given list of reservoirs  

'''
import h5py
import pandas as pd
import numpy as np
from .import h5common


class HydroH5:
    _GEOM_PATH = '/hydro/geometry'
    _DATA_PATH = '/hydro/data'
    _INPUT_PATH = '/hydro/input'

    _DATA_TABLES = ['channel flow', 'channel area', 'channel stage', 'channel avg area',
                    'qext flow', 'reservoir flow', 'reservoir height', 'transfer flow']

    def __init__(self, filename):
        '''
        opens the file and initializes variables for subsequent requests
        '''
        self.filename = filename
        self.h5 = h5py.File(filename, 'r')
        self._check_has_hydro_tables()
        # -- initialization code -- FIXME: be more lazy
        self.get_channels()
        self.get_channel_locations()
        self.get_reservoirs()
        self.get_qext()

    def __del__(self):
        '''
        closes file as cleanup
        '''
        self.h5.close()

    def _check_has_hydro_tables(self):
        return (self.h5['/hydro'] != None)

    def get_channels(self):
        '''
        return pandas DataFrame of channel ids as indexed
        '''
        channels = pd.DataFrame(self.h5.get(
            HydroH5._GEOM_PATH+'/channel_number'), dtype=np.str)
        self.channel_index2number = channels[0].to_dict()
        self.channel_number2index = {
            value: key for key, value in self.channel_index2number.items()}
        return channels

    def get_channel_locations(self):
        '''
        return pandas DataFrame of channel locations ( upstream or downstream)
        '''
        self.channel_locs = pd.DataFrame(self.h5.get(
            HydroH5._GEOM_PATH+'/channel_location'), dtype=np.str)
        self.channel_location2number = self.channel_locs[0].to_dict()
        self.channel_location2index = {
            value: key for key, value in self.channel_location2number.items()}
        return self.channel_locs

    def get_reservoirs(self):
        '''
        return pandas DataFrame of reservoirs
        '''
        self.reservoirs = pd.DataFrame(self.h5.get(
            HydroH5._GEOM_PATH+'/reservoir_names'), dtype=np.str)
        self.reservoir_node_connections = h5common.read_compound_table(
            self.h5, HydroH5._GEOM_PATH+'/reservoir_node_connect')
        return self.reservoirs

    def get_qext(self):
        '''
        return external flows as defined in DSM2 Hydro
        '''
        self.qext = h5common.read_compound_table(
            self.h5, HydroH5._GEOM_PATH+'/qext')
        return self.qext

    def get_data_tables(self):
        return HydroH5._DATA_TABLES

    def _read_attributes_from_table(self, data):
        #
        interval_string = data.attrs['interval'][0].decode('UTF-8')
        # FIXME: these conversions are HECDSS. Move them to pyhecdss utility function
        interval_string = interval_string.replace('min', 'T')
        interval_string = interval_string.replace('hour', 'H')
        interval_string = interval_string.replace('day', 'D')
        interval_string = interval_string.replace('mon', 'M')
        model = data.attrs['model'][0].decode('UTF-8')
        model_version = data.attrs['model_version'][0].decode('UTF-8')
        start_time = pd.to_datetime(
            data.attrs['start_time'][0].decode('UTF-8'))
        return {'interval': interval_string, 'model': model, 'model_version': model_version, 'start_time': start_time}

    def _is_sequence_like(self, obj):
        tobj = type(obj)
        return hasattr(tobj, '__len__') and hasattr(tobj, '__getitem__')

    def _channel_ids_to_indicies(self, channel_id_slice):
        '''
        convert a slice of channel ids to a slice of channel indices into data table
        '''
        if isinstance(channel_id_slice, str):
            return self.channel_number2index[channel_id_slice]
        elif self._is_sequence_like(channel_id_slice):
            return [self.channel_number2index[id] for id in channel_id_slice]
        elif isinstance(channel_id_slice, slice):
            return channel_id_slice
        else:
            raise RuntimeError('Channel id should be string, sequence of strings or slice: Called with : '
                               + channel_id_slice+' of type '+type(channel_id_slice))

    def _channel_locations_to_indicies(self, channel_location_slice):
        '''
        convert a slice of channel ids to a slice of channel indices into data table
        '''
        if isinstance(channel_location_slice, str):
            return self.channel_location2index[channel_location_slice]
        elif self._is_sequence_like(channel_location_slice):
            return [self.channel_location2index[id] for id in channel_location_slice]
        elif isinstance(channel_location_slice, slice):
            return channel_location_slice
        else:
            raise RuntimeError('Channel location should be string, sequence of strings or slice: Called with : '
                               + channel_location_slice+' of type '+type(channel_location_slice))

    def _convert_time_to_table_slice(start_time, end_time, interval, table_start_time, table_time_length):
        '''
        start_time and end_time as convertable to to_datetime
        interval as convertable to Timedelta
        table_start_time convertable to_datetime
        table_time_length int
        '''
        st = pd.to_datetime(start_time)
        et = pd.to_datetime(end_time)
        table_start_time = pd.to_datetime(table_start_time)
        interval = pd.Timedelta(interval)
        if et < st:
            raise "Start time: "+st+" is ahead of end time: "+et
        table_end_time = table_start_time+interval*table_time_length
        if st < table_start_time:
            st = table_start_time
        if et > table_end_time:
            et = table_end_time
        start_index = int((st-table_start_time)/interval)
        end_index = int((et-table_start_time)/interval)
        return slice(start_index, end_index, 1)

    def _get_channel_ts(self, table_path, channels, location='upstream', timewindow=None):
        '''
        return a pandas DataFrame of time series from the table for the given 
            list of channels
            channel_location ('upstream' or 'downstream')
            timewindow in the format of <<start time str>> - <<end time str>>, e.g. 01JAN1990 - 05JUL1992
        '''
        data = self.h5.get(table_path)
        #
        attrs = self._read_attributes_from_table(data)
        #
        channel_indices = self._channel_ids_to_indicies(channels)
        location_indices = self._channel_locations_to_indicies(
            location)  # FIXME: add location slicing
        stime = pd.to_datetime(attrs['start_time'])
        # if start time and end time given use the slice
        if timewindow:
            twse = [s.strip() for s in timewindow.split('-')]
            timeSlice = HydroH5._convert_time_to_table_slice(
                twse[0], twse[1], attrs['interval'], attrs['start_time'], data.shape[0])
        else:
            timeSlice = slice(None)
        if timeSlice.start:
            stime = stime + pd.Timedelta(attrs['interval'])*timeSlice.start
        darr = data[timeSlice, channel_indices, location_indices]
        df = pd.DataFrame(darr,
                          columns=[str(id)+'-'+location for id in channels],
                          index=pd.date_range(stime,
                                              freq=attrs['interval'],
                                              periods=darr.shape[0]),
                          dtype=np.float32)
        return df

    def get_channel_flow(self, channel_id, location_id='upstream', timewindow=None):
        return self._get_channel_ts('/hydro/data/channel flow', channel_id, location_id, timewindow)

    def get_channel_area(self, channel_id, location_id='upstream', timewindow=None):
        return self._get_channel_ts('/hydro/data/channel area', channel_id, location_id, timewindow)

    def get_channel_stage(self, channel_id, location_id='upstream', timewindow=None):
        return self._get_channel_ts('/hydro/data/channel stage', channel_id, location_id, timewindow)

    def get_channel_avg_area(self, channel_id):
        pass

    def get_reservoir_flow(self, reservoir_id, connection_id):
        pass

    def get_reservoir_height(self, reservoir_id):
        pass

    def get_qext_flow(self):
        pass

    def get_transfer_flow(self):
        pass
