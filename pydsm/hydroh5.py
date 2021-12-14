'''
Hydro H5 reader. 

check to make sure it has hydro data
get list of channels 
get list of reservoirs
get time series data as pandas DataFrame 

    for given list of channels 
    for given list of reservoirs  

'''
import h5py
import pandas as pd
import numpy as np
from .import dsm2h5


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
        if not dsm2h5.get_model(self.h5) == 'hydro':
            raise f'{filename} is not a hydro tidefile: Could be '+dsm2h5.get_model(self.h5)+' ?'
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

    def get_input_tables(self):
        return dsm2h5.get_paths_for_group_path(self.h5, '/hydro/input')

    def get_input_table(self, table_path):
        """ See get_input_tables for a list of table paths
        Returns a dataframe for the contents of the table at the path"""
        return dsm2h5.read_table_as_df(self.h5, table_path)

    def get_geometry_tables(self):
        return dsm2h5.get_paths_for_group_path(self.h5, '/hydro/geometry')

    def get_geometry_table(self, table_path):
        return dsm2h5.read_table_as_df(self.h5, table_path)

    def get_channels(self):
        '''
        return pandas DataFrame of channel ids as indexed
        '''
        channels = pd.DataFrame(self.h5.get(HydroH5._GEOM_PATH+'/channel_number'), dtype=str)
        self.channel_index2number = channels[0].to_dict()
        self.channel_number2index = {
            value: key for key, value in self.channel_index2number.items()}
        return channels

    def get_channel_locations(self):
        '''
        return pandas DataFrame of channel locations ( upstream or downstream)
        '''
        self.channel_locs = pd.DataFrame(self.h5.get(HydroH5._GEOM_PATH+'/channel_location'))
        self.channel_locs.iloc[:, 0] = self.channel_locs.iloc[:, 0].str.decode('utf-8')
        self.channel_location2number = self.channel_locs[0].to_dict()
        self.channel_location2index = {
            value: key for key, value in self.channel_location2number.items()}
        return self.channel_locs

    def get_reservoirs(self):
        '''
        return pandas DataFrame of reservoirs
        '''
        self.reservoirs = pd.DataFrame(self.h5.get(
            HydroH5._GEOM_PATH+'/reservoir_names'), columns=['name'])
        self.reservoirs.iloc[:, 0] = self.reservoirs.iloc[:, 0].str.decode('utf-8')
        self.reservoir_node_connections = dsm2h5.read_table_as_df(
            self.h5, HydroH5._GEOM_PATH+'/reservoir_node_connect')
        return self.reservoirs

    def get_qext(self):
        '''
        return external flows as defined in DSM2 Hydro
        '''
        self.qext = dsm2h5.read_table_as_df(
            self.h5, HydroH5._GEOM_PATH+'/qext')
        return self.qext

    def get_channel_bottom(self, channels):
        if isinstance(channels, list):
            channels = [str(c) for c in channels]
        else:
            channels = str(channels)
        cids = self._channel_ids_to_indicies(channels)
        df = self.get_geometry_table('/hydro/geometry/channel_bottom')[cids]
        df.columns = [f'{id}' for id in self._channel_ids_to_sequence(channels)]
        df=df.T
        df.columns=['upstream','downstream']
        return df

    def get_data_tables(self):
        return HydroH5._DATA_TABLES

    def _channel_ids_to_sequence(self, channel_id_slice):
        '''
        convert a slice of channel ids to a slice of channel indices into data table
        '''
        if isinstance(channel_id_slice, str):
            return [channel_id_slice]
        else:
            return channel_id_slice

    def _channel_ids_to_indicies(self, channel_id_slice):
        '''
        convert a slice of channel ids to a slice of channel indices into data table
        '''
        if isinstance(channel_id_slice, str):
            return self.channel_number2index[channel_id_slice]
        elif dsm2h5.is_sequence_like(channel_id_slice):
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

    def _get_channel_ts(self, table_path, channels, location='upstream', timewindow=None):
        '''
        return a pandas DataFrame of time series from the table for the given 
            list of channels
            channel_location ('upstream' or 'downstream')
            timewindow in the format of <<start time str>> - <<end time str>>, e.g. 01JAN1990 - 05JUL1992
        '''
        if isinstance(channels, list):
            channels = [str(c) for c in channels]
        else:
            channels = str(channels)
        channel_indices = self._channel_ids_to_indicies(channels)
        if location:
            location_indices = self._channel_locations_to_indicies(location)
            df = dsm2h5.read_time_indexed_table(self.h5,
                                                table_path, timewindow, channel_indices, location_indices)
            df.columns = [f'{id}-{location}' for id in self._channel_ids_to_sequence(channels)]
        else:
            df = dsm2h5.read_time_indexed_table(self.h5,
                                                table_path, timewindow, channel_indices)
            df.columns = [f'{id}' for id in self._channel_ids_to_sequence(channels)]
        return df

    def _get_reservoir_ts(self, table_path, reservoirs_names, connection_ids=None, timewindow=None):
        '''
        '''
        res = self.get_reservoirs()
        rnc = self.reservoir_node_connections
        res_names = dsm2h5.normalize_to_slice(reservoirs_names)
        if connection_ids is None:
            indices = res[res.name.isin(res_names)].index.values
        else:
            if len(connection_ids) != len(res_names):
                raise "Connection ID array length should match reservoir names"
            mask = rnc[['res_name', 'connect_index']].isin(
                {'res_name': res_names, 'connect_index': connection_ids})
            indices = rnc[mask.all(axis=1)].index.values
        df = dsm2h5.read_time_indexed_table(self.h5, table_path, timewindow, indices)
        if connection_ids is None:
            df.columns = list(res_names)
        else:
            df.columns = list(map(lambda x: f'x[0]/x[1]', zip(reservoirs_names, connection_ids)))
        return df

    def _get_qext_ts(self, table_path, qext_names, timewindow=None):
        '''
        '''
        qext = self.get_qext()
        qext_norm = dsm2h5.normalize_to_slice(qext_names)
        qext_indicies = qext[qext.name.isin(qext_norm)].index.values
        df = dsm2h5.read_time_indexed_table(self.h5, table_path, timewindow, qext_indicies)
        df.columns = list(qext_norm)
        return df

    def get_channel_flow(self, channel_id, location_id='upstream', timewindow=None):
        return self._get_channel_ts('/hydro/data/channel flow', channel_id, location_id, timewindow)

    def get_channel_area(self, channel_id, location_id='upstream', timewindow=None):
        return self._get_channel_ts('/hydro/data/channel area', channel_id, location_id, timewindow)

    def get_channel_stage(self, channel_id, location_id='upstream', timewindow=None):
        return self._get_channel_ts('/hydro/data/channel stage', channel_id, location_id, timewindow)

    def get_channel_avg_area(self, channel_id, timewindow=None):
        return self._get_channel_ts('/hydro/data/channel avg area', channel_id, timewindow)

    def get_reservoir_flow(self, reservoir_name, timewindow=None):
        res = self.get_reservoirs()
        rt = self.reservoir_node_connections[self.reservoir_node_connections.res_name == reservoir_name]
        return self._get_reservoir_ts('/hydro/data/reservoir flow', rt.res_name.values, rt.connect_index.values, timewindow)

    def get_reservoir_height(self, reservoir_name, timewindow=None):
        return self._get_reservoir_ts('/hydro/data/reservoir height', reservoir_name, timewindow)

    def get_qext_flow(self, qext_id, timewindow=None):
        return self._get_qext_ts('/hydro/data/qext flow', qext_id, timewindow)

    def get_transfer_flow(self, transfer_id, timewindow=None):
        return self._get_channel_ts('/hydro/data/transfer flow', transfer_id, timewindow)
