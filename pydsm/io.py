import pandas as pd
import numpy as np
import h5py
"""
This module loads h5 files created by a DSM2 hydro or qual run.
All the input, geometry and data tables are available as pandas DataFrame objects

In addition there are convenience methods for retrieving the data tables as
DataFrame that represent time series. The date range is based on attributes in
the tables, namely start time and time interval.


"""
_path_structure_map = {"hydro": ['/hydro/input','/hydro/data','/hydro/geometry'],
    "qual":["input","output"]}
def get_model(filename):
    """
    returns one of "hydro" or "qual"
    """
    with h5py.File(filename,'r') as f:
        if f.get('/hydro'):
            return "hydro"
        else:
            return "qual"
def list_table_paths(filename):
    """
    returns a list of paths to tables (h5py.DataSets)
    """
    table_paths=[]
    with h5py.File(filename,'r') as f:
        # incase both hydro and qual output to the same file (some ovewriting possible?)
        group_paths=['/hydro/input','/hydro/data','/hydro/geometry','input','output']
        for path in group_paths:
            g=f.get(path)
            if not g: continue
            for key in g.keys():
                table_paths.append(path+'/'+key)
    return table_paths
def list_timeseries_available(filename):
    pass
def list_groups_as_df(filename, group_path):
    ''' reads listing of group path as pd.DataFrame '''
    with h5py.File(filename,'r') as f:
        return pd.DataFrame(f[group_path])
def read_table_attr(filename, table_path):
    '''
    reads a tables attribute as a pandas.DataFrame
    returns a data frame of attributes as Name and Value columns
    '''
    with h5py.File(filename,'r') as f:
        bf=f[table_path]
        a=pd.DataFrame(bf.attrs.items(),columns=['Name','Value'],dtype=np.str)
        a=a.append(pd.DataFrame([('shape',str(bf.shape))],columns=['Name','Value']),ignore_index=True)
    return a
def df_to_dict(df, key_column='Name', value_column='Value'):
    '''
    creates a dictionary based on a key_column (default Name) mapped to a value_column (default Value)
    '''
    return dict(zip(df[key_column],df[value_column]))
def _convert_time_to_table_slice(start_time, end_time, interval, table_start_time, table_time_length):
    '''
    start_time and end_time as convertable to to_datetime
    interval as convertable to Timedelta
    table_start_time convertable to_datetime
    table_time_length int
    '''
    st=pd.to_datetime(start_time)
    et=pd.to_datetime(end_time)
    table_start_time=pd.to_datetime(table_start_time)
    interval=pd.Timedelta(interval)
    if et < st : raise "Start time: "+st+" is ahead of end time: "+et
    table_end_time=table_start_time+interval*table_time_length
    if st < table_start_time: st = table_start_time
    if et > table_end_time: et = table_end_time
    start_index = int((st-table_start_time)/interval)
    end_index=int((et-table_start_time)/interval)
    return slice(start_index,end_index,1)
def df_column_values_to_index(df,column_label,matching_values):
    '''
    returns in the index values for the column_label for the matching_values
    in the DataFrame of that column
    '''
    return df[df[column_label].isin(matching_values)].index.values
def read_table_as_df(filename, table_path, sliver=slice(None)):
    '''
    reads table as a pandas.DateFrame
    if slice is specified, only that slice of the table is read, default is slice(None)
    returns a data frame with forcing datatype to string
    '''
    with h5py.File(filename,'r') as f:
        bf=f[table_path][sliver]
        x=pd.DataFrame(bf,dtype=np.str)
    return x
def read_table_as_array(filename, table_path, dtype=str):
    '''
    reads table from h5 filename from the table_path and returns array of dtype
    '''
    with h5py.File(filename,'r') as f:
        return np.array(f[table_path]).astype(dtype)
def read_dsm2_table(filename, table_path, column_values, column_names, start_time_key='start_time', interval_key='interval'):
    '''
    filename: Name of h5 file (full path or relative path)
    table_path: Path within the h5 file to the values table e.g. /output/channel_concentrations
    column_values: Values used for the 2nd and 3rd dimension of table.
        For DSM2 the 2nd dimension is the variable dimension (flow, stage, constituent)
                 the 3rd dimension is the location dimension (channel, reservoir)
                 Time is always assumed to be the first dimension in the table
    column_names: Names for the 2nd and 3rd dimensions
    returns DataFrame with time as index and variable and location (channel number)
    is the MultiIndex for columns
    '''
    with h5py.File(filename,'r') as f:
        v=f[table_path]
        a=v.attrs
        start_time=a[start_time_key].astype(str)[0]
        interval=a[interval_key].astype(str)[0]
        vals=np.array(v)
    c1=column_values[0]
    c2=column_values[1]
    x1=c1.repeat(c2.size)
    x2=c2.repeat(c1.size)
    vi=pd.MultiIndex.from_arrays([x1,x2],names=tuple(column_names))
    vti=pd.DatetimeIndex(data=pd.date_range(start=start_time,freq=interval,periods=vals.shape[0])
            ,name="Time")
    return pd.DataFrame(data=vals.reshape(vals.shape[0],vals.shape[1]*vals.shape[2]),index=vti,columns=vi)
