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
_path_structure_map = {
    "hydro": ["/hydro/input", "/hydro/data", "/hydro/geometry"],
    "qual": ["input", "output"],
}


"""
hardwired lists of data available for modeltype. Currently
the model type is either 'hydro' or 'qual'
"""
_MODEL_TO_DATA_PATHS_MAP = {
    "hydro": [
        "/hydro/data/" + s
        for s in [
            "channel area",
            "channel avg area",
            "channel flow",
            "channel stage",
            "qext flow",
            "reservoir flow",
            "reservoir height",
            "transfer flow",
        ]
    ],
    "qual": [
        "/output/" + s
        for s in [
            "channel avg concentration",
            "channel concentration",
            "reservoir concentration",
        ]
    ],
    "gtm": [
        "/output" + s
        for s in [
            "cell concentration",
            "constituent_names",
            "reservoir concentration",
            "reservoir_names",
        ]
    ],
}


def get_model(h5f):
    """
    returns one of "hydro" or "qual"
    """
    if h5f.get("/hydro"):  # hydro files have hydro at top level
        return "hydro"
    elif h5f.get("/output/channel concentration"):
        return "gtm"
    elif h5f.get("/output"):  # qual files have output group not found in hydro
        return "qual"
    else:
        return "unknown"


def get_model_from_file(filename):
    """
    returns one of "hydro" or "qual"
    """
    with h5py.File(filename, "r") as f:
        return get_model(f)


def get_start_end_dates(tidef, scalar_table="/hydro/input/scalar"):
    scalar = tidef.get_input_table(scalar_table)
    sdate = scalar.loc[scalar["name"] == "run_start_date", "value"].values[0]
    edate = scalar.loc[scalar["name"] == "run_end_date", "value"].values[0]
    return sdate, edate


def get_datapaths(modeltype="hydro"):
    """
    Returns expected paths in model type (hydro or qual)

    :param modeltype: defaults to "hydro". could be "hydro" or "qual"
    :type modeltype: str, optional
    :return: list of model output paths
    :rtype: list
    """
    return _MODEL_TO_DATA_PATHS_MAP[modeltype]


def get_table_paths(
    filename,
    group_paths=["/hydro/input", "/hydro/data", "/hydro/geometry", "input", "output"],
):
    """
    returns a list of paths to tables (h5py.DataSets)
    """
    table_paths = []
    with h5py.File(filename, "r") as f:
        for path in group_paths:
            table_paths += get_paths_for_group_path(f, path)
    return table_paths


def get_paths_for_group_path(h5f, path):
    """
    get paths for the open file handle to .h5 and the path to the containing group

    :param h5f: filehandle
    :type h5f: h5py.H5 handle
    :param path: full path to group within .h5
    :type path: str
    :return: list of contained paths
    :rtype: list
    """
    gpath = h5f.get(path)
    if gpath:
        return [path + "/" + key for key in gpath]
    else:
        return []


def list_groups_as_df(filename, group_path):
    """reads listing of group path as pd.DataFrame"""
    with h5py.File(filename, "r") as f:
        return pd.DataFrame(f[group_path])


def read_table_attr(filename, table_path):
    """
    returns a dictionary of attribute names to values
    """
    return df_to_dict(read_table_attrs_as_df(filename, table_path))


def read_table_attrs_as_df(filename, table_path):
    """
    reads a tables attribute as a pandas.DataFrame
    returns a data frame of attributes as Name and Value columns
    """
    with h5py.File(filename, "r") as f:
        bf = f[table_path]
        a = pd.DataFrame(bf.attrs.items(), columns=["Name", "Value"], dtype=np.str)
        a = a.append(
            pd.DataFrame([("shape", str(bf.shape))], columns=["Name", "Value"]),
            ignore_index=True,
        )
    return a


def df_to_dict(df, key_column="Name", value_column="Value"):
    """
    creates a dictionary based on a key_column (default Name) mapped to a value_column (default Value)
    """
    return dict(zip(df[key_column], df[value_column]))


def df_column_values_to_index(df, column_label, matching_values):
    """
    returns in the index values for the column_label for the matching_values
    in the DataFrame of that column
    """
    return df[df[column_label].isin(matching_values)].index.values


def read_table_as_df(h5, tpath):
    """
    reads table as a pandas.DateFrame
    converts byte types to string and strips empty spaces
    all other types are kept as is

    :param h5: [description]
    :type h5: [type]
    :param tpath: [description]
    :type tpath: [type]
    :return: [description]
    :rtype: [type]
    """
    x = h5.get(tpath)
    if x.dtype.names:
        result = pd.DataFrame(
            [
                [
                    (
                        v[k].astype(str).strip()
                        if v.dtype.fields[k][0].name.startswith("bytes")
                        else v[k]
                    )
                    for k in v.dtype.fields
                ]
                for v in x
            ],
            columns=[name for name in x.dtype.names],
        )
        result = result.astype(
            dtype={
                k: "unicode" if x.dtype[k].name.startswith("bytes") else x.dtype[k].name
                for k in x.dtype.fields
            }
        )
    else:
        result = pd.DataFrame(
            [
                v.astype(str).strip() if x.dtype.name.startswith("bytes") else v
                for v in x
            ]
        )
        result = result.astype(
            dtype="unicode" if x.dtype.name.startswith("bytes") else x.dtype.name
        )
        result = strip(result)
    return result


def read_table_as_array(filename, table_path, dtype=str):
    """
    reads table from h5 filename from the table_path and returns array of dtype
    """
    with h5py.File(filename, "r") as f:
        return np.array(f[table_path]).astype(dtype)


def convert_time_to_table_slice(
    start_time, end_time, interval, table_start_time, table_time_length
):
    """
    start_time and end_time as convertable to to_datetime
    interval as convertable to Timedelta
    table_start_time convertable to_datetime
    table_time_length int
    """
    st = pd.to_datetime(start_time)
    et = pd.to_datetime(end_time)
    table_start_time = pd.to_datetime(table_start_time)
    interval = pd.Timedelta(interval)
    if et < st:
        raise "Start time: " + st + " is ahead of end time: " + et
    table_end_time = table_start_time + interval * table_time_length
    if st < table_start_time:
        st = table_start_time
    if et > table_end_time:
        et = table_end_time
    start_index = int((st - table_start_time) / interval)
    end_index = int((et - table_start_time) / interval)
    return slice(start_index, end_index, 1)


def read_attributes_from_table(data):
    #
    interval_string = decode_if_bytes(data.attrs["interval"][0])
    # FIXME: these conversions are HECDSS. Move them to pyhecdss utility function
    interval_string = interval_string.replace("hour", "h")
    interval_string = interval_string.replace("day", "D")
    interval_string = interval_string.replace("mon", "MS")
    model = decode_if_bytes(data.attrs["model"][0])
    model_version = decode_if_bytes(data.attrs["model_version"][0])
    if isinstance(data.attrs["start_time"], np.ndarray):
        start_time = pd.to_datetime(decode_if_bytes(data.attrs["start_time"][0]))
    else:
        start_time = pd.to_datetime(decode_if_bytes(data.attrs["start_time"]))
    return {
        "interval": interval_string,
        "model": model,
        "model_version": model_version,
        "start_time": start_time,
    }


def read_time_indexed_table(h5, table_path, timewindow=None, *other_indices):
    """
    returns a pandas DataFrame of time series from the table where
     - the first index is time (if None retrieves the entire time window)
        - specified as timewindow in the format of <<start time str>> - <<end time str>>, e.g. 01JAN1990 - 05JUL1992
     - second index are the ids
     - third index (if any) is the location identifier within the id aka 3rd dimension

    The attributes of the HDF5 data sets contains "start_time" and "interval" for evenly spaced data. This is
    used to infer the time window if none is given
    """
    data = h5.get(table_path)
    #
    attrs = read_attributes_from_table(data)
    #
    stime = pd.to_datetime(attrs["start_time"])
    # if start time and end time given use the slice
    if timewindow:
        twse = [s.strip() for s in timewindow.split("-")]
        timeSlice = convert_time_to_table_slice(
            twse[0], twse[1], attrs["interval"], attrs["start_time"], data.shape[0]
        )
    else:
        timeSlice = slice(None)
    if timeSlice.start:
        stime = stime + pd.Timedelta(attrs["interval"]) * timeSlice.start
    darr = data.__getitem__((timeSlice, *other_indices))
    df = pd.DataFrame(
        darr,
        index=pd.date_range(stime, freq=attrs["interval"], periods=darr.shape[0]),
        dtype=np.float32,
    )
    return df


# Utility funcs


def strip(df):
    """
    returns a striped copy of the str types of DataFrame
    """
    return df.map(lambda x: x.strip() if isinstance(x, str) else x)


def is_sequence_like(obj):
    tobj = type(obj)
    return hasattr(tobj, "__len__") and hasattr(tobj, "__getitem__")


def normalize_to_slice(str_or_seq):
    if isinstance(str_or_seq, str):
        return [str_or_seq]
    elif is_sequence_like(str_or_seq):
        return str_or_seq
    elif isinstance(str_or_seq, slice):
        return str_or_seq
    else:
        raise RuntimeError(
            "%s should be string, sequence of strings or slice: called with type: %s"
            % (str_or_seq, type(str_or_seq))
        )


def create_catalog_entry(
    filename, dfc, variable, unit, updown=True, prefix="CHAN_", id_column=0
):
    catalog = dfc.copy()
    catalog = catalog.rename(columns={id_column: "id"})
    catalog["id"] = prefix + catalog["id"].astype(str)
    catalog["variable"] = variable
    catalog["unit"] = unit
    catalog["filename"] = filename
    if updown:
        catalog_up = catalog.copy()
        catalog_down = catalog.copy()
        catalog_up["id"] = catalog_up["id"] + "_UP"
        catalog_down["id"] = catalog_down["id"] + "_DOWN"
        catalog = pd.concat([catalog_up, catalog_down])
    return catalog.reset_index(drop=True)


def decode_if_bytes(x):
    if isinstance(x, bytes):
        try:
            return x.decode("utf-8")
        except UnicodeDecodeError:
            return x.decode("utf-8", errors="replace")
    return x
