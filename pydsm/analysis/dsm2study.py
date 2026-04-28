# A module to give a higher level view of a DSM2 study
import os
from datetime import datetime, timedelta
from functools import lru_cache
import numpy as np
import pandas as pd
import geopandas as gpd
import shapely

# our imports
import pyhecdss
from pydsm.input import parser, network
from pydsm.output import hydroh5
from vtools.functions.filter import godin

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def parse_military_date(date_str):
    if date_str.endswith("2400"):
        date_str = date_str.replace("2400", "0000")
        date_obj = datetime.strptime(date_str, "%d%b%Y %H%M")
        date_obj += timedelta(days=1)
    else:
        date_obj = datetime.strptime(date_str, "%d%b%Y %H%M")
    return date_obj


def load_echo_file(fname):
    with open(fname, "r") as file:
        df = parser.parse(file.read())
    return df


def abs_path(filename, hydro_echo_file, study_dir="../"):
    """
    builds absolute path to file using the path to the hydro_echo_file and the study_dir
    Assumes the default study directory to be one above the location of the hydro_echo_file
    """
    return os.path.normpath(
        os.path.join(os.path.dirname(hydro_echo_file), study_dir, filename)
    )


#
def get_hydro_tidefile(tables):
    """
    Get the hydro tidefile from the IO_FILE table in all tables list loaded by load_echo_file
    """
    io_file_table = tables["IO_FILE"]
    hdf5_file_io_row = io_file_table[io_file_table.TYPE == "hdf5"]
    return hdf5_file_io_row.iloc[0]["FILE"]


#
def get_in_out_edges(gc, nodeid):
    out_edges = []
    in_edges = []
    for n in gc.successors(nodeid):
        out_edges += list(gc[nodeid][n].values())
    for n in gc.predecessors(nodeid):
        in_edges += list(gc[n][nodeid].values())
    return {"in": in_edges, "out": out_edges}


def get_in_out_channel_numbers(gc, nodeid):
    ine, oute = get_in_out_edges(gc, nodeid).values()
    ine, oute = [e["CHAN_NO"] for e in ine], [e["CHAN_NO"] for e in oute]
    return ine, oute


#
def get_inflows_outflow(hydro, in_channels, out_channels, timewindow):
    """
    get the inflows and outflows corresponding to the list of channels for the given timewindow
    The in_channels and out_channels are assumed to be related by being in channel
    """
    in_flows = [
        hydro.get_channel_flow(
            cid, "upstream", timewindow="01JUL2008 0000 - 01AUG2008 0000"
        )
        for cid in in_channels
    ]
    out_flows = [
        hydro.get_channel_flow(
            cid, "downstream", timewindow="01JUL2008 0000 - 01AUG2008 0000"
        )
        for cid in out_channels
    ]


def get_data_for_source(sn):
    datasign = sn.sign
    dssfile = sn.file
    dsspath = sn.path
    return datasign * next(pyhecdss.get_ts(dssfile, dsspath))[0]


def load_dsm2_channelline_shapefile(channel_shapefile):
    return gpd.read_file(channel_shapefile).to_crs(epsg=26910)


def join_channels_info_with_dsm2_channel_line(dsm2_chan_lines, tables, geo_id="id"):
    return dsm2_chan_lines.merge(tables["CHANNEL"], right_on="CHAN_NO", left_on=geo_id)


def load_dsm2_flowline_shapefile(shapefile):
    dsm2_chans = gpd.read_file(shapefile).to_crs(epsg=26910)

    # First, filter out invalid geometries
    valid_mask = dsm2_chans.geometry.is_valid & dsm2_chans.geometry.notna()
    if not valid_mask.all():
        print(
            f"Warning: {(~valid_mask).sum()} invalid or null geometries found and will be excluded from simplification"
        )

    # Apply simplify only to valid geometries
    valid_indices = valid_mask[valid_mask].index
    if len(valid_indices) > 0:
        dsm2_chans.loc[valid_indices, "geometry"] = dsm2_chans.loc[
            valid_indices, "geometry"
        ].simplify(tolerance=50)
        dsm2_chans.loc[valid_indices, "geometry"] = dsm2_chans.loc[
            valid_indices, "geometry"
        ].buffer(250, cap_style=1, join_style=1)

    return dsm2_chans


def join_channels_info_with_shapefile(dsm2_chans, tables, geo_id="id"):
    return dsm2_chans.merge(tables["CHANNEL"], right_on="CHAN_NO", left_on=geo_id)


def load_dsm2_node_shapefile(node_shapefile):
    return gpd.read_file(node_shapefile).to_crs(epsg=3857)


def to_node_tuple_map(nodes):
    nodes = nodes.set_index(nodes["id"])
    nodes = nodes.drop(["geometry", "id"], axis=1)
    node_dict = nodes.to_dict(orient="index")
    return {k: (node_dict[k]["x"], node_dict[k]["y"]) for k in node_dict}


def get_location_on_channel_line(channel_id, distance, dsm2_chan_lines):
    """
    Returns a point on the channel line at the specified distance.

    Parameters
    ----------
    channel_id : int
        The channel ID to find
    distance : float or str
        Either a numeric distance or "LENGTH" for the full channel length
    dsm2_chan_lines : GeoDataFrame
        GeoDataFrame with channel geometries

    Returns
    -------
    shapely.geometry.Point
        Point on the channel line at specified distance, or Point(nan, nan) if not found
    """
    chan = dsm2_chan_lines[dsm2_chan_lines.CHAN_NO == channel_id]
    if chan.empty:
        logger.warning(f"Channel {channel_id} not found in the channel line shapefile")
        return pd.NA

    try:
        if isinstance(distance, str) and distance.upper() == "LENGTH":
            pt = chan.interpolate(1, normalized=True)
        else:
            pt = chan.interpolate(float(distance) / chan.LENGTH, normalized=True)
        return pt.values[0]  # Return the actual point object
    except Exception as e:
        logger.warning(f"Error calculating point on channel {channel_id}: {str(e)}")
        return pd.NA


def get_runtime(tables):
    scalars = tables["SCALAR"]
    rs = scalars[scalars["NAME"].str.contains("run")]
    tmap = dict(zip(rs["NAME"], rs["VALUE"]))
    stime = tmap["run_start_date"] + " " + tmap.get("run_start_time", "0000")
    etime = tmap["run_end_date"] + " " + tmap.get("run_end_time", "0000")
    return parse_military_date(stime), parse_military_date(etime)


class DSM2TimeSeriesReference:
    """
    Represents one row of a DSM2 time-series input table backed by a HEC-DSS file.

    Every DSM2 input table that drives time-varying boundary conditions (e.g.
    BOUNDARY_FLOW, SOURCE_FLOW, OPRULE_TIME_SERIES) has at least FILE and PATH
    columns.  This class wraps those columns together with the optional SIGN
    (+1/-1 multiplier) and FILLIN (interpolation hint) fields so that the raw
    data can be retrieved as a pandas Series via :meth:`get_data`.

    When ``file`` is the keyword ``"constant"`` (case-insensitive), ``path``
    holds the numeric constant value and :meth:`get_data` returns a float
    instead of a time series.

    Parameters
    ----------
    name : str
        Identifier—usually the NAME (or compound GATE_NAME/DEVICE/VARIABLE)
        column value from the parsed input table.
    file : str
        Absolute path to the HEC-DSS file, or the literal string ``"constant"``.
    path : str
        DSS pathname (``/A/B/C//E/F/``) or the numeric value when *file* is
        ``"constant"``.
    sign : float, optional
        Multiplier applied to the loaded series (default ``1.0``).  Use
        ``-1.0`` for diversions/exports whose measured values are positive but
        represent sinks.
    fillin : str, optional
        DSM2 fill-in strategy (``"last"``, ``"interp"``, ``"none"``).
        Informational only—pydsm does not apply fill-in logic itself.
    """

    def __init__(
        self,
        name: str,
        file: str,
        path: str,
        sign: float = 1.0,
        fillin: str = "last",
    ):
        self.name = name
        self.file = file
        self.path = path
        self.sign = float(sign)
        self.fillin = fillin

    def __repr__(self) -> str:
        fname = os.path.basename(self.file) if not self.is_constant else "constant"
        return (
            f"DSM2TimeSeriesReference(name={self.name!r}, file={fname!r}, "
            f"path={self.path!r}, sign={self.sign}, fillin={self.fillin!r})"
        )

    @property
    def is_constant(self) -> bool:
        """True when this entry uses the ``constant`` keyword in place of a DSS file."""
        return self.file.lower() == "constant"

    def get_data(self, timewindow: str = None):
        """
        Retrieve the time series as a pandas Series.

        For ``constant`` entries a plain float is returned (the constant value
        multiplied by *sign*).

        Parameters
        ----------
        timewindow : str, optional
            Time window string in DSM2 format, e.g.
            ``"01JAN2020 0000 - 01JAN2024 0000"``.  When provided the returned
            series is sliced to this window after loading.  Ignored for
            constant entries.

        Returns
        -------
        pandas.Series or float
            Raw DSS data multiplied by :attr:`sign`.
        """
        if self.is_constant:
            return float(self.path) * self.sign

        gen = pyhecdss.get_ts(self.file, self.path)
        result = next(gen, None)
        if result is None:
            raise ValueError(
                f"No data found in {os.path.basename(self.file)!r} for path {self.path!r}"
            )
        ts = result[0]
        # pyhecdss returns a single-column DataFrame; squeeze to Series so that
        # callers can call .rename("name") without triggering the callable-mapper path.
        if isinstance(ts, pd.DataFrame):
            ts = ts.iloc[:, 0]
        # pyhecdss sometimes returns a PeriodIndex; convert to DatetimeIndex so
        # that downstream callers can use .resample(), .loc[start:end], etc.
        if isinstance(ts.index, pd.PeriodIndex):
            ts.index = ts.index.to_timestamp()
        ts = self.sign * ts

        if timewindow is not None:
            # Prefer splitting on " - " to avoid ambiguity with date separators.
            if " - " in timewindow:
                start_str, end_str = timewindow.split(" - ", 1)
            else:
                start_str, end_str = timewindow.split("-", 1)
            start_str = start_str.strip()
            end_str = end_str.strip()
            # Append time component if only a date was given (DSM2 format).
            if len(start_str.split()) == 1:
                start_str += " 0000"
            if len(end_str.split()) == 1:
                end_str += " 0000"
            start = parse_military_date(start_str)
            end = parse_military_date(end_str)
            ts = ts.loc[start:end]

        return ts


def _load_dss_ts_table(
    table: pd.DataFrame,
    name_col: str,
    ref_file: str,
    study_dir: str = "../",
) -> "dict[str, DSM2TimeSeriesReference]":
    """
    Convert a parsed DSM2 input table into a ``{name: DSM2TimeSeriesReference}`` dict.

    Parameters
    ----------
    table : pandas.DataFrame
        Must have at least FILE and PATH columns.  SIGN and FILLIN are used
        when present.
    name_col : str
        Column whose values become both the dict key and ``DSM2TimeSeriesReference.name``.
    ref_file : str
        Absolute path to the echo file used by :func:`abs_path` for resolving
        relative FILE paths.
    study_dir : str, optional
        Passed to :func:`abs_path`; default ``"../"`` assumes the echo file
        lives inside an ``output/`` sub-directory of the study root.

    Returns
    -------
    dict[str, DSM2TimeSeriesReference]
    """
    result = {}
    has_sign = "SIGN" in table.columns
    has_fillin = "FILLIN" in table.columns
    for _, row in table.iterrows():
        name = str(row[name_col])
        file = str(row["FILE"])
        path = str(row["PATH"])
        sign = float(row["SIGN"]) if has_sign else 1.0
        fillin = str(row["FILLIN"]) if has_fillin else "last"
        if file.lower() != "constant":
            file = abs_path(file, ref_file, study_dir)
        result[name] = DSM2TimeSeriesReference(
            name=name, file=file, path=path, sign=sign, fillin=fillin
        )
    return result


class DSM2Study:

    def __init__(self, echo_file):
        self.echo_file = echo_file
        self.tables = load_echo_file(echo_file)
        # build network view
        self.gc = network.build_network_channels(self.tables)
        # get handle to hydro tidefile
        self.hydro_tidefile = abs_path(get_hydro_tidefile(self.tables), self.echo_file)
        self.hydro = hydroh5.HydroH5(self.hydro_tidefile)
        # replace file with absolute based on echo file location
        output_channels = self.tables["OUTPUT_CHANNEL"]
        output_channels["FILE"] = output_channels.apply(
            lambda r: abs_path(r["FILE"], self.echo_file), axis=1
        )
        # source flow from tidefile, so uses tidefile location for relative paths
        self.source_flow = self.hydro.get_input_table("/hydro/input/source_flow")
        self.source_flow["file"] = self.source_flow.apply(
            lambda r: abs_path(r["file"], self.hydro_tidefile), axis=1
        )
        # DSS-backed input time-series tables from the echo file
        self.boundary_flow = self._load_input_ts("BOUNDARY_FLOW", "NAME")
        self.boundary_stage = self._load_input_ts("BOUNDARY_STAGE", "NAME")
        self.source_flow_ts = self._load_input_ts("SOURCE_FLOW", "NAME")
        self.source_flow_reservoir = self._load_input_ts(
            "SOURCE_FLOW_RESERVOIR", "NAME"
        )
        self.input_gate = self._load_input_ts_gate()
        self.input_transfer_flow = self._load_input_ts(
            "INPUT_TRANSFER_FLOW", "TRANSFER_NAME"
        )
        self.oprule_time_series = self._load_input_ts("OPRULE_TIME_SERIES", "NAME")

    def _load_input_ts(self, table_name: str, name_col: str) -> "dict[str, DSM2TimeSeriesReference]":
        """
        Load a named input table from the parsed echo file as DSM2TimeSeriesReference objects.

        Returns an empty dict when the table is absent or empty.
        """
        table = self.tables.get(table_name)
        if table is None or table.empty:
            return {}
        return _load_dss_ts_table(table, name_col, self.echo_file)

    def _load_input_ts_gate(self) -> "dict[str, DSM2TimeSeriesReference]":
        """
        Load INPUT_GATE rows using a compound ``"GATE_NAME/DEVICE/VARIABLE"`` key.
        """
        table = self.tables.get("INPUT_GATE")
        if table is None or table.empty:
            return {}
        result = {}
        has_fillin = "FILLIN" in table.columns
        for _, row in table.iterrows():
            key = f"{row['GATE_NAME']}/{row['DEVICE']}/{row['VARIABLE']}"
            file = str(row["FILE"])
            path = str(row["PATH"])
            fillin = str(row["FILLIN"]) if has_fillin else "last"
            if file.lower() != "constant":
                file = abs_path(file, self.echo_file)
            result[key] = DSM2TimeSeriesReference(
                name=key, file=file, path=path, sign=1.0, fillin=fillin
            )
        return result

    def load_channelline_shapefile(self, channel_shapefile):
        self.dsm2_chan_lines = load_dsm2_channelline_shapefile(channel_shapefile)
        self.dsm2_chan_lines = join_channels_info_with_dsm2_channel_line(
            self.dsm2_chan_lines, self.tables
        )

    def get_runtime(self):
        return get_runtime(self.tables)

    def get_output_channels(self):
        return self.tables["OUTPUT_CHANNEL"]

    def get_inflows_outflows(self, nodeid: int, timewindow: str):
        """
        For the node id, get the in flows (upstream channels) and out flows (downstream channels)

        Parameters
        ----------
        nodeid : int
            The node id
        timewindow : str
            timewindow string as two times separted by "-", e.g. 01JAN2000 - 05APR2001

        Returns
        -------
        tuple of arrays
           a tuple of 2 arrays i.e. inflows, outflows
        """
        in_channels, out_channels = get_in_out_channel_numbers(self.gc, nodeid)
        in_flows = [
            self.hydro.get_channel_flow(cid, "upstream", timewindow=timewindow)
            for cid in in_channels
        ]
        out_flows = [
            self.hydro.get_channel_flow(cid, "downstream", timewindow=timewindow)
            for cid in out_channels
        ]
        return in_flows, out_flows

    def get_source_flows(self, nodeid: int):
        """
        get source flows

        Parameters
        ----------
        nodeid : int
            node id

        Returns
        -------
        array of pandas dataframes
            array of source flow time series for the node typically diversion, return, seepage
        """
        sflow_node = self.source_flow[self.source_flow["node"] == nodeid]
        return [get_data_for_source(sn) for _, sn in sflow_node.iterrows()]

    def get_net_source_flow(self, nodeid: int):
        """
        uses the sign information from source flow table to addup the net source/sink value

        Parameters
        ----------
        nodeid : int
            node id

        Returns
        -------
        pandas DataFrame
            net source flow time series
        """
        sdata = self.get_source_flows(nodeid)
        return sum([df.iloc[:, 0] for df in sdata])
