# Functions to help with DSM2 and GIS related tasks
import math
import pandas as pd
import geopandas as gpd
import click
import shapely
from shapely.geometry import Point, MultiLineString
from shapely.ops import nearest_points
from pydsm.input import parser


def find_closest_line_and_distance(point: Point, gdf):
    min_distance = float("inf")
    closest_multiline = None

    for idx, row in gdf.iterrows():
        # For each MultiLineString, find the closest point to the specified point
        closest_point = nearest_points(row["geometry"], point)[0]
        # Calculate the distance from the point to this closest point
        distance = point.distance(closest_point)
        # Update minimum distance and closest MultiLineString if this is the closest so far
        if distance < min_distance:
            min_distance = distance
            closest_multiline = row
    return closest_multiline, min_distance


def get_distance_from_start(point: Point, closest_line: gpd.GeoDataFrame):
    point_on_line = nearest_points(closest_line["geometry"], point)
    distance_from_start = closest_line["geometry"].project(point_on_line[0])
    return distance_from_start


def read_stations(file_path):
    """Read a stations CSV and return a UTM Zone 10N (EPSG:26910) GeoDataFrame.

    UTM coordinates (``x``/``y``, or ``utm_easting``/``utm_northing`` as
    written by ``dsm2ui datastore extract --stations``) are generally more
    accurate than the accompanying lat/lon columns -- station locations are
    often hand-verified/corrected in UTM without always re-deriving lat/lon.
    When both are present, UTM is used on a per-row basis; lat/lon is used as
    a fallback for rows where UTM is missing. Rows with neither a usable UTM
    nor lat/lon coordinate are dropped.
    """
    # Read the CSV file
    stations = pd.read_csv(file_path)

    if "x" in stations.columns and "y" in stations.columns:
        easting_col, northing_col = "x", "y"
    elif "utm_easting" in stations.columns and "utm_northing" in stations.columns:
        easting_col, northing_col = "utm_easting", "utm_northing"
    else:
        easting_col, northing_col = None, None
    has_utm = easting_col is not None and northing_col is not None
    has_lat_lon = "lat" in stations.columns and "lon" in stations.columns

    if not has_utm and not has_lat_lon:
        raise ValueError(
            "Input file must contain 'lat' and 'lon' columns or 'x'/'y' "
            "(or 'utm_easting'/'utm_northing') columns"
        )

    if has_lat_lon:
        geometry = gpd.GeoSeries(
            gpd.points_from_xy(stations.lon, stations.lat),
            index=stations.index,
            crs="EPSG:4326",
        ).to_crs(epsg=26910)
    else:
        geometry = gpd.GeoSeries(
            [None] * len(stations), index=stations.index, crs="EPSG:26910"
        )

    if has_utm:
        easting = pd.to_numeric(stations[easting_col], errors="coerce")
        northing = pd.to_numeric(stations[northing_col], errors="coerce")
        utm_mask = easting.notna() & northing.notna()
        if utm_mask.any():
            geometry.loc[utm_mask] = gpd.points_from_xy(
                easting[utm_mask], northing[utm_mask]
            )

    stations_utm = gpd.GeoDataFrame(stations, geometry=geometry, crs="EPSG:26910")
    stations_utm = stations_utm[stations_utm.geometry.notna()].reset_index(drop=True)
    return stations_utm


def get_id_and_distance_from_start(point, gdf):
    closest_line, dist_from_line = find_closest_line_and_distance(point, gdf)
    dist = get_distance_from_start(point, closest_line)
    if math.isclose(closest_line["geometry"].length, dist, abs_tol=1):
        dist = "LENGTH"
    else:
        dist = int(dist)
    return closest_line.id, dist, dist_from_line


def snap_stations_to_centerlines(stations_file, centerlines_file, distance_tolerance=100):
    """
    Snap each station (station_id, lat/lon or UTM) to the nearest DSM2 channel centerline.

    Parameters
    ----------
    stations_file : str
        Path to the stations file
    centerlines_file : str
        Path to the centerlines file
    distance_tolerance : int
        Maximum distance from a line that a station can be to be considered on that line
        default is 100 (feet, but depends if geojson file units are in feet or meters)

    Returns
    -------
    pandas.DataFrame
        Columns NAME, CHAN_NO, DISTANCE for stations matched within distance_tolerance.
        Stations that fail to match are skipped (a warning is printed for each).
    """
    centerlines = gpd.read_file(centerlines_file)
    stations = read_stations(stations_file)
    station_dist_tuple = []
    for _, station in stations.iterrows():
        id, dist, dist_from_line = get_id_and_distance_from_start(
            station["geometry"], centerlines
        )
        if dist_from_line > distance_tolerance:
            print(
                f"Station {station['station_id']} is not close enough to a line. Distance: {dist_from_line}, Closest line: {id}"
            )
        else:
            print(f"Station {station['station_id']} is on line {id} at distance {dist}")
            station_dist_tuple.append((station["station_id"], id, dist))
    return pd.DataFrame(station_dist_tuple, columns=["NAME", "CHAN_NO", "DISTANCE"])


def create_stations_output_file(
    stations_file, centerlines_file, output_file, distance_tolerance=100
):
    """
    Create DSM2 channels output compatible file for given stations info (station_id, lat lon)
    and centerlines geojson file (DSM2 channels centerlines) and writing out output_file

    The distance_tolerance is the maximum distance from a line that a station can be to be considered on that line

    The output file can be used to then create the channels file for DSM2 for these stations.
    Parameters
    ----------
    stations_file : str
        Path to the stations file
    centerlines_file : str
        Path to the centerlines file
    output_file : str
        Path to the output file
    distance_tolerance : int
        Maximum distance from a line that a station can be to be considered on that line
        default is 100 (feet, but depends if geojson file units are in feet or meters)
    """
    dfstation_dist = snap_stations_to_centerlines(
        stations_file, centerlines_file, distance_tolerance
    )
    print("Writing to hydro compatible format: ", output_file)
    dfstation_dist.to_csv(output_file, index=False, sep=" ")


def create_output_channel_inp(
    stations_file,
    centerlines_file,
    output_inp_file,
    variables=("flow", "stage"),
    interval="15MIN",
    period_op="inst",
    dss_file="${OUTPUTDSS}",
    distance_tolerance=100,
    append=False,
):
    """
    Snap stations to DSM2 channel centerlines and write a ready-to-use OUTPUT_CHANNEL
    table section to a DSM2 .inp file.

    Parameters
    ----------
    stations_file : str
        Path to the stations file (station_id, lat/lon or UTM columns)
    centerlines_file : str
        Path to the DSM2 channel centerlines GeoJSON
    output_inp_file : str
        Path to the .inp file to write the OUTPUT_CHANNEL section to
    variables : str or sequence of str
        DSM2 output variable(s), e.g. "flow", "stage", "ec". One row is written per
        station per variable.
    interval : str
        DSM2 output interval, e.g. "15MIN", "1HOUR" (or an ENVVAR like "${FINE_OUT}")
    period_op : str
        DSM2 period operation, "inst" or "ave"
    dss_file : str
        Output DSS path or ENVVAR, e.g. "${OUTPUTDSS}"
    distance_tolerance : int
        Maximum distance from a line that a station can be to be considered on that line
    append : bool
        If True, append the OUTPUT_CHANNEL section to an existing file; otherwise overwrite

    Returns
    -------
    pandas.DataFrame
        The OUTPUT_CHANNEL table that was written (NAME, CHAN_NO, DISTANCE, VARIABLE,
        INTERVAL, PERIOD_OP, FILE)
    """
    snapped = snap_stations_to_centerlines(
        stations_file, centerlines_file, distance_tolerance
    )
    if isinstance(variables, str):
        variables = [variables]
    snapped["NAME"] = snapped["NAME"].str.upper()
    rows = [
        {
            "NAME": row["NAME"],
            "CHAN_NO": row["CHAN_NO"],
            "DISTANCE": row["DISTANCE"],
            "VARIABLE": variable,
            "INTERVAL": interval,
            "PERIOD_OP": period_op,
            "FILE": dss_file,
        }
        for _, row in snapped.iterrows()
        for variable in variables
    ]
    output_table = pd.DataFrame(
        rows,
        columns=["NAME", "CHAN_NO", "DISTANCE", "VARIABLE", "INTERVAL", "PERIOD_OP", "FILE"],
    )
    print("Writing OUTPUT_CHANNEL section to: ", output_inp_file)
    parser.pretty_print(output_inp_file, output_table, "OUTPUT_CHANNEL", append=append)
    return output_table


@click.command()
@click.argument("dsm2_echo_file", type=click.Path(exists=True, readable=True))
@click.argument("centerlines_geojson_file", type=click.Path(exists=True, readable=True))
@click.argument("output_geojson_file", type=click.Path())
def geolocate_output_locations(
    dsm2_echo_file, centerlines_geojson_file, output_geojson_file
):
    """
    Create stations output file from DSM2 echo file and the centerlines for the channels
    and writing out output_file

    The output file is a geojson file with the names of the stations and their locations

    Returns the GeoDataFrame of the output stations

    Parameters
    ----------
    dsm2_echo_file : str
        Path to the DSM2 file
    centerlines_geojson_file : str
        Path to the centerlines file
    output_geojson_file : str
        Path to the output file
    """
    tables = parser.read_input(dsm2_echo_file)
    channels_table = tables["CHANNEL"]
    output_table = tables["OUTPUT_CHANNEL"]
    centerlines = gpd.read_file(centerlines_geojson_file).to_crs(epsg=26910)
    geometry = []
    for idx, row in output_table.iterrows():
        cline = centerlines[centerlines.id == row["CHAN_NO"]]
        channel = channels_table[channels_table["CHAN_NO"] == row["CHAN_NO"]]
        channel_length = channel["LENGTH"].values[0]
        if row["DISTANCE"].strip().upper() == "LENGTH":
            distance = 1
        else:
            distance = float(row["DISTANCE"]) / channel_length
        multi_line = cline.geometry.values[0]
        point = multi_line.interpolate(distance, normalized=True)
        geometry.append(point)
    dsm2_output_stations = output_table[["NAME", "CHAN_NO", "DISTANCE"]].copy()
    dsm2_output_stations = gpd.GeoDataFrame(
        dsm2_output_stations, geometry=geometry, crs="EPSG:26910"
    )
    dsm2_output_stations = dsm2_output_stations.drop_duplicates(
        subset=["NAME"]
    ).reset_index(drop=True)
    dsm2_output_stations.to_file(output_geojson_file, driver="GeoJSON")
    print("Writing to geojson format: ", output_geojson_file)
    return dsm2_output_stations
