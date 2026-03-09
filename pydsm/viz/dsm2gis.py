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
    # Read the CSV file
    stations = pd.read_csv(file_path)
    if "lat" in stations.columns and "lon" in stations.columns:
        # Create GeoDataFrame with Point geometry and set the CRS to WGS84 (EPSG:4326)
        stations = gpd.GeoDataFrame(
            stations,
            geometry=[Point(xy) for xy in zip(stations.lon, stations.lat)],
            crs="EPSG:4326",  # Define CRS during GeoDataFrame creation
        )
        stations.set_crs(epsg=4326, inplace=True)
        stations_utm = stations.to_crs(epsg=26910)
        stations.set_crs(epsg=4326, inplace=True)  # Set CRS again to avoid warning
    elif "x" in stations.columns and "y" in stations.columns:
        # Create GeoDataFrame with Point geometry and set the CRS to UTM Zone 10N (EPSG:26910)
        stations = gpd.GeoDataFrame(
            stations,
            geometry=[Point(xy) for xy in zip(stations.x, stations.y)],
            crs="EPSG:26910",  # Define CRS during GeoDataFrame creation
        )
        stations.set_crs(epsg=26910, inplace=True)
        stations.set_crs(epsg=26910, inplace=True)
        stations_utm = stations
    else:
        raise ValueError(
            "Input file must contain 'lat' and 'lon' columns or 'x' and 'y' columns"
        )
    return stations_utm


def get_id_and_distance_from_start(point, gdf):
    closest_line, dist_from_line = find_closest_line_and_distance(point, gdf)
    dist = get_distance_from_start(point, closest_line)
    if math.isclose(closest_line["geometry"].length, dist, abs_tol=1):
        dist = "LENGTH"
    else:
        dist = int(dist)
    return closest_line.id, dist, dist_from_line


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
    dfstation_dist = pd.DataFrame(
        station_dist_tuple, columns=["NAME", "CHAN_NO", "DISTANCE"]
    )
    print("Writing to hydro compatible format: ", output_file)
    dfstation_dist.to_csv(output_file, index=False, sep=" ")


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
