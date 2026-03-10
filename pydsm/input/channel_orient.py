import pandas as pd
import geopandas as gpd
import shapely
import math
import numpy as np
from shapely.geometry import LineString, Point
from . import parser


def calculate_angle(p1, p2):
    """Calculate the angle (in degrees) of the line segment defined by two points (p1, p2)."""
    dx = p2[0] - p1[0]
    dy = p2[1] - p1[1]
    angle = math.degrees(math.atan2(dy, dx))
    return angle


def find_segment_and_angle(line, distance, normalized=True):
    """Find the segment of the line where the distance lies and calculate the angle of that segment."""
    coords = list(line.coords)  # Extract coordinates from the LineString
    total_distance = 0
    if normalized:
        distance = distance * line.length
    for i in range(len(coords) - 1):
        p1 = coords[i]
        p2 = coords[i + 1]

        # Calculate the length of the current segment
        segment_length = Point(p1).distance(Point(p2))

        if (
            total_distance + segment_length >= distance
        ):  # Check if the distance lies within this segment
            angle = calculate_angle(p1, p2)
            return {
                "segment_start": p1,
                "segment_end": p2,
                "angle": angle,
                "distance_within_segment": distance - total_distance,
            }

        total_distance += segment_length

    return None  # If the distance is longer than the length of the line


import click


@click.command()
@click.argument("channel_line_geojson_file")
@click.argument("hydro_echo_file")
@click.option("--channel_orient_file", default="channel_orient.inp")
def generate_channel_orientation(
    channel_line_geojson_file, hydro_echo_file, channel_orient_file="channel_orient.inp"
):
    """Generate a channel orientation (angle) file from a GeoJSON channel geometry and a Hydro echo file."""
    from pydsm.analysis import dsm2study

    hydro_tables = dsm2study.load_echo_file(hydro_echo_file)
    channel_lines = dsm2study.load_dsm2_flowline_shapefile(channel_line_geojson_file)
    channels = dsm2study.join_channels_info_with_dsm2_channel_line(
        channel_lines, hydro_tables
    )
    deltax = hydro_tables["SCALAR"].query('NAME=="deltax"').iloc[0].VALUE
    deltax = float(deltax)

    rows = []
    for i, row in channels.iterrows():
        line = row["geometry"]
        line = shapely.ops.linemerge(line)
        chan_length = float(row["LENGTH"])
        nsegments = math.ceil(chan_length / deltax)
        for norm_distance in np.linspace(0, 1, nsegments + 1):
            result = find_segment_and_angle(line, norm_distance, normalized=True)
            rows.append([row["CHAN_NO"], norm_distance, result["angle"]])

    df = pd.DataFrame(rows, columns=["CHAN_NO", "NORM_DISTANCE", "ANGLE"])
    df["NORM_DISTANCE"] = round(df["NORM_DISTANCE"], 3)
    df["ANGLE"] = round(df["ANGLE"], 2)
    with open(channel_orient_file, "w") as f:
        parser.write(f, {"CHANNEL_ORIENTATION": df})
