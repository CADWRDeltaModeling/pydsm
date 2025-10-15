import numpy as np
import pandas as pd
from io import StringIO
import matplotlib.pyplot as plt
from pathlib import Path
import pydsm
from pydsm.input.parser import read_input, write_input


def adjust_xsection_wetted_perimeter(df, channel_no, elev_range, pct_change):
    """
    Adjust the wetted perimeter of a channel cross-section  within the specified elevation range
    Simply increases the wetted perimeter by a percentage change at the layers within the elevation range.

    Parameters:
    channel_no : int
        Channel number to adjust
    elev_range : tuple
        Elevation range (min, max) to apply adjustment
        Elevation at which to apply wetted perimeter adjustment
    pct_change : float
        Percentage change to apply to wetted perimeter (e.g., 0.1 for 10% increase)

    Returns:
    --------
    pandas.DataFrame or None
        Modified cross-section data if output_path is None, otherwise None
    """
    # Filter by channel number
    all_channel_data = df[df["CHAN_NO"] == channel_no].copy()
    if all_channel_data.empty:
        raise ValueError(f"Channel number {channel_no} not found in the data")

    # channels with same channel number may have different distances, so loop over all distances available
    distances = all_channel_data["DIST"].unique()
    for dist in distances:
        channel_data = all_channel_data[all_channel_data["DIST"] == dist].copy()
        if channel_data.empty:
            continue

        # Sort by elevation
        channel_data = channel_data.sort_values(by="ELEV")

        # Find rows to adjust (at or above the specified elevation)
        if (
            elev_range[0] < channel_data["ELEV"].min()
            or elev_range[1] > channel_data["ELEV"].max()
        ):
            raise ValueError(
                f"Elevation range {elev_range} is outside the range of the data"
            )
        # Find the rows just below and above the adjustment elevation
        below_idx = channel_data[channel_data["ELEV"] < elev_range[0]].index[-1]
        above_idx = channel_data[channel_data["ELEV"] > elev_range[1]].index[0]

        # Interpolate width at the adjustment elevation
        below_elev = channel_data.loc[below_idx, "ELEV"]
        above_elev = channel_data.loc[above_idx, "ELEV"]
        below_wp = channel_data.loc[below_idx, "WET_PERIM"]
        above_wp = channel_data.loc[above_idx, "WET_PERIM"]
        # adjust wetted perimeter by the percentage change
        adjust_indices = channel_data[
            (channel_data["ELEV"] >= elev_range[0])
            & (channel_data["ELEV"] <= elev_range[1])
        ].index
        for idx in adjust_indices:
            current_wp = channel_data.loc[idx, "WET_PERIM"]
            channel_data.loc[idx, "WET_PERIM"] = current_wp * (1 + pct_change)

        all_channel_data.update(channel_data)
    # Update the original dataframe with the modified channel data
    df.update(all_channel_data)

    return df


def adjust_xsection_width(df, channel_no, elev_adjust, width_pct_change):
    """
    Adjust the width of a channel cross-section at a specific elevation and recalculate area and
    wetted perimeter for all elevations above the adjustment point.

    Parameters:
    channel_no : int
        Channel number to adjust
    elev_adjust : float
        Elevation at which to apply width adjustment
    width_pct_change : float
        Percentage change to apply to width (e.g., 0.1 for 10% increase)

    Returns:
    --------
    pandas.DataFrame or None
        Modified cross-section data if output_path is None, otherwise None
    """
    # Filter by channel number
    all_channel_data = df[df["CHAN_NO"] == channel_no].copy()
    if all_channel_data.empty:
        raise ValueError(f"Channel number {channel_no} not found in the data")
    # channels with same channel number may have different distances, so loop over all distances available
    distances = all_channel_data["DIST"].unique()
    for dist in distances:
        channel_data = all_channel_data[all_channel_data["DIST"] == dist].copy()
        if channel_data.empty:
            continue
        # Sort by elevation
        channel_data = channel_data.sort_values(by="ELEV")

        # Find rows to adjust (at or above the specified elevation)
        if (
            elev_adjust < channel_data["ELEV"].min()
            or elev_adjust > channel_data["ELEV"].max()
        ):
            raise ValueError(
                f"Adjustment elevation {elev_adjust} is outside the range of the data"
            )

        # If the exact elevation is not in the data, we need to interpolate
        exact_match = channel_data[channel_data["ELEV"] == elev_adjust]
        if exact_match.empty:
            # Find the rows just below and above the adjustment elevation
            below_idx = channel_data[channel_data["ELEV"] < elev_adjust].index[-1]
            above_idx = channel_data[channel_data["ELEV"] > elev_adjust].index[0]

            # Interpolate width at the adjustment elevation
            below_elev = channel_data.loc[below_idx, "ELEV"]
            above_elev = channel_data.loc[above_idx, "ELEV"]
            below_width = channel_data.loc[below_idx, "WIDTH"]
            above_width = channel_data.loc[above_idx, "WIDTH"]
            max_elev = channel_data["ELEV"].max()
            # Linear interpolation
            ratio = (elev_adjust - below_elev) / (above_elev - below_elev)
            interp_width = below_width + ratio * (above_width - below_width)

            # Calculate adjusted width
            adjusted_width = interp_width * (1 + width_pct_change)

            # Calculate width adjustment factor
            width_factor = adjusted_width / interp_width

            # Apply adjustment to all elevations above the adjustment point
            adjust_indices = channel_data[channel_data["ELEV"] >= elev_adjust].index

            # Interpolate adjustment factor for each elevation above
            for idx in adjust_indices:
                current_elev = channel_data.loc[idx, "ELEV"]
                # Linear scaling of the adjustment factor based on elevation
                if current_elev == elev_adjust:
                    # Direct adjustment at the above elevation
                    factor = width_factor
                else:
                    # Scale the elev_ratio from 0 to 1
                    elev_ratio = (current_elev - elev_adjust) / (max_elev - elev_adjust)
                    # Gradually reduce adjustment effect as we move up
                    factor = 1 + (width_factor - 1) * max(0, (1 - 0.5 * elev_ratio))

                channel_data.loc[idx, "WIDTH"] *= factor
        else:
            # Direct adjustment at the exact elevation
            adjust_idx = exact_match.index[0]
            channel_data.loc[adjust_idx, "WIDTH"] *= 1 + width_pct_change

            # Apply to all elevations above
            adjust_indices = channel_data[channel_data["ELEV"] > elev_adjust].index
            for idx in adjust_indices:
                channel_data.loc[idx, "WIDTH"] *= 1 + width_pct_change

        # Recalculate areas based on trapezoid formula
        elevs = channel_data["ELEV"].values
        widths = channel_data["WIDTH"].values

        # Skip the first row since we need a previous row to calculate area
        for i in range(1, len(channel_data)):
            h = elevs[i] - elevs[i - 1]  # Height difference
            avg_width = (widths[i] + widths[i - 1]) / 2  # Average width for trapezoid
            channel_data.iloc[i, channel_data.columns.get_loc("AREA")] = (
                channel_data.iloc[i - 1, channel_data.columns.get_loc("AREA")]
                + h * avg_width
            )

        # Recalculate wetted perimeter using pythagoras for each segment
        for i in range(1, len(channel_data)):
            h = elevs[i] - elevs[i - 1]  # Height difference
            w_diff = abs(widths[i] - widths[i - 1]) / 2  # Half-width difference
            segment_length = np.sqrt(h**2 + w_diff**2) * 2  # Both sides

            # Update wetted perimeter - this is approximate and could be refined
            prev_wet_perim = channel_data.iloc[
                i - 1, channel_data.columns.get_loc("WET_PERIM")
            ]
            new_wet_perim = prev_wet_perim + segment_length
            channel_data.iloc[i, channel_data.columns.get_loc("WET_PERIM")] = (
                new_wet_perim
            )
        # final check to ensure that area and widths and wetted perimeters are increasing with elevation or set to previous elelvation value
        for i in range(1, len(channel_data)):
            if (
                channel_data.iloc[i, channel_data.columns.get_loc("ELEV")]
                < channel_data.iloc[i - 1, channel_data.columns.get_loc("ELEV")]
            ):
                raise ValueError("Elevation values must be strictly increasing.")
            if (
                channel_data.iloc[i, channel_data.columns.get_loc("AREA")]
                < channel_data.iloc[i - 1, channel_data.columns.get_loc("AREA")]
            ):
                channel_data.iloc[i, channel_data.columns.get_loc("AREA")] = (
                    channel_data.iloc[i - 1, channel_data.columns.get_loc("AREA")]
                )
            if (
                channel_data.iloc[i, channel_data.columns.get_loc("WIDTH")]
                < channel_data.iloc[i - 1, channel_data.columns.get_loc("WIDTH")]
            ):
                channel_data.iloc[i, channel_data.columns.get_loc("WIDTH")] = (
                    channel_data.iloc[i - 1, channel_data.columns.get_loc("WIDTH")]
                )
            if (
                channel_data.iloc[i, channel_data.columns.get_loc("WET_PERIM")]
                < channel_data.iloc[i - 1, channel_data.columns.get_loc("WET_PERIM")]
            ):
                channel_data.iloc[i, channel_data.columns.get_loc("WET_PERIM")] = (
                    channel_data.iloc[i - 1, channel_data.columns.get_loc("WET_PERIM")]
                )
        all_channel_data.update(channel_data)
    # Update the original dataframe with the modified channel data
    df.update(all_channel_data)

    return df


# Example usage:
if __name__ == "__main__":
    input_file = "tests/data/test_xsection_data_all_dist.inp"
    output_file = "tests/data/test_xsection_data_all_dist_modified.inp"

    dflist = read_input(input_file)
    df = dflist["XSECT_LAYER"]
    # Adjust channel 130 at elevation 1.5 by increasing width 10%
    dfadj = adjust_xsection_width(
        df,
        channel_no=124,
        elev_adjust=4,
        width_pct_change=0.1,
    )
    dflist["XSECT_LAYER"] = dfadj
    # Write the modified data back to a file
    write_input(output_file, dflist, append=False)
    dfwpadj = adjust_xsection_wetted_perimeter(
        dfadj, channel_no=124, elev_range=(2, 5), pct_change=0.2
    )
    dflist["XSECT_LAYER"] = dfwpadj
    write_input(output_file, dflist, append=False)
