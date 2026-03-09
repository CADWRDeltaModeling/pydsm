"""Utility functions for working with GTM/Qual HDF5 outputs.

This module centralizes helper functions that were previously defined inside
`tests/ex_gtmh5_restart.py` so that they can be reused across the codebase
and by users.
"""

from __future__ import annotations

from datetime import datetime
from typing import Tuple

import numpy as np
import pandas as pd

from . import qualh5

_DSM2_TIME_FORMAT = "%d%b%Y %H%M"


def _parse_dsm2_time(value) -> datetime:
    """Parse a DSM2 time string (DDMMMYYYY HHMM) to a naive ``datetime``.

    Tries explicit DSM2 format first, then falls back to ``pandas.to_datetime``.
    Accepts existing ``datetime`` / ``Timestamp`` objects and returns a
    built-in ``datetime`` (naive) for consistency.
    """
    if isinstance(value, (datetime, pd.Timestamp)):
        return pd.to_datetime(value).to_pydatetime()
    s = str(value).strip().upper()
    try:
        return datetime.strptime(s, _DSM2_TIME_FORMAT)
    except ValueError:
        return pd.to_datetime(s).to_pydatetime()


def _format_time(dt: datetime | str) -> str:
    """Format a datetime or parseable string into DSM2 style: ``DDMMMYYYY HHMM``.

    Examples
    --------
    >>> _format_time("1990-01-01 01:00")
    '01JAN1990 0100'
    """
    if isinstance(dt, str):
        try:
            ts = pd.to_datetime(dt, format=_DSM2_TIME_FORMAT)
        except ValueError:
            ts = pd.to_datetime(dt)
    else:
        ts = pd.to_datetime(dt)
    return ts.strftime(_DSM2_TIME_FORMAT).upper()


def _nearest_time(
    target_time: datetime | str,
    start_time: datetime | str,
    end_time: datetime | str,
    output_freq,
) -> datetime:
    """Return the model output timestamp nearest to ``target_time``.

    Parameters
    ----------
    target_time : datetime | str
        Desired target time.
    start_time, end_time : datetime | str
        Inclusive simulation time span.
    output_freq : pandas offset / frequency or string parseable by ``to_timedelta``
        Regular interval of outputs (e.g. '60min').

    Returns
    -------
    datetime
        The nearest output timestamp clamped within [start_time, end_time].
    """
    target = pd.to_datetime(target_time)
    start = pd.to_datetime(start_time)
    end = pd.to_datetime(end_time)

    if target <= start:
        return start.to_pydatetime()
    if target >= end:
        return end.to_pydatetime()

    try:
        freq_td = pd.to_timedelta(output_freq)
    except Exception as e:  # pragma: no cover - defensive
        raise ValueError(f"Unsupported frequency {output_freq}: {e}")

    if freq_td <= pd.Timedelta(0):  # pragma: no cover - sanity check
        raise ValueError("Non-positive frequency interval")

    elapsed = target - start
    steps_floor = int(elapsed // freq_td)
    cand1 = start + steps_floor * freq_td
    cand2 = cand1 + freq_td

    if cand2 > end:
        return cand1.to_pydatetime()

    return (
        cand1.to_pydatetime()
        if (target - cand1) <= (cand2 - target)
        else cand2.to_pydatetime()
    )


def build_timewindow_for_time(
    tidefile: str,
    time: datetime | str,
) -> Tuple[str, datetime]:
    """Determine nearest model time and build a single-interval timewindow.

    Parameters
    ----------
    tidefile : str
        Path to GTM/Qual HDF5 file.
    time : datetime | str
        Requested (approximate) time.

    Returns
    -------
    (timewindow, model_time)
        timewindow is 'START-END' using DSM2 format; model_time is datetime.
    """
    qualt = qualh5.QualH5(tidefile)
    output_freq = qualt.get_output_freq()
    start_date, end_date = qualt.get_start_end_dates()

    start_time = _parse_dsm2_time(start_date)
    end_time = _parse_dsm2_time(end_date)

    model_time = _nearest_time(time, start_time, end_time, output_freq)
    interval_end = model_time + output_freq

    timewindow = f"{_format_time(model_time)}-{_format_time(interval_end)}"
    return timewindow, model_time


def get_interpolated_cell_concentrations(
    tidefile: str,
    timewindow: str,
    constituent: str = "ec",
) -> np.ndarray:
    """Compute per-cell interpolated concentrations for a provided timewindow.

    Produces an array shape (1, total_cells) combining upstream and downstream
    concentrations with linear interpolation across channel cells.
    """
    qualt = qualh5.QualH5(tidefile)
    ct = qualt.get_input_table("/geometry/channel")
    channel_ids = list(qualt.get_channel_numbers().values.flatten())

    upconc = (
        qualt.get_channel_concentration(
            constituent, channel_ids, "upstream", timewindow
        )
        .values.flatten()
        .astype(float)
    )
    dnconc = (
        qualt.get_channel_concentration(
            constituent, channel_ids, "downstream", timewindow
        )
        .values.flatten()
        .astype(float)
    )

    num_channels = len(channel_ids)
    num_cells = int(ct["end_cell"].max())

    channel_cell_mask = np.zeros((num_channels, num_cells), dtype=float)
    interp_weights = np.zeros((num_channels, num_cells), dtype=float)

    for _, row in ct.iterrows():
        ch_num = int(row["channel_num"])
        ch_index = channel_ids.index(ch_num)
        start_cell = int(row["start_cell"])
        end_cell = int(row["end_cell"])
        ncells = end_cell - start_cell + 1
        factor = 1.0 / (ncells + 1)
        interp_weights[ch_index, start_cell - 1 : end_cell] = np.linspace(
            factor, 1.0, ncells, endpoint=False
        )
        channel_cell_mask[ch_index, start_cell - 1 : end_cell] = 1.0

    conc_matrix = (
        upconc[:, None] * channel_cell_mask
        + (dnconc - upconc)[:, None] * interp_weights
    )
    single_row = conc_matrix.max(axis=0, keepdims=True)
    return single_row


__all__ = [
    "_DSM2_TIME_FORMAT",
    "_parse_dsm2_time",
    "_format_time",
    "_nearest_time",
    "build_timewindow_for_time",
    "get_interpolated_cell_concentrations",
]
