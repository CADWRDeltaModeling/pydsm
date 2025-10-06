"""Create a GTM restart file from a tide (Qual/GTM HDF5) file.

Generates a restart file containing:
 - Cell concentrations (interpolated per cell from channel end concentrations)
 - Reservoir concentrations
for a requested model time.

Example
-------
>>> write_gtm_restart(
...    tidefile="tests/data/hist_v82_mss2_extran_gtm.h5",
...    target_time="05FEB2020 0300",
...    outfile="restart_created.qrf",
...    constituent="ec",
... )

Output file format (illustrative):
30DEC2024 2400/time
   65744640 /julmin
         1 /n_column
      4279 /n_cell
                   cell_no                ec
                        1            685.4853218050435544
... (cells) ...
         9 /n_resv
              reservoir_name                ec
clifton_court                               416.6467054602688336
... (reservoirs) ...
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

import numpy as np

from .gtmh5 import (
    build_timewindow_for_time,
    get_interpolated_cell_concentrations,
    _format_time,
)
from .qualh5 import QualH5

_EPOCH_JULMIN = datetime(1899, 12, 31, 0, 0)  # Excel-like epoch (matches sample)


def _time_label_with_2400(dt: datetime) -> str:
    """Format time similar to DSM2/expected restart label.

    If the time is exactly on midnight (00:00) we represent it as previous day 2400.
    """
    if dt.hour == 0 and dt.minute == 0:
        prev = dt - timedelta(days=1)
        return prev.strftime("%d%b%Y").upper() + " 2400"
    return dt.strftime("%d%b%Y %H%M").upper()


def _compute_julmin(dt: datetime) -> int:
    """Compute julian minutes relative to the _EPOCH_JULMIN baseline.

    This matches the magnitude of values seen in existing restart samples.
    """
    return int((dt - _EPOCH_JULMIN).total_seconds() // 60)


def _format_cell_header(constituent: str) -> str:
    return f"{'cell_no':>29}{constituent:>20}              "


def _format_res_header(constituent: str) -> str:
    return f"{'reservoir_name':>29}{constituent:>20}              "


def write_gtm_restart(
    tidefile: str | Path,
    target_time: str | datetime,
    outfile: str | Path,
    constituent: str = "ec",
) -> Path:
    """Write a GTM restart file at the requested time.

    Parameters
    ----------
    tidefile : str | Path
       Path to GTM/Qual HDF5 file (tide file).
    target_time : str | datetime
       Desired time (will snap to nearest output time).
    outfile : str | Path
       Destination restart file path.
    constituent : str, default 'ec'
       Constituent to export.

    Returns
    -------
    Path to the written restart file.
    """
    tidefile = Path(tidefile)
    outfile = Path(outfile)
    qualt = QualH5(str(tidefile))

    # Determine timewindow and chosen model time
    timewindow, model_time = build_timewindow_for_time(str(tidefile), target_time)

    # End of interval (label uses interval end so that 2400 formatting matches sample)
    output_freq = qualt.get_output_freq()
    interval_end = model_time + output_freq

    # Gather cell concentrations
    cell_conc = get_interpolated_cell_concentrations(
        str(tidefile), timewindow, constituent=constituent
    ).ravel()
    n_cells = cell_conc.size

    # Gather reservoir concentrations
    res_df = qualt.get_reservoirs()
    res_names = list(res_df["name"].values)
    if res_names:
        res_conc_df = qualt.get_reservoir_concentration(
            constituent, res_names, timewindow=timewindow
        )
        # Single row expected
        res_conc = res_conc_df.iloc[0].values.astype(float)
    else:  # pragma: no cover - unlikely empty dataset
        res_conc = np.array([])
    n_res = len(res_conc)

    # Compose header
    time_label = _time_label_with_2400(interval_end)
    julmin = _compute_julmin(interval_end)

    lines: list[str] = []
    lines.append(f"{time_label}/time")
    lines.append(f"{julmin:12d} /julmin")
    lines.append(f"{1:12d} /n_column")
    lines.append(f"{n_cells:12d} /n_cell")
    lines.append(_format_cell_header(constituent))

    # Cell data lines: id (1-based) and value
    for i, val in enumerate(cell_conc, start=1):
        # Align cell number under 'cell_no' and value under constituent header
        lines.append(f"{i:32d}{val:32.16f}")

    lines.append(f"{n_res:12d} /n_resv")
    lines.append(_format_res_header(constituent))

    for name, val in zip(res_names, res_conc):
        lines.append(f"{name:<32}{val:>32.16f}")

    outfile.write_text("\n".join(lines) + "\n")
    return outfile


__all__ = ["write_gtm_restart"]


if __name__ == "__main__":  # rudimentary CLI usage
    import argparse

    parser = argparse.ArgumentParser(description="Create GTM restart file")
    parser.add_argument("tidefile", help="Path to GTM/Qual tide HDF5 file")
    parser.add_argument("target_time", help="Target time (e.g. '05FEB2020 0300')")
    parser.add_argument("outfile", help="Output restart file path")
    parser.add_argument(
        "-c", "--constituent", default="ec", help="Constituent name (default ec)"
    )
    args = parser.parse_args()

    path = write_gtm_restart(
        args.tidefile, args.target_time, args.outfile, constituent=args.constituent
    )
    print(f"Wrote restart file: {path}")
