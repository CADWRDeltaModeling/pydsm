"""DSM2 study input differencer.

Compare two DSM2 studies by their hydro echo files, reporting:
- SCALAR / ENVVAR value changes
- Structural differences in all static input tables (added, removed, changed rows)
- Time-series input differences (RMSE, bias) for DSS-backed tables

Public API
----------
``DSM2Diff(echo_a, echo_b).run(...)`` → ``FullReport``

``FullReport.print_report()``  — render to stdout
``FullReport.to_csv(outdir)``  — write CSV artefacts
"""
from __future__ import annotations

import logging
import math
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import click
import numpy as np
import pandas as pd

from pydsm.analysis.dsm2study import (
    DSM2TimeSeriesReference,
    _load_dss_ts_table,
    get_runtime,
    load_echo_file,
    parse_military_date,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Table key definitions (columns that uniquely identify a row)
# ---------------------------------------------------------------------------
TABLE_KEYS: Dict[str, List[str]] = {
    "ENVVAR": ["NAME"],
    "SCALAR": ["NAME"],
    "IO_FILE": ["MODEL", "TYPE"],
    "CHANNEL": ["CHAN_NO"],
    "XSECT": ["CHAN_NO", "DIST"],
    "XSECT_LAYER": ["CHAN_NO", "DIST", "ELEV"],
    "RESERVOIR": ["NAME"],
    "RESERVOIR_VOL": ["RES_NAME", "ELEV"],
    "RESERVOIR_CONNECTION": ["RES_NAME", "NODE"],
    "GATE": ["NAME"],
    "GATE_WEIR_DEVICE": ["GATE_NAME", "DEVICE"],
    "GATE_PIPE_DEVICE": ["GATE_NAME", "DEVICE"],
    "TRANSFER": ["NAME"],
    "CHANNEL_IC": ["CHAN_NO", "DISTANCE"],
    "RESERVOIR_IC": ["RES_NAME"],
    "BOUNDARY_STAGE": ["NAME"],
    "BOUNDARY_FLOW": ["NAME"],
    "SOURCE_FLOW": ["NAME"],
    "SOURCE_FLOW_RESERVOIR": ["NAME"],
    "INPUT_GATE": ["GATE_NAME", "DEVICE", "VARIABLE"],
    "INPUT_TRANSFER_FLOW": ["TRANSFER_NAME"],
    "OPERATING_RULE": ["NAME"],
    "OPRULE_EXPRESSION": ["NAME"],
    "OPRULE_TIME_SERIES": ["NAME"],
    "OUTPUT_CHANNEL": ["NAME", "VARIABLE"],
    "OUTPUT_RESERVOIR": ["NAME", "VARIABLE"],
    "OUTPUT_GATE": ["NAME", "VARIABLE"],
}

# ---------------------------------------------------------------------------
# TS-backed tables: table name → name column used by _load_dss_ts_table
# None → special INPUT_GATE handling (composite key synthesised as NAME)
# ---------------------------------------------------------------------------
TS_TABLE_NAME_COL: Dict[str, Optional[str]] = {
    "BOUNDARY_FLOW": "NAME",
    "BOUNDARY_STAGE": "NAME",
    "SOURCE_FLOW": "NAME",
    "SOURCE_FLOW_RESERVOIR": "NAME",
    "INPUT_GATE": None,
    "INPUT_TRANSFER_FLOW": "TRANSFER_NAME",
    "OPRULE_TIME_SERIES": "NAME",
}

DEFAULT_TS_TABLES: Tuple[str, ...] = (
    "BOUNDARY_FLOW",
    "BOUNDARY_STAGE",
    "OPRULE_TIME_SERIES",
)

MAX_TS_DEFAULT: int = 25
RMSE_THRESHOLD_DEFAULT: float = 0.01


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _format_timewindow(start: datetime, end: datetime) -> str:
    return (
        start.strftime("%d%b%Y %H%M").upper()
        + " - "
        + end.strftime("%d%b%Y %H%M").upper()
    )


def _parse_timewindow(tw_str: str) -> Tuple[datetime, datetime]:
    """Parse a DSM2 timewindow string into ``(start, end)`` datetimes.

    Accepts ``'01JAN2020 0000 - 01JAN2022 0000'`` or ``'01JAN2020 - 01JAN2022'``.
    """
    if " - " in tw_str:
        parts = tw_str.split(" - ", 1)
    else:
        parts = [p.strip() for p in tw_str.split("-", 1)]

    def _ensure_time(s: str) -> str:
        s = s.strip()
        if len(s.split()) == 1:
            s += " 0000"
        return s

    return parse_military_date(_ensure_time(parts[0])), parse_military_date(
        _ensure_time(parts[1])
    )


def _idx_to_str(idx: pd.Index) -> pd.Index:
    """Cast index (single- or multi-level) to all-string for robust set operations."""
    if isinstance(idx, pd.MultiIndex):
        return pd.MultiIndex.from_arrays(
            [idx.get_level_values(i).astype(str) for i in range(idx.nlevels)],
            names=idx.names,
        )
    return idx.astype(str)


def _prep_ts_table(
    table_name: str, df: Optional[pd.DataFrame]
) -> Tuple[Optional[pd.DataFrame], str]:
    """Return *(preprocessed_df, name_col)* ready for :func:`_load_dss_ts_table`.

    For INPUT_GATE a synthetic ``NAME`` column is added from the composite
    ``GATE_NAME/DEVICE/VARIABLE`` key so that subsequent TS loading is uniform.
    """
    if df is None or df.empty:
        return df, TS_TABLE_NAME_COL.get(table_name) or "NAME"

    if table_name == "INPUT_GATE":
        df = df.copy()
        df["NAME"] = (
            df["GATE_NAME"].astype(str)
            + "/"
            + df["DEVICE"].astype(str)
            + "/"
            + df["VARIABLE"].astype(str)
        )
        return df, "NAME"

    return df, TS_TABLE_NAME_COL.get(table_name, "NAME")


# ---------------------------------------------------------------------------
# Result containers
# ---------------------------------------------------------------------------

@dataclass
class StaticDiff:
    """Structural difference between two instances of the same DSM2 input table.

    Attributes
    ----------
    table:   table name
    added:   rows present in study_b only (full row from B)
    removed: rows present in study_a only (full row from A)
    changed: rows with matching keys whose values differ; has ``*_a`` / ``*_b``
             value-column pairs plus a ``changed_cols`` column listing which
             columns actually changed.
    """

    table: str
    added: pd.DataFrame
    removed: pd.DataFrame
    changed: pd.DataFrame

    @property
    def has_diff(self) -> bool:
        return not (self.added.empty and self.removed.empty and self.changed.empty)

    def summary_line(self) -> str:
        parts = []
        if not self.added.empty:
            parts.append(f"+{len(self.added)} added")
        if not self.removed.empty:
            parts.append(f"-{len(self.removed)} removed")
        if not self.changed.empty:
            parts.append(f"~{len(self.changed)} changed")
        return ", ".join(parts) if parts else "no diff"


@dataclass
class TSDiff:
    """Time-series data comparison for one DSM2 TS-backed input table.

    Attributes
    ----------
    table:         table name
    summary:       DataFrame with columns
                   ``name | path_match | rmse | bias | n_points | skipped | skip_reason``
    missing:       DataFrame with columns ``name | present_in`` for entries
                   that exist in only one study (``'study_a'`` or ``'study_b'``)
    diff_series:   ``{name: DataFrame}`` mapping for every entry whose RMSE
                   exceeds the threshold; columns ``a | b | diff``,
                   DatetimeIndex (or integer index for constant entries)
    skipped_table: True when the whole table was skipped (e.g. > max_ts rows)
    skip_reason:   Human-readable reason for the table-level skip
    """

    table: str
    summary: pd.DataFrame
    missing: pd.DataFrame
    diff_series: Dict[str, pd.DataFrame]
    skipped_table: bool = False
    skip_reason: str = ""

    @property
    def has_diff(self) -> bool:
        if self.skipped_table:
            return False
        if not self.missing.empty:
            return True
        if not self.summary.empty and "rmse" in self.summary.columns:
            rmse_col = self.summary["rmse"].dropna()
            return bool((rmse_col > 0).any())
        return False


@dataclass
class FullReport:
    """Complete diff report for two DSM2 studies.

    Attributes
    ----------
    echo_a:        absolute path to the first echo file
    echo_b:        absolute path to the second echo file
    timewindow:    ``(start, end)`` effective comparison window
    static_diffs:  ``{table_name: StaticDiff}`` for every known table
    ts_diffs:      ``{table_name: TSDiff}`` for requested TS tables
    """

    echo_a: str
    echo_b: str
    timewindow: Tuple[datetime, datetime]
    static_diffs: Dict[str, StaticDiff]
    ts_diffs: Dict[str, TSDiff]

    # ------------------------------------------------------------------
    # Terminal output
    # ------------------------------------------------------------------

    def print_report(self) -> None:
        """Render the full diff report to stdout."""
        SEP = "=" * 72
        THIN = "-" * 72

        print(SEP)
        print("  DSM2 Study Diff")
        print(f"  A: {self.echo_a}")
        print(f"  B: {self.echo_b}")
        print(f"  Comparison window: {_format_timewindow(*self.timewindow)}")
        print(SEP)

        # --- Static table diffs ---
        tables_with_diff = [
            (n, sd) for n, sd in self.static_diffs.items() if sd.has_diff
        ]
        tables_clean = [
            n for n, sd in self.static_diffs.items() if not sd.has_diff
        ]
        print(
            f"\n[STATIC TABLES]  {len(tables_with_diff)} have differences, "
            f"{len(tables_clean)} are identical"
        )
        if tables_clean:
            print("  Identical:", ", ".join(tables_clean))

        for tname, sd in tables_with_diff:
            print(f"\n{THIN}")
            print(f"  {tname}  —  {sd.summary_line()}")
            print(THIN)
            if not sd.added.empty:
                print(f"  Added ({len(sd.added)} rows — present in study_b only):")
                print(_indent(sd.added.to_string(index=False)))
            if not sd.removed.empty:
                print(f"  Removed ({len(sd.removed)} rows — present in study_a only):")
                print(_indent(sd.removed.to_string(index=False)))
            if not sd.changed.empty:
                print(f"  Changed ({len(sd.changed)} rows — same key, different values):")
                print(_indent(sd.changed.to_string(index=False)))

        # --- TS diffs ---
        if self.ts_diffs:
            print(f"\n{SEP}")
            print("  TIME SERIES DATA COMPARISONS")
            print(SEP)
            for tname, ts in self.ts_diffs.items():
                print(f"\n{THIN}")
                print(f"  {tname}")
                print(THIN)
                if ts.skipped_table:
                    print(f"  SKIPPED: {ts.skip_reason}")
                    continue
                if not ts.missing.empty:
                    print(
                        f"  Missing ({len(ts.missing)} entries not in both studies):"
                    )
                    print(_indent(ts.missing.to_string(index=False)))
                if ts.summary.empty:
                    print("  No common entries to compare.")
                else:
                    display = _format_ts_summary(ts.summary)
                    print(
                        f"  Summary ({len(ts.summary)} entries compared):"
                    )
                    print(_indent(display.to_string(index=False)))

    # ------------------------------------------------------------------
    # CSV output
    # ------------------------------------------------------------------

    def to_csv(self, outdir: str) -> None:
        """Write CSV artefacts to *outdir*."""
        os.makedirs(outdir, exist_ok=True)
        written: List[str] = []

        # Static diffs — one file per (table, part) that has rows
        for tname, sd in self.static_diffs.items():
            for part, df in [
                ("added", sd.added),
                ("removed", sd.removed),
                ("changed", sd.changed),
            ]:
                if not df.empty:
                    path = os.path.join(outdir, f"{tname.lower()}_{part}.csv")
                    df.to_csv(path, index=False)
                    written.append(path)

        # TS diffs
        for tname, ts in self.ts_diffs.items():
            if ts.skipped_table:
                continue
            if not ts.missing.empty:
                path = os.path.join(outdir, f"{tname.lower()}_missing.csv")
                ts.missing.to_csv(path, index=False)
                written.append(path)
            if not ts.summary.empty:
                path = os.path.join(outdir, f"{tname.lower()}_ts_summary.csv")
                ts.summary.to_csv(path, index=False)
                written.append(path)
            for name, diff_df in ts.diff_series.items():
                safe_name = name.replace("/", "_").replace(" ", "_")
                path = os.path.join(outdir, f"{tname.lower()}_{safe_name}_diff.csv")
                diff_df.to_csv(path)
                written.append(path)

        if written:
            print(f"\nCSV files written to {os.path.abspath(outdir)}:")
            for p in written:
                print(f"  {p}")
        else:
            print("\nNo differences found; no CSV files written.")


# ---------------------------------------------------------------------------
# Internal helpers for StaticDiff computation
# ---------------------------------------------------------------------------

def _compute_static_diff(
    table_name: str,
    df_a: Optional[pd.DataFrame],
    df_b: Optional[pd.DataFrame],
    keys: List[str],
) -> StaticDiff:
    """Build a :class:`StaticDiff` by key-based comparison of two DataFrames."""
    empty = pd.DataFrame()

    n_a = len(df_a) if df_a is not None else 0
    n_b = len(df_b) if df_b is not None else 0

    if n_a == 0 and n_b == 0:
        return StaticDiff(table_name, empty, empty, empty)
    if n_a == 0:
        return StaticDiff(table_name, added=df_b.copy(), removed=empty, changed=empty)
    if n_b == 0:
        return StaticDiff(table_name, added=empty, removed=df_a.copy(), changed=empty)

    # Validate key columns
    missing_a = [k for k in keys if k not in df_a.columns]
    missing_b = [k for k in keys if k not in df_b.columns]
    if missing_a or missing_b:
        logger.warning(
            "Table %r: key columns missing — study_a: %s, study_b: %s",
            table_name,
            missing_a,
            missing_b,
        )
        return StaticDiff(table_name, empty, empty, empty)

    # Set index on key columns
    key = keys[0] if len(keys) == 1 else keys
    a_idx = df_a.set_index(key)
    b_idx = df_b.set_index(key)

    # Cast to string for robust set membership (avoids int/float type drift)
    a_idx.index = _idx_to_str(a_idx.index)
    b_idx.index = _idx_to_str(b_idx.index)

    only_a = a_idx.index.difference(b_idx.index)
    only_b = b_idx.index.difference(a_idx.index)
    common = a_idx.index.intersection(b_idx.index)

    added = b_idx.loc[only_b].reset_index() if len(only_b) else empty
    removed = a_idx.loc[only_a].reset_index() if len(only_a) else empty

    if len(common) == 0:
        return StaticDiff(table_name, added, removed, empty)

    a_common = a_idx.loc[common]
    b_common = b_idx.loc[common].reindex(a_common.index)

    # Compare only value columns present in both frames
    val_cols = sorted(set(a_common.columns) & set(b_common.columns))
    if not val_cols:
        return StaticDiff(table_name, added, removed, empty)

    # String-cast for type-robust cell comparison
    a_str = a_common[val_cols].fillna("").astype(str)
    b_str = b_common[val_cols].fillna("").astype(str)

    diff_mask = (a_str != b_str).any(axis=1)
    if not diff_mask.any():
        return StaticDiff(table_name, added, removed, empty)

    a_changed = a_common.loc[diff_mask]
    b_changed = b_common.loc[diff_mask].reindex(a_changed.index)

    # Identify which columns actually changed (for the changed_cols annotation)
    a_diff_str = a_str.loc[diff_mask]
    b_diff_str = b_str.loc[diff_mask].reindex(a_diff_str.index)
    col_diff_mask = a_diff_str != b_diff_str
    changed_cols_series = col_diff_mask.apply(
        lambda row: ",".join(c for c, v in row.items() if v), axis=1
    )

    # Build side-by-side with suffixed value columns; keys are restored by reset_index
    a_suffixed = a_changed.add_suffix("_a").reset_index()
    b_suffixed = b_changed.add_suffix("_b").reset_index()

    # After set_index + add_suffix + reset_index the key columns have no suffix
    changed = a_suffixed.merge(b_suffixed, on=keys)
    changed["changed_cols"] = changed_cols_series.values

    return StaticDiff(table_name, added, removed, changed)


# ---------------------------------------------------------------------------
# Print helpers
# ---------------------------------------------------------------------------

def _indent(text: str, prefix: str = "    ") -> str:
    return "\n".join(prefix + line for line in text.splitlines())


def _format_ts_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Return a display copy of a TS summary DataFrame with formatted floats."""
    display = df.copy()
    for col in ("rmse", "bias"):
        if col in display.columns:
            display[col] = display[col].map(
                lambda v: f"{v:.6f}"
                if (v is not None and not (isinstance(v, float) and math.isnan(v)))
                else "N/A"
            )
    return display


# ---------------------------------------------------------------------------
# Main differencer class
# ---------------------------------------------------------------------------

class DSM2Diff:
    """Compare two DSM2 studies identified by their hydro echo files.

    Parameters
    ----------
    echo_a, echo_b : str
        Paths to the two hydro echo (``*.inp``) files to compare.
    """

    def __init__(self, echo_a: str, echo_b: str) -> None:
        self.echo_a = os.path.abspath(echo_a)
        self.echo_b = os.path.abspath(echo_b)
        self.tables_a = load_echo_file(self.echo_a)
        self.tables_b = load_echo_file(self.echo_b)

    # ------------------------------------------------------------------
    # Time window
    # ------------------------------------------------------------------

    def get_effective_timewindow(
        self, override: Optional[str] = None
    ) -> Tuple[datetime, datetime]:
        """Return the effective comparison window.

        If *override* is given it is parsed directly.  Otherwise the
        intersection of both studies' ``run_start_date`` / ``run_end_date``
        scalars is used.

        Raises
        ------
        ValueError
            When the studies' run periods do not overlap.
        """
        if override is not None:
            return _parse_timewindow(override)

        start_a, end_a = get_runtime(self.tables_a)
        start_b, end_b = get_runtime(self.tables_b)
        start = max(start_a, start_b)
        end = min(end_a, end_b)
        if start >= end:
            raise ValueError(
                f"Studies have non-overlapping run periods: "
                f"A={start_a}–{end_a}, B={start_b}–{end_b}"
            )
        return start, end

    # ------------------------------------------------------------------
    # Static diff
    # ------------------------------------------------------------------

    def diff_static(self, table_name: str) -> StaticDiff:
        """Return a :class:`StaticDiff` for *table_name*.

        Returns an empty diff when the table name is not in :data:`TABLE_KEYS`.
        """
        keys = TABLE_KEYS.get(table_name)
        if keys is None:
            logger.warning("Unknown table %r; no key definition.", table_name)
            empty = pd.DataFrame()
            return StaticDiff(table_name, empty, empty, empty)

        return _compute_static_diff(
            table_name,
            self.tables_a.get(table_name),
            self.tables_b.get(table_name),
            keys,
        )

    # ------------------------------------------------------------------
    # TS diff
    # ------------------------------------------------------------------

    def diff_ts_table(
        self,
        table_name: str,
        timewindow: Optional[str] = None,
        threshold: float = RMSE_THRESHOLD_DEFAULT,
        max_ts: int = MAX_TS_DEFAULT,
        force: bool = False,
    ) -> TSDiff:
        """Compare DSS time-series data for every entry in *table_name*.

        Parameters
        ----------
        table_name : str
            Must be a key in :data:`TS_TABLE_NAME_COL`.
        timewindow : str, optional
            Comparison window; default is the effective intersection of run periods.
        threshold : float
            RMSE below this value is not considered a difference.
        max_ts : int
            Table rows above this count trigger a skip unless *force* is True.
        force : bool
            Load DSS data even when the table exceeds *max_ts* rows.
        """
        empty_df = pd.DataFrame()

        if table_name not in TS_TABLE_NAME_COL:
            logger.warning("Table %r is not a known TS table.", table_name)
            return TSDiff(
                table=table_name,
                summary=empty_df,
                missing=empty_df,
                diff_series={},
                skipped_table=True,
                skip_reason="Not a known TS table",
            )

        raw_a = self.tables_a.get(table_name)
        raw_b = self.tables_b.get(table_name)
        df_a, name_col = _prep_ts_table(table_name, raw_a)
        df_b, _ = _prep_ts_table(table_name, raw_b)

        n_a = len(df_a) if df_a is not None and not df_a.empty else 0
        n_b = len(df_b) if df_b is not None and not df_b.empty else 0
        max_rows = max(n_a, n_b)

        if max_rows > max_ts and not force:
            reason = f"table has {max_rows} rows > --max-ts {max_ts}; use --force to override"
            logger.warning("Skipping TS comparison for %r: %s", table_name, reason)
            return TSDiff(
                table=table_name,
                summary=empty_df,
                missing=empty_df,
                diff_series={},
                skipped_table=True,
                skip_reason=reason,
            )

        # Build name → DSM2TimeSeriesReference dicts
        refs_a: Dict[str, DSM2TimeSeriesReference] = {}
        refs_b: Dict[str, DSM2TimeSeriesReference] = {}

        if df_a is not None and not df_a.empty:
            try:
                refs_a = _load_dss_ts_table(df_a, name_col, self.echo_a)
            except Exception as exc:
                logger.warning(
                    "Failed to load TS refs study_a %r: %s", table_name, exc
                )

        if df_b is not None and not df_b.empty:
            try:
                refs_b = _load_dss_ts_table(df_b, name_col, self.echo_b)
            except Exception as exc:
                logger.warning(
                    "Failed to load TS refs study_b %r: %s", table_name, exc
                )

        names_a = set(refs_a)
        names_b = set(refs_b)

        # Missing rows
        missing_rows = (
            [{"name": n, "present_in": "study_a"} for n in sorted(names_a - names_b)]
            + [{"name": n, "present_in": "study_b"} for n in sorted(names_b - names_a)]
        )
        missing_df = pd.DataFrame(missing_rows) if missing_rows else empty_df

        # Compare common entries
        summary_rows: List[dict] = []
        diff_series: Dict[str, pd.DataFrame] = {}

        for name in sorted(names_a & names_b):
            ref_a = refs_a[name]
            ref_b = refs_b[name]
            path_match = ref_a.path == ref_b.path and ref_a.file == ref_b.file

            row: dict = {
                "name": name,
                "path_match": path_match,
                "rmse": None,
                "bias": None,
                "n_points": None,
                "skipped": False,
                "skip_reason": "",
            }

            try:
                data_a = ref_a.get_data(timewindow)
                data_b = ref_b.get_data(timewindow)

                if isinstance(data_a, float) and isinstance(data_b, float):
                    # Both constant entries
                    diff_val = data_a - data_b
                    row["rmse"] = abs(diff_val)
                    row["bias"] = diff_val
                    row["n_points"] = 1
                    if abs(diff_val) > threshold:
                        diff_series[name] = pd.DataFrame(
                            {"a": [data_a], "b": [data_b], "diff": [diff_val]}
                        )
                elif isinstance(data_a, float) or isinstance(data_b, float):
                    row["skipped"] = True
                    row["skip_reason"] = "mismatched types (constant vs time series)"
                else:
                    # Both time series — align on shared timestamps
                    aligned = pd.concat(
                        [data_a.rename("a"), data_b.rename("b")], axis=1
                    ).dropna()
                    if aligned.empty:
                        row["skipped"] = True
                        row["skip_reason"] = "no overlapping timestamps after alignment"
                    else:
                        diff = aligned["a"] - aligned["b"]
                        rmse = float(np.sqrt((diff**2).mean()))
                        bias = float(diff.mean())
                        row["rmse"] = rmse
                        row["bias"] = bias
                        row["n_points"] = len(aligned)
                        if rmse > threshold:
                            aligned["diff"] = diff
                            diff_series[name] = aligned
            except Exception as exc:
                row["skipped"] = True
                row["skip_reason"] = str(exc)

            summary_rows.append(row)

        summary_df = pd.DataFrame(summary_rows) if summary_rows else empty_df

        return TSDiff(
            table=table_name,
            summary=summary_df,
            missing=missing_df,
            diff_series=diff_series,
        )

    # ------------------------------------------------------------------
    # Full run
    # ------------------------------------------------------------------

    def run(
        self,
        ts_tables: Optional[List[str]] = None,
        timewindow: Optional[str] = None,
        threshold: float = RMSE_THRESHOLD_DEFAULT,
        max_ts: int = MAX_TS_DEFAULT,
        force: bool = False,
    ) -> FullReport:
        """Run the full diff and return a :class:`FullReport`.

        Parameters
        ----------
        ts_tables : list of str, optional
            TS-backed tables to compare with DSS data retrieval.
            Defaults to :data:`DEFAULT_TS_TABLES`.
        timewindow : str, optional
            Override comparison window.  Defaults to the intersection of both
            studies' run periods.
        threshold : float
            RMSE below this value is not flagged as a difference.
        max_ts : int
            Max rows in a TS table before skipping DSS loading.
        force : bool
            Load DSS data even when tables exceed *max_ts* rows.
        """
        requested_ts = [t.upper() for t in (ts_tables or list(DEFAULT_TS_TABLES))]

        tw_start, tw_end = self.get_effective_timewindow(timewindow)
        tw_str = _format_timewindow(tw_start, tw_end)

        # All tables present in either study + all tables with known keys
        all_known = (
            set(self.tables_a.keys())
            | set(self.tables_b.keys())
            | set(TABLE_KEYS.keys())
        )
        static_diffs: Dict[str, StaticDiff] = {
            tname: self.diff_static(tname)
            for tname in sorted(all_known)
            if tname in TABLE_KEYS
        }

        ts_diffs: Dict[str, TSDiff] = {}
        for tname in requested_ts:
            if tname not in TS_TABLE_NAME_COL:
                logger.warning(
                    "Table %r is not a recognised TS table; skipping.", tname
                )
                continue
            ts_diffs[tname] = self.diff_ts_table(
                tname, tw_str, threshold, max_ts, force
            )

        return FullReport(
            echo_a=self.echo_a,
            echo_b=self.echo_b,
            timewindow=(tw_start, tw_end),
            static_diffs=static_diffs,
            ts_diffs=ts_diffs,
        )


# ---------------------------------------------------------------------------
# Click command (registered in pydsm.cli)
# ---------------------------------------------------------------------------

@click.command("diff")
@click.argument("echo_a", type=click.Path(exists=True))
@click.argument("echo_b", type=click.Path(exists=True))
@click.option(
    "--tables",
    "-t",
    multiple=True,
    default=DEFAULT_TS_TABLES,
    show_default=True,
    help="DSS-backed tables to compare for time-series data. May be repeated.",
)
@click.option(
    "--all-tables",
    is_flag=True,
    default=False,
    help="Compare DSS data in all known TS-backed tables (overrides --tables).",
)
@click.option(
    "--timewindow",
    default=None,
    help=(
        'Comparison window e.g. "01JAN2020 0000 - 01JAN2022 0000". '
        "Default: intersection of both study run periods."
    ),
)
@click.option(
    "--threshold",
    default=RMSE_THRESHOLD_DEFAULT,
    type=float,
    show_default=True,
    help="RMSE below this value is not reported as a difference.",
)
@click.option(
    "--max-ts",
    default=MAX_TS_DEFAULT,
    type=int,
    show_default=True,
    help="Skip DSS loading for tables with more rows than this.",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Load DSS data even when a table exceeds --max-ts rows.",
)
@click.option(
    "--outdir",
    default=".",
    type=click.Path(),
    show_default=True,
    help="Directory for CSV output files.",
)
@click.option(
    "--no-csv",
    is_flag=True,
    default=False,
    help="Suppress CSV output; print report to terminal only.",
)
def dsm2_diff(
    echo_a,
    echo_b,
    tables,
    all_tables,
    timewindow,
    threshold,
    max_ts,
    force,
    outdir,
    no_csv,
):
    """Compare two DSM2 studies by their hydro echo files.

    Reports structural differences in all input tables and (optionally)
    computes RMSE / bias for DSS-backed time-series inputs.

    ECHO_A and ECHO_B are paths to the two hydro echo .inp files.
    """
    ts_tables = list(TS_TABLE_NAME_COL.keys()) if all_tables else list(tables)
    d = DSM2Diff(echo_a, echo_b)
    report = d.run(
        ts_tables=ts_tables,
        timewindow=timewindow,
        threshold=threshold,
        max_ts=max_ts,
        force=force,
    )
    report.print_report()
    if not no_csv:
        report.to_csv(outdir)
