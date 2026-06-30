"""
Evaluate DSM2 OPERATING_RULE triggers and actions over a run period,
returning a time-indexed long-format DataFrame of gate states.

Public API
----------
get_gate_devices(tables) -> pd.DataFrame
    Structural information: GATE + GATE_WEIR_DEVICE + GATE_PIPE_DEVICE joined.

get_gate_state(echo_file, start, end, interval, hydro_file) -> pd.DataFrame
    Long-format DataFrame with columns:
    gate_name, device, variable, direction, value
    and a DatetimeIndex at the requested interval.
"""
import logging
import re

import numpy as np
import pandas as pd

from pydsm.input.parser import read_input
from pydsm.analysis.dsm2study import (
    get_runtime,
    parse_military_date,
    _load_dss_ts_table,
)
from pydsm.analysis.oprule_parser import (
    parse_trigger as _parse_trigger,
    parse_expr as _parse_expr,
    parse_action as _parse_action,
    eval_node as _eval_node,
    UnsupportedFeatureError,
    ParseError,
    needs_hydro as _needs_hydro_ast,
    _resample_series_to_index,
    _resample_to_index,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Compiled regular expressions (used by legacy _eval_chan_expr / _eval_chan_numeric)
# ---------------------------------------------------------------------------
_CHAN_FUNC_RE = re.compile(r"\b(chan_stage|chan_vel)\s*\(", re.IGNORECASE)
_CHAN_STAGE_RE = re.compile(
    r"chan_stage\(\s*channel\s*=\s*(\d+)\s*,\s*dist\s*=\s*(\d+)\s*\)", re.IGNORECASE
)
_CHAN_VEL_RE = re.compile(
    r"chan_vel\(\s*channel\s*=\s*(\d+)\s*,\s*dist\s*=\s*(\d+)\s*\)", re.IGNORECASE
)

# Maps DEFAULT_OP keyword → {direction: initial gate_op value}
_DEFAULT_OP_GATE_OP = {
    "gate_open": {"from_node": 1.0, "to_node": 1.0},
    "gate_close": {"from_node": 0.0, "to_node": 0.0},
    "unidir_from_node": {"from_node": 1.0, "to_node": 0.0},
    "unidir_to_node": {"from_node": 0.0, "to_node": 1.0},
}


# ---------------------------------------------------------------------------
# Time-series resampling helpers (implementations now live in oprule_parser)
# ---------------------------------------------------------------------------
# _resample_series_to_index and _resample_to_index are imported above.
# They remain available under the same names for backwards compatibility.


# ---------------------------------------------------------------------------
# Action string parsing
# ---------------------------------------------------------------------------


def _parse_action_str(action_str: str) -> list:
    """Parse a DSM2 OPERATING_RULE ACTION string into a list of action dicts.

    Delegates to :func:`oprule_parser.parse_action` and converts the
    returned :class:`~oprule_parser.ActionSpec` objects back into dicts for
    compatibility with :func:`_apply_rules_to_state`.
    """
    try:
        specs = _parse_action(action_str)
    except Exception as exc:
        logger.warning("Cannot parse action %r: %s", action_str, exc)
        return []
    return [
        {
            # gate_op/elev/etc: gate= kwarg holds the name.
            # ext_flow / ext_source: no gate= kwarg — use name= instead.
            "gate_name": s.kwargs.get("gate") or s.kwargs.get("name"),
            "device": s.kwargs.get("device"),
            "variable": s.variable,
            "direction": s.kwargs.get("direction"),
            "_value_ast": s.value_ast,
            "ramp_min": s.ramp_min,
        }
        for s in specs
    ]


# ---------------------------------------------------------------------------
# Value expression evaluation
# ---------------------------------------------------------------------------


def _eval_value_expr(value_expr_str: str, time_idx: pd.DatetimeIndex, ts_data: dict, _ts_cache: dict = None):
    """Evaluate an action value expression using the oprule_parser.

    Returns either a float scalar or a pd.Series aligned to *time_idx*.
    Falls back to 0.0 with a WARNING on any parse / eval error.
    """
    value_expr_str = value_expr_str.strip()
    try:
        ast = _parse_expr(value_expr_str)
        return _eval_node(ast, time_idx, ts_data, {}, None, {}, _ts_cache or {})
    except (UnsupportedFeatureError, ParseError, ValueError, KeyError) as exc:
        logger.warning("Cannot evaluate value expression %r: %s", value_expr_str, exc)
        return 0.0


# ---------------------------------------------------------------------------
# HDF5-backed channel expression helpers
# ---------------------------------------------------------------------------


def _eval_chan_numeric(
    expr_str: str, time_idx: pd.DatetimeIndex, hydro, _cache: dict = None
) -> pd.Series:
    """Evaluate a chan_stage / chan_vel arithmetic sub-expression (old regex path).

    This function is retained for internal use by ``_eval_chan_expr`` (the
    regex-based channel expression evaluator called directly from within the
    old ``_eval_trigger``).  New code should use the Pratt parser instead.
    """
    if _cache is None:
        _cache = {}
    expr_str = expr_str.strip()

    # Strip outer parentheses if the whole expression is parenthesized
    end_idx = _matching_paren_end(expr_str)
    if end_idx == len(expr_str) - 1:
        return _eval_chan_numeric(expr_str[1:-1].strip(), time_idx, hydro, _cache)

    # Top-level subtraction / addition
    for sep in (" - ", " + "):
        parts = _split_top_level(expr_str, sep)
        if parts and len(parts) == 2:
            lhs = _eval_chan_numeric(parts[0], time_idx, hydro, _cache)
            rhs = _eval_chan_numeric(parts[1], time_idx, hydro, _cache)
            return (lhs - rhs) if sep == " - " else (lhs + rhs)

    # chan_stage(channel=N, dist=D)
    m = _CHAN_STAGE_RE.match(expr_str)
    if m:
        channel_id = int(m.group(1))
        dist = int(m.group(2))
        # Use upstream for dist=0, downstream otherwise (approximation)
        location = "upstream" if dist == 0 else "downstream"
        cache_key = ("stage", channel_id, location)
        if cache_key not in _cache:
            df = hydro.get_channel_stage(str(channel_id), location)
            series = df.iloc[:, 0] if isinstance(df, pd.DataFrame) else df
            if isinstance(series.index, pd.PeriodIndex):
                series.index = series.index.to_timestamp()
            _cache[cache_key] = series
        return _resample_series_to_index(_cache[cache_key], time_idx)

    # chan_vel(channel=N, dist=D) — computed as flow / area
    m = _CHAN_VEL_RE.match(expr_str)
    if m:
        channel_id = int(m.group(1))
        dist = int(m.group(2))
        location = "upstream" if dist == 0 else "downstream"
        cache_key = ("vel", channel_id, location)
        if cache_key not in _cache:
            flow_df = hydro.get_channel_flow(str(channel_id), location)
            area_df = hydro.get_channel_area(str(channel_id), location)
            flow = flow_df.iloc[:, 0] if isinstance(flow_df, pd.DataFrame) else flow_df
            area = area_df.iloc[:, 0] if isinstance(area_df, pd.DataFrame) else area_df
            if isinstance(flow.index, pd.PeriodIndex):
                flow.index = flow.index.to_timestamp()
            if isinstance(area.index, pd.PeriodIndex):
                area.index = area.index.to_timestamp()
            _cache[cache_key] = flow / area.where(area != 0, other=np.nan)
        return _resample_series_to_index(_cache[cache_key], time_idx)

    raise ValueError(f"Cannot evaluate channel expression: {expr_str!r}")


def _eval_chan_expr(expr_str: str, time_idx: pd.DatetimeIndex, hydro, _cache: dict = None) -> pd.Series:
    """Evaluate a full boolean channel expression (e.g. ``chan_stage(...) > 0.3``)."""
    if _cache is None:
        _cache = {}
    expr_str = expr_str.strip()

    # Match comparison at the outermost level: LHS OP threshold
    # LHS may itself be parenthesized arithmetic.
    m = re.match(r"^(.*?)\s*(>=|<=|>|<|==)\s*([\d.+-]+)\s*$", expr_str, re.DOTALL)
    if not m:
        raise ValueError(
            f"Cannot parse channel expression comparison: {expr_str!r}"
        )
    lhs_str = m.group(1).strip()
    op_str = m.group(2)
    threshold = float(m.group(3))
    lhs_series = _eval_chan_numeric(lhs_str, time_idx, hydro, _cache)
    return _CMP_OPS[op_str](lhs_series, threshold)


# ---------------------------------------------------------------------------
# Trigger evaluation helpers
# ---------------------------------------------------------------------------


def _matching_paren_end(text: str) -> int:
    """Return the index of the closing parenthesis matching ``text[0]='('``.

    Returns -1 if text does not start with '(' or the parenthesis is unbalanced.
    """
    if not text.startswith("("):
        return -1
    depth = 0
    for i, c in enumerate(text):
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return i
    return -1


def _split_top_level(text: str, separator: str):
    """Split *text* on *separator* only at parenthesis depth 0.

    Returns a list of parts if the separator was found at depth 0, otherwise None.
    """
    depth = 0
    positions = []
    sep_len = len(separator)
    i = 0
    while i < len(text):
        c = text[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif depth == 0 and text[i : i + sep_len] == separator:
            positions.append(i)
            i += sep_len
            continue
        i += 1
    if not positions:
        return None
    parts = []
    prev = 0
    for pos in positions:
        parts.append(text[prev:pos].strip())
        prev = pos + sep_len
    parts.append(text[prev:].strip())
    return parts


def _needs_hydro(text: str, expressions: dict, _visited: set = None) -> bool:
    """Return True if *text* (or any named OPRULE_EXPRESSION it references) requires
    HDF5 data (i.e. contains ``chan_stage`` or ``chan_vel`` calls).

    Delegates to the AST-based implementation in oprule_parser.
    """
    return _needs_hydro_ast(text, expressions, _visited)


def _find_rules_needing_hydro(oprule_table: pd.DataFrame, expressions: dict) -> list:
    """Return a list of OPERATING_RULE names whose triggers need HDF5 data."""
    return [
        str(row["NAME"])
        for _, row in oprule_table.iterrows()
        if _needs_hydro(str(row["TRIGGER"]), expressions)
    ]


def _eval_trigger(
    trigger_str: str,
    time_idx: pd.DatetimeIndex,
    ts_data: dict,
    expressions: dict,
    hydro=None,
    _cache: dict = None,
    _ts_cache: dict = None,
) -> pd.Series:
    """Evaluate a DSM2 OPERATING_RULE TRIGGER string as a boolean pd.Series.

    Delegates to the Pratt parser in oprule_parser.  Falls back to
    ``Series(False)`` on any parse / eval error, logging a WARNING.
    """
    if _cache is None:
        _cache = {}
    if _ts_cache is None:
        _ts_cache = {}
    trigger_str = trigger_str.strip()

    try:
        ast = _parse_trigger(trigger_str)
        result = _eval_node(ast, time_idx, ts_data, expressions, hydro, _cache, _ts_cache)
    except UnsupportedFeatureError as exc:
        logger.warning("Skipping trigger (unsupported feature): %s", exc)
        return pd.Series(False, index=time_idx, dtype=bool)
    except Exception as exc:
        logger.warning("Cannot evaluate trigger %r: %s", trigger_str, exc)
        return pd.Series(False, index=time_idx, dtype=bool)

    # Normalise result to a boolean Series
    if isinstance(result, pd.Series):
        return result.astype(bool)
    return pd.Series(bool(result), index=time_idx, dtype=bool)


# ---------------------------------------------------------------------------
# Initial state construction
# ---------------------------------------------------------------------------


def _initial_state(tables: dict, time_idx: pd.DatetimeIndex, last_state: dict = None) -> dict:
    """Build the initial gate state for a time chunk.

    Returns a dict keyed by ``(gate_name, device, variable, direction)`` where
    values are ``numpy.ndarray`` of shape ``(len(time_idx),)`` initialised from
    the device tables.

    Parameters
    ----------
    last_state : dict | None
        When provided (subsequent chunks), scalar values from this dict
        override the DEFAULT_OP table values to carry state across chunk
        boundaries.  Pass ``None`` for the first chunk.
    """
    n = len(time_idx)
    state = {}

    # gate_install (gate-level, device=None)
    gate_table = tables.get("GATE", pd.DataFrame())
    for _, row in gate_table.iterrows():
        key = (row["NAME"], None, "gate_install", None)
        init_val = last_state.get(key, 1.0) if last_state else 1.0
        state[key] = np.full(n, init_val, dtype=float)

    for device_table_name in ("GATE_WEIR_DEVICE", "GATE_PIPE_DEVICE"):
        dev_table = tables.get(device_table_name, pd.DataFrame())
        for _, row in dev_table.iterrows():
            gname = row["GATE_NAME"]
            dev = row["DEVICE"]
            default_op = str(row.get("DEFAULT_OP", "gate_open")).lower()

            # gate_op: one entry per direction
            op_map = _DEFAULT_OP_GATE_OP.get(
                default_op, {"from_node": 1.0, "to_node": 1.0}
            )
            for direction, default_val in op_map.items():
                key = (gname, dev, "gate_op", direction)
                if key not in state:
                    init_val = last_state.get(key, default_val) if last_state else default_val
                    state[key] = np.full(n, init_val, dtype=float)

            # gate_elev: static geometry (may be overridden by rules)
            elev = row.get("ELEV", np.nan)
            default_val = float(elev) if elev is not None else np.nan
            key = (gname, dev, "gate_elev", None)
            init_val = last_state.get(key, default_val) if last_state else default_val
            state[key] = np.full(n, init_val, dtype=float)

            # gate_nduplicate
            ndup = row.get("NDUPLICATE", 1)
            default_val = float(ndup) if ndup is not None else 1.0
            key = (gname, dev, "gate_nduplicate", None)
            init_val = last_state.get(key, default_val) if last_state else default_val
            state[key] = np.full(n, init_val, dtype=float)

            # gate_width only for weir devices
            if device_table_name == "GATE_WEIR_DEVICE":
                width = row.get("WIDTH", np.nan)
                default_val = float(width) if width is not None else np.nan
                key = (gname, dev, "gate_width", None)
                init_val = last_state.get(key, default_val) if last_state else default_val
                state[key] = np.full(n, init_val, dtype=float)

    return state


def _extract_last_state(state: dict) -> dict:
    """Return the last scalar value for each state array (for next chunk init)."""
    return {key: float(arr[-1]) for key, arr in state.items() if len(arr) > 0}


def _apply_rules_to_state(
    state: dict,
    chunk_idx: pd.DatetimeIndex,
    oprule_table: "pd.DataFrame",
    ts_data: dict,
    expressions: dict,
    hydro,
    hydro_cache: dict,
    ts_cache: dict,
) -> None:
    """Apply OPERATING_RULE rows to *state* for *chunk_idx* (mutates *state* in-place)."""
    for _, row in oprule_table.iterrows():
        rule_name = str(row["NAME"])
        trigger_str = str(row["TRIGGER"]).strip()
        action_str = str(row["ACTION"]).strip()

        try:
            trigger_mask = _eval_trigger(
                trigger_str, chunk_idx, ts_data, expressions, hydro, hydro_cache, ts_cache
            )
        except Exception as exc:
            logger.warning(
                "Skipping rule %r — error evaluating trigger: %s", rule_name, exc
            )
            continue

        if not trigger_mask.any():
            continue

        actions = _parse_action_str(action_str)
        for act in actions:
            key = (act["gate_name"], act["device"], act["variable"], act["direction"])
            # Evaluate value using the pre-parsed AST stored by _parse_action_str
            value_ast = act.get("_value_ast")
            try:
                if value_ast is not None:
                    vals = _eval_node(
                        value_ast, chunk_idx, ts_data, expressions, hydro, hydro_cache, ts_cache
                    )
                else:
                    # Fallback: should not happen if _parse_action_str succeeded
                    vals = _eval_value_expr(act.get("value_expr", "0"), chunk_idx, ts_data, ts_cache)
            except Exception as exc:
                logger.warning(
                    "Skipping action in rule %r — error evaluating value: %s",
                    rule_name,
                    exc,
                )
                continue

            if key not in state:
                # gate_coef, gate_height, and any other variables not in the
                # default tables start as NaN (unknown until first rule fires)
                state[key] = np.full(len(chunk_idx), np.nan, dtype=float)

            mask_arr = trigger_mask.values
            if isinstance(vals, pd.Series):
                state[key][mask_arr] = vals.values[mask_arr]
            else:
                state[key][mask_arr] = float(vals)


def _entity_ds_path(variable: str, device: str, direction: str) -> str:
    """Return the HDF5 dataset path within ``/gate_state`` for a given entity key."""
    parts = [variable]
    if device:
        parts.append(device)
        if direction:
            parts.append(direction)
    return "/".join(parts)


def _append_chunk_to_hdf(
    chunk_df: pd.DataFrame,
    output_file: str,
    mode: str,
    start_time_str: str = "",
    interval_str: str = "",
) -> None:
    """Write *chunk_df* to an HDF5 file in wide-format TIMESERIES style.

    Creates one dataset per ``(variable, device, direction)`` combination,
    shaped ``(n_timesteps, n_gates)``, matching the DSM2 HYDRO TIMESERIES
    layout used for ``channel flow`` and ``channel stage``.

    On the first write (``mode='w'``) the file is created and all datasets
    are initialised from the first chunk.  Subsequent calls (``mode='a'``)
    extend each dataset along the time axis.
    """
    import h5py

    df = chunk_df.copy()
    for col in ("gate_name", "device", "variable", "direction"):
        if col in df.columns:
            df[col] = df[col].where(df[col].notna(), other="").astype(str)
    df = df[df["variable"] != ""]
    if df.empty:
        return

    h5_mode = "w" if mode == "w" else "a"
    with h5py.File(output_file, h5_mode) as h5f:
        root = h5f.require_group("gate_state")

        for (var, dev, direc), grp_df in df.groupby(
            ["variable", "device", "direction"]
        ):
            # Pivot: rows = timestamps, cols = gate_name (alphabetically sorted)
            pivot = grp_df.pivot_table(
                index=grp_df.index,
                columns="gate_name",
                values="value",
                aggfunc="first",
            )
            pivot = pivot.reindex(sorted(pivot.columns), axis=1)
            data = pivot.to_numpy(dtype=np.float32)  # (n_t, n_gates)

            ds_path = _entity_ds_path(var, dev, direc)
            gate_names_np = np.array(
                [gn.encode("utf-8") for gn in pivot.columns], dtype="S64"
            )

            if ds_path not in root:
                n_t, n_g = data.shape
                ds = root.create_dataset(
                    ds_path,
                    data=data,
                    maxshape=(None, n_g),
                    dtype=np.float32,
                    chunks=(min(720, n_t), max(1, n_g)),
                    compression="gzip",
                    compression_opts=5,
                )
                ds.attrs["gate_names"] = gate_names_np
                ds.attrs["CLASS"] = np.bytes_("GATE_STATE")
                ds.attrs["start_time"] = np.bytes_(start_time_str)
                ds.attrs["interval"] = np.bytes_(interval_str)
            else:
                ds = root[ds_path]
                # Reorder columns to match stored gate_names order
                stored_names = [
                    b.decode("utf-8") for b in ds.attrs["gate_names"]
                ]
                if list(pivot.columns) != stored_names:
                    pivot = pivot.reindex(columns=stored_names)
                    data = pivot.to_numpy(dtype=np.float32)
                n_existing = ds.shape[0]
                ds.resize((n_existing + data.shape[0], ds.shape[1]))
                ds[n_existing:, :] = data


def read_gate_state_hdf5(path: str) -> pd.DataFrame:
    """Read a wide-format gate_state HDF5 file back into a long-format DataFrame.

    This is the inverse of the wide-format writer in :func:`get_gate_state`.
    Each dataset under ``/gate_state`` has shape ``(n_timesteps, n_gates)``
    and attrs ``gate_names``, ``start_time``, ``interval``.

    Parameters
    ----------
    path : str
        Path to the gate_state HDF5 file produced by :func:`get_gate_state`.

    Returns
    -------
    pd.DataFrame
        Long-format DataFrame with DatetimeIndex (name=``"timestamp"``) and
        columns ``gate_name``, ``device``, ``variable``, ``direction``,
        ``value``.
    """
    import h5py

    frames = []

    def _visit(name, obj):
        if not isinstance(obj, h5py.Dataset):
            return
        if "gate_names" not in obj.attrs:
            return

        # Parse path parts back into (variable, device, direction)
        parts = name.split("/")
        variable = parts[0]
        device = parts[1] if len(parts) > 1 else ""
        direction = parts[2] if len(parts) > 2 else ""

        gate_names = [b.decode("utf-8") for b in obj.attrs["gate_names"]]
        start_time = obj.attrs.get("start_time", b"").decode("utf-8")
        interval = obj.attrs.get("interval", b"1h").decode("utf-8")

        data = obj[:]  # (n_timesteps, n_gates)
        n_t = data.shape[0]
        timestamps = pd.date_range(start=start_time, periods=n_t, freq=interval)

        for i, gate_name in enumerate(gate_names):
            s = pd.Series(
                data[:, i],
                index=timestamps,
                name="value",
                dtype="float64",
            )
            df = s.reset_index()
            df.columns = ["timestamp", "value"]
            df["gate_name"] = gate_name
            df["device"] = device
            df["variable"] = variable
            df["direction"] = direction
            df = df.set_index("timestamp")
            frames.append(
                df[["gate_name", "device", "variable", "direction", "value"]]
            )

    with h5py.File(path, "r") as h5f:
        h5f["gate_state"].visititems(_visit)

    if not frames:
        return pd.DataFrame(
            columns=["gate_name", "device", "variable", "direction", "value"]
        )
    return pd.concat(frames, sort=False)


# ---------------------------------------------------------------------------
# State → DataFrame conversion
# ---------------------------------------------------------------------------


def _state_to_dataframe(state: dict, time_idx: pd.DatetimeIndex) -> pd.DataFrame:
    """Convert the internal state dict to a long-format DataFrame."""
    if not state:
        return pd.DataFrame(
            columns=["gate_name", "device", "variable", "direction", "value"]
        )

    gate_names, devices, variables, directions, values_list = [], [], [], [], []
    for (gate_name, device, variable, direction), arr in state.items():
        n = len(arr)
        gate_names.extend([gate_name] * n)
        devices.extend([device] * n)
        variables.extend([variable] * n)
        directions.extend([direction] * n)
        values_list.extend(arr.tolist())

    timestamps = list(time_idx) * len(state)
    # Reconstruct the index in the correct interleaved order
    idx = []
    for arr in state.values():
        idx.extend(list(time_idx))

    result = pd.DataFrame(
        {
            "gate_name": gate_names,
            "device": devices,
            "variable": variables,
            "direction": directions,
            "value": values_list,
        },
        index=pd.DatetimeIndex(idx, name="timestamp"),
    )
    return result


# ---------------------------------------------------------------------------
# OPRULE_TIME_SERIES loader
# ---------------------------------------------------------------------------


def _load_oprule_ts(tables: dict, echo_file: str) -> dict:
    """Load OPRULE_TIME_SERIES rows into a dict of DSM2TimeSeriesReference objects."""
    table = tables.get("OPRULE_TIME_SERIES")
    if table is None or table.empty:
        return {}
    return _load_dss_ts_table(table, "NAME", echo_file)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_gate_devices(tables: dict) -> pd.DataFrame:
    """Return a combined DataFrame of all gate devices (weir + pipe) joined with GATE.

    Parameters
    ----------
    tables : dict[str, pd.DataFrame]
        Parsed DSM2 tables from :func:`pydsm.input.parser.read_input`.

    Returns
    -------
    pd.DataFrame
        Columns include GATE_NAME, DEVICE, device_type, DEFAULT_OP plus
        FROM_OBJ, FROM_IDENTIFIER, TO_NODE from the GATE table.
    """
    gate = tables.get("GATE", pd.DataFrame())

    weir = tables.get("GATE_WEIR_DEVICE", pd.DataFrame()).copy()
    if not weir.empty:
        weir["device_type"] = "weir"

    pipe = tables.get("GATE_PIPE_DEVICE", pd.DataFrame()).copy()
    if not pipe.empty:
        pipe["device_type"] = "pipe"

    devices = pd.concat([weir, pipe], sort=False).reset_index(drop=True)

    if not gate.empty and not devices.empty:
        devices = devices.merge(
            gate, left_on="GATE_NAME", right_on="NAME", how="left"
        ).drop(columns=["NAME"], errors="ignore")

    return devices


def get_gate_state(
    echo_file: str,
    start=None,
    end=None,
    interval: str = "1h",
    hydro_file: str = None,
    chunk_size: int = 720,
    output_file: str = None,
):
    """Evaluate DSM2 OPERATING_RULE triggers and actions over the run period.

    Processes the time window in *chunk_size* steps to bound memory use.
    State (gate_op, gate_install, …) is carried across chunk boundaries so
    the result is identical to a single-pass evaluation.

    Parameters
    ----------
    echo_file : str
        Path to the DSM2 hydro echo (``.inp``) file.
    start : datetime | None
        Start of the evaluation period.  Defaults to ``run_start_date`` from
        the SCALAR table.
    end : datetime | None
        End of the evaluation period.  Defaults to ``run_end_date`` from the
        SCALAR table.
    interval : str, optional
        pandas frequency string for the output time index.  Default ``"1h"``.
    hydro_file : str | None
        Path to the DSM2 hydro HDF5 tidefile.  Required when any
        OPERATING_RULE trigger involves ``chan_stage`` or ``chan_vel``.
    chunk_size : int, optional
        Number of time-steps per processing chunk.  Default 720 (≈30 days at
        1-hour resolution).  Larger values use more memory; smaller values
        reduce peak memory at the cost of slightly more overhead.
    output_file : str | None
        When provided, each chunk is appended to this HDF5 file at key
        ``/gate_state`` using pandas table format, then freed from memory.
        The function returns ``None`` in this case.  When ``None`` all chunks
        are concatenated in memory and the full DataFrame is returned.

    Returns
    -------
    pd.DataFrame or None
        Long-format DataFrame with a DatetimeIndex (name=``"timestamp"``) and
        columns ``gate_name``, ``device``, ``variable``, ``direction``,
        ``value``.  Returns ``None`` when *output_file* is supplied.

    Raises
    ------
    ValueError
        If any trigger requires HDF5 data and *hydro_file* is not provided.
    """
    tables = read_input(echo_file)

    if start is None or end is None:
        t_start, t_end = get_runtime(tables)
        if start is None:
            start = t_start
        if end is None:
            end = t_end

    time_idx = pd.date_range(start, end, freq=interval)

    # Load OPRULE_TIME_SERIES references (lazy — data read on first .get_data())
    ts_data = _load_oprule_ts(tables, echo_file)

    # Build OPRULE_EXPRESSION name → definition mapping
    expr_table = tables.get("OPRULE_EXPRESSION", pd.DataFrame())
    expressions = (
        dict(zip(expr_table["NAME"], expr_table["DEFINITION"]))
        if not expr_table.empty
        else {}
    )

    # Pre-scan: collect rules that need HDF5 and raise early if not provided
    oprule_table = tables.get("OPERATING_RULE", pd.DataFrame())
    if not oprule_table.empty:
        needing_hydro = _find_rules_needing_hydro(oprule_table, expressions)
        if needing_hydro and hydro_file is None:
            raise ValueError(
                "The following OPERATING_RULE triggers require a hydro HDF5 file "
                f"(chan_stage or chan_vel): {needing_hydro}. "
                "Provide hydro_file=<path_to_h5>."
            )

    # Open HDF5 if provided
    hydro = None
    if hydro_file is not None:
        from pydsm.output.hydroh5 import HydroH5
        hydro = HydroH5(hydro_file)

    # Shared caches for expensive reads — persist across ALL chunks so each
    # channel/ts is only read from disk once per get_gate_state() call.
    hydro_cache: dict = {}  # (kind, channel_id, location) → pd.Series
    ts_cache: dict = {}     # ts_name → pd.Series

    n_total = len(time_idx)
    last_state: dict = None   # None → first chunk uses DEFAULT_OP
    accumulated: list = []    # used when output_file is None
    hdf_mode = "w"            # 'w' for first chunk, 'a' for subsequent

    for chunk_start in range(0, n_total, chunk_size):
        chunk_idx = time_idx[chunk_start: chunk_start + chunk_size]

        # Build state arrays for this chunk, seeded with end-of-previous-chunk
        state = _initial_state(tables, chunk_idx, last_state=last_state)

        if not oprule_table.empty:
            _apply_rules_to_state(
                state, chunk_idx, oprule_table,
                ts_data, expressions, hydro, hydro_cache, ts_cache,
            )

        # Save end-of-chunk scalar state for next chunk's initialisation
        last_state = _extract_last_state(state)

        chunk_df = _state_to_dataframe(state, chunk_idx)
        del state  # release chunk-size arrays before next iteration

        if output_file is not None:
            _append_chunk_to_hdf(
                chunk_df,
                output_file,
                hdf_mode,
                start_time_str=str(time_idx[0]) if hdf_mode == "w" else "",
                interval_str=interval if hdf_mode == "w" else "",
            )
            hdf_mode = "a"
            del chunk_df  # free memory immediately after writing
        else:
            accumulated.append(chunk_df)

    if output_file is not None:
        logger.info("Gate state written to: %s", output_file)
        return None

    if not accumulated:
        return pd.DataFrame(
            columns=["gate_name", "device", "variable", "direction", "value"]
        )
    return pd.concat(accumulated, sort=False)
