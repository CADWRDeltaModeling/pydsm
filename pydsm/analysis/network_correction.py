"""Network-aware IDW correction of DSM2 channel-end values using sparse observations.

This module provides tools to bias-correct DSM2 model output (e.g. EC) at every
channel-end using a sparse set of observation stations, via Inverse Distance
Weighting (IDW) on a directed network graph.

The correction is additive:

    corrected(x, t) = model(x, t) + Σ w_i(x) · r_i(t)  /  Σ w_i(x)

where r_i(t) = obs_i(t) − model(x_i, t) is the residual at station i, and
w_i(x) = d(i → x)^(−power) is the IDW weight based on directed network distance
from the observation node to the target channel-end node.

Directed distances follow the UPNODE → DOWNNODE orientation of DSM2 channels
(approximately the tidally-filtered flow direction).  Channel-ends that are
upstream of all active observations receive zero correction.

When observations are NaN (missing) they are excluded automatically; if all
observations are missing the model values are returned unchanged.

Typical workflow::

    from pydsm.output.hydroh5 import HydroH5
    from pydsm.output.qualh5 import QualH5
    from pydsm.analysis.network_correction import (
        extract_channel_end_values,
        snap_stations_to_channel_ends,
        NetworkIDWCorrector,
    )
    import geopandas as gpd
    import pandas as pd

    # 1. Load model channel-end EC for the full run
    qual = QualH5("historical_v82_ec.h5")
    model_df = extract_channel_end_values(qual, "ec")

    # 2. Snap observation stations to channel-ends
    hydro = HydroH5("historical_v82.h5")
    channels_df = hydro.get_channels()
    stations = read_stations("obs_stations.csv")        # from pydsm.viz.dsm2gis
    centerlines = gpd.read_file("channel_centerlines.geojson")
    snapped = snap_stations_to_channel_ends(stations, centerlines, channels_df)

    # 3. Build corrector (pre-computes distance matrix once)
    corrector = NetworkIDWCorrector(channels_df, snapped, power=2)

    # 4. Apply correction at each time step
    for t, model_row in model_df.iterrows():
        obs = pd.Series({"RSAC075": measured_ec_at_t, "RSAN007": float("nan")})
        corrected = corrector.correct(model_row, obs)
"""

import abc
import warnings

import networkx as nx
import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------


class NetworkCorrector(abc.ABC):
    """Abstract base for network-aware additive correction of DSM2 channel-end values.

    Both :class:`NetworkIDWCorrector` (inverse-distance weighting on a directed
    graph) and :class:`NetworkOICorrector` (optimal interpolation on an
    undirected graph) implement this interface.

    Subclasses pre-compute all expensive data at construction time so that
    :meth:`correct` is cheap enough to call at every animation time step.
    """

    @abc.abstractmethod
    def correct(
        self, model_series: "pd.Series", observations: "pd.Series"
    ) -> "pd.Series":
        """Apply additive correction to one time step of channel-end values.

        Parameters
        ----------
        model_series : pd.Series
            Channel-end model values indexed by ``"{chan_no}-{location}"``.
        observations : pd.Series
            Observed values indexed by ``station_id``.  ``NaN`` entries are
            treated as missing and excluded automatically.

        Returns
        -------
        pd.Series
            Corrected values with the same index and dtype as *model_series*.
            When no valid observations are available the method returns an
            unchanged copy of *model_series*.
        """


# ---------------------------------------------------------------------------
# Kernel factories for NetworkOICorrector
# ---------------------------------------------------------------------------


def exponential_kernel(length_scale: "float | None" = None):
    """Return a symmetric exponential correlation kernel.

    The kernel computes::

        corr = exp(−total_path_length / length_scale)

    This is the default kernel for :class:`NetworkOICorrector`.  It is
    guaranteed to produce a symmetric positive-definite covariance matrix
    because the exponential function is a positive-definite kernel on any
    metric space.

    Parameters
    ----------
    length_scale : float or None
        Correlation length in feet.  When ``None`` (default) the caller
        (typically :class:`NetworkOICorrector`) auto-estimates the scale
        from the mean DISPERSION value and the M2 tidal period.

    Returns
    -------
    callable
        ``corr_fn(total_length_ft: float) -> float`` in ``[0, 1]``.
    """
    if length_scale is not None and length_scale <= 0:
        raise ValueError("length_scale must be positive.")
    _L = length_scale  # captured in closure; may be None (resolved by caller)

    def _kernel(total_length_ft: float) -> float:
        L = _L
        if L is None:
            raise RuntimeError(
                "length_scale was None when corr_fn was called; resolve it "
                "before use (NetworkOICorrector does this automatically)."
            )
        return float(np.exp(-total_length_ft / L))

    _kernel._length_scale = length_scale  # type: ignore[attr-defined]
    _kernel._kind = "exponential"         # type: ignore[attr-defined]
    return _kernel


def channel_direction_kernel(
    length_scale: "float | None" = None,
    resistance: float = 3.0,
):
    """Return an asymmetry-aware correlation kernel using CHANNEL table flow direction.

    Each path segment is classified as *with-flow* (traversed UPNODE→DOWNNODE)
    or *against-flow* (DOWNNODE→UPNODE).  An effective distance is computed
    as the geometric mean of the forward and reverse directed costs::

        d_fwd = Σ L_k × (1 if with-flow, resistance if against-flow)
        d_rev = Σ L_k × (resistance if with-flow, 1 if against-flow)
        d_sym = sqrt(d_fwd × d_rev)
        corr  = exp(−d_sym / length_scale)

    The geometric-mean symmetrisation guarantees a symmetric covariance matrix.

    .. note::
        This kernel **cannot** distinguish an upstream neighbour from a
        downstream neighbour at equal total path length.  For single-direction
        paths of length L the effective distance is always ``L × sqrt(resistance)``
        regardless of direction.  The asymmetry only manifests for paths that
        mix with-flow and against-flow segments — such paths receive a *larger*
        effective distance than uniformly-directed paths of the same total length.
        For a fully physics-based asymmetric kernel see the Green's-function
        approach (requires the hydro HDF5 file; not yet implemented).

    Parameters
    ----------
    length_scale : float or None
        Correlation length in feet (same as :func:`exponential_kernel`).
    resistance : float
        Cost multiplier for against-flow traversal.  ``1.0`` → symmetric
        (same as :func:`exponential_kernel`); higher values increasingly
        penalise mixed-direction paths.  Default ``3.0``.

    Returns
    -------
    callable
        ``corr_fn(segments: list[tuple[str, float]]) -> float``
        where each element is ``(chan_no, signed_length_ft)``:
        positive = UPNODE→DOWNNODE, negative = DOWNNODE→UPNODE.
    """
    if resistance < 1.0:
        raise ValueError("resistance must be >= 1.0 (1.0 = symmetric).")
    if length_scale is not None and length_scale <= 0:
        raise ValueError("length_scale must be positive.")
    _L = length_scale
    _r = resistance

    def _kernel(segments: "list[tuple[str, float]]") -> float:
        L = _L
        if L is None:
            raise RuntimeError(
                "length_scale was None when corr_fn was called; resolve it "
                "before use (NetworkOICorrector does this automatically)."
            )
        d_fwd = sum(abs(s) * (1.0 if s >= 0 else _r) for _, s in segments)
        d_rev = sum(abs(s) * (_r if s >= 0 else 1.0) for _, s in segments)
        d_sym = float(np.sqrt(d_fwd * d_rev))
        return float(np.exp(-d_sym / L))

    _kernel._length_scale = length_scale  # type: ignore[attr-defined]
    _kernel._kind = "channel_direction"   # type: ignore[attr-defined]
    _kernel._resistance = resistance      # type: ignore[attr-defined]
    return _kernel


# ---------------------------------------------------------------------------
# Helper: build normalised CHANNEL DataFrame
# ---------------------------------------------------------------------------


def _normalise_channels(channels_df: "pd.DataFrame") -> "pd.DataFrame":
    """Return channels_df with lowercase columns and typed numeric columns."""
    norm = channels_df.rename(columns=lambda c: c.lower()).copy()
    if "length" not in norm.columns:
        raise ValueError(
            "channels_df must contain a 'length' or 'LENGTH' column."
        )
    norm["chan_no"]  = norm["chan_no"].astype(str)
    norm["upnode"]   = norm["upnode"].astype(int)
    norm["downnode"] = norm["downnode"].astype(int)
    norm["length"]   = norm["length"].astype(float)
    return norm


def _auto_length_scale(norm: "pd.DataFrame") -> float:
    """Estimate a sensible correlation length from DISPERSION and tidal period.

    Uses ``sqrt(mean_dispersion × T_M2)`` where T_M2 = 44700 s (M2 tidal
    constituent period).  For typical Delta DISPERSION values of 200–500 ft²/s
    this gives roughly 3 000–4 700 ft (~1–1.4 km).
    """
    T_M2 = 44700.0  # seconds
    if "dispersion" in norm.columns:
        mean_D = float(norm["dispersion"].mean())
    else:
        mean_D = 360.0  # fallback: DSM2 default dispersion (ft²/s)
    return float(np.sqrt(mean_D * T_M2))


def extract_channel_end_values(qual_h5, constituent, time_window=None):
    """Return all channel-end concentrations from a QualH5 file.

    Calls ``get_channel_concentration`` for both the upstream and downstream
    ends of every channel and concatenates the results into a single DataFrame.

    Parameters
    ----------
    qual_h5 : QualH5
        Open QualH5 (or GtmH5) file.
    constituent : str
        Constituent name as stored in the file (e.g. ``"ec"``).
    time_window : str or None
        Optional DSM2 time-window string, e.g. ``"05JAN1990 - 11JAN1990"``.

    Returns
    -------
    pd.DataFrame
        Time-indexed DataFrame.  Columns are ``"{chan_no}-upstream"`` and
        ``"{chan_no}-downstream"`` for every channel in the file, in index
        order (upstream columns first, then downstream).
    """
    df_up = qual_h5.get_channel_concentration(constituent, "all", "upstream", time_window)
    df_dn = qual_h5.get_channel_concentration(constituent, "all", "downstream", time_window)
    return pd.concat([df_up, df_dn], axis=1)


def snap_stations_to_channel_ends(
    stations_gdf, centerlines_gdf, channels_df, max_snap_distance=None
):
    """Project observation stations onto the nearest channel-end in the network.

    Each station is projected perpendicularly onto the nearest channel
    centerline.  The station is then assigned to the **upstream** channel-end
    when the projected point falls in the first half of the centerline, and to
    the **downstream** channel-end otherwise.

    Parameters
    ----------
    stations_gdf : geopandas.GeoDataFrame
        Observation station points.  Must have a ``station_id`` column and
        ``Point`` geometry.  CRS must match *centerlines_gdf*.
    centerlines_gdf : geopandas.GeoDataFrame
        Channel centerline geometries with an ``id`` column whose values match
        ``chan_no`` values in *channels_df*.
    channels_df : pd.DataFrame
        CHANNEL table from ``HydroH5.get_channels()`` or
        ``parser.parse_input()["CHANNEL"]``.  Required columns
        (case-insensitive): ``chan_no``, ``upnode``, ``downnode``.
    max_snap_distance : float or None
        Maximum perpendicular distance from a centerline (in the CRS units of
        *centerlines_gdf*) at which a station is considered on that channel.
        Stations beyond this threshold emit a warning and are skipped.
        ``None`` means no limit.

    Returns
    -------
    pd.DataFrame
        Indexed by ``station_id``.  Columns:

        - ``chan_no``            — channel number (string)
        - ``location``          — ``"upstream"`` or ``"downstream"``
        - ``node_id``           — integer network node (``UPNODE`` or
          ``DOWNNODE``)
        - ``distance_fraction`` — 0–1 fraction along the centerline where
          the station projects
    """
    from pydsm.viz.dsm2gis import (
        find_closest_line_and_distance,
        get_distance_from_start,
    )

    norm = channels_df.rename(columns=lambda c: c.lower())
    chan_lookup = {
        str(row["chan_no"]): (int(row["upnode"]), int(row["downnode"]))
        for _, row in norm.iterrows()
    }

    records = []
    for _, station in stations_gdf.iterrows():
        closest_line, dist_from_line = find_closest_line_and_distance(
            station.geometry, centerlines_gdf
        )

        if max_snap_distance is not None and dist_from_line > max_snap_distance:
            warnings.warn(
                f"Station '{station['station_id']}' is {dist_from_line:.1f} units "
                f"from the nearest channel (threshold {max_snap_distance}). Skipping.",
                UserWarning,
                stacklevel=2,
            )
            continue

        chan_no = str(closest_line["id"])
        if chan_no not in chan_lookup:
            warnings.warn(
                f"Channel {chan_no!r} for station '{station['station_id']}' not found "
                "in channels_df. Skipping.",
                UserWarning,
                stacklevel=2,
            )
            continue

        line_length = closest_line.geometry.length
        if line_length > 0:
            dist_along = get_distance_from_start(station.geometry, closest_line)
            fraction = dist_along / line_length
        else:
            fraction = 0.0

        upnode, downnode = chan_lookup[chan_no]
        if fraction < 0.5:
            location, node_id = "upstream", upnode
        else:
            location, node_id = "downstream", downnode

        records.append(
            {
                "station_id": station["station_id"],
                "chan_no": chan_no,
                "location": location,
                "node_id": node_id,
                "distance_fraction": fraction,
            }
        )

    result = pd.DataFrame(records)
    if not result.empty:
        result = result.set_index("station_id")
    return result


# ---------------------------------------------------------------------------
# Corrector classes
# ---------------------------------------------------------------------------


class NetworkIDWCorrector(NetworkCorrector):
    """Correct DSM2 channel-end values using sparse observations via network IDW.

    Network distances are computed along a directed graph that follows the
    UPNODE → DOWNNODE orientation of DSM2 channels (approximately the
    tidally-filtered flow direction), weighted by channel length in feet.
    Corrections therefore spread only to channel-ends that are *downstream*
    of an observation node in the network.

    The distance matrix is pre-computed once at construction time.  The
    per-time-step :meth:`correct` call is a lightweight weighted-sum that
    drops NaN observations automatically.

    Parameters
    ----------
    channels_df : pd.DataFrame
        CHANNEL table from ``HydroH5.get_channels()`` or
        ``parser.parse_input()["CHANNEL"]``.  Required columns
        (case-insensitive): ``chan_no``, ``upnode``, ``downnode``, ``length``.
    snapped_stations_df : pd.DataFrame
        Station-to-channel-end mapping from :func:`snap_stations_to_channel_ends`.
        Index must be ``station_id``; columns must include ``chan_no``,
        ``location``, and ``node_id``.
    power : float
        IDW distance exponent.  Default ``2``.
    max_distance : float or None
        Maximum network distance (feet) beyond which an observation has no
        influence.  ``None`` means no cutoff.

    Examples
    --------
    >>> corrector = NetworkIDWCorrector(channels_df, snapped, power=2)
    >>> obs = pd.Series({"RSAC075": 820.0, "RSAN007": float("nan")})
    >>> corrected_row = corrector.correct(model_df.iloc[0], obs)
    """

    def __init__(self, channels_df, snapped_stations_df, power=2, max_distance=None):
        self.power = power
        self.max_distance = max_distance
        self._snapped = snapped_stations_df  # index = station_id

        # Normalise column names to lowercase so both HydroH5 (lowercase) and
        # parse_input (uppercase) formats are accepted transparently.
        norm = _normalise_channels(channels_df)

        # channel-end key (e.g. "441-upstream") → integer network node id
        self._channel_end_to_node: dict[str, int] = {}
        for _, row in norm.iterrows():
            chan = str(row["chan_no"])
            self._channel_end_to_node[f"{chan}-upstream"] = int(row["upnode"])
            self._channel_end_to_node[f"{chan}-downstream"] = int(row["downnode"])

        # Directed graph: UPNODE → DOWNNODE, edge weight = length (ft)
        self._graph = nx.from_pandas_edgelist(
            norm,
            source="upnode",
            target="downnode",
            edge_attr=["length"],
            create_using=nx.MultiDiGraph,
        )

        # Pre-compute raw IDW weights once.
        # _weights[channel_end_key][station_id] = d^(-power) or inf (same node)
        self._weights: dict[str, dict[str, float]] = self._build_weights()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_weights(self) -> dict[str, dict[str, float]]:
        """Pre-compute IDW weights from each station node to every channel-end.

        Returns
        -------
        dict[str, dict[str, float]]
            ``{channel_end_key: {station_id: weight}}``.
            Weight is ``distance ** (-power)`` for d > 0, or ``inf`` when the
            observation and target share the same node.  Unreachable pairs are
            absent (zero contribution at correction time).
        """
        weights: dict[str, dict[str, float]] = {
            ce: {} for ce in self._channel_end_to_node
        }

        for station_id, row in self._snapped.iterrows():
            obs_node = int(row["node_id"])

            # Directed shortest-path distances from obs_node to all reachable nodes.
            reachable: dict[int, float] = nx.single_source_dijkstra_path_length(
                self._graph, obs_node, weight="length", cutoff=self.max_distance
            )

            for ce_key, target_node in self._channel_end_to_node.items():
                if target_node == obs_node:
                    # Same network node: correction is the exact residual.
                    weights[ce_key][station_id] = float("inf")
                elif target_node in reachable:
                    d = reachable[target_node]
                    if d > 0:
                        weights[ce_key][station_id] = d ** (-self.power)
                    else:
                        # Zero-length path to a different node (degenerate graph):
                        # treat the same as a same-node exact match.
                        weights[ce_key][station_id] = float("inf")
                # else: target_node unreachable → no entry; contributes weight 0.

        return weights

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def correct(self, model_series: pd.Series, observations: pd.Series) -> pd.Series:
        """Apply IDW correction to one time step of channel-end model values.

        Parameters
        ----------
        model_series : pd.Series
            Channel-end model values indexed by ``"{chan_no}-{location}"``.
            Typically a single row of the DataFrame returned by
            :func:`extract_channel_end_values`.
        observations : pd.Series
            Observed values indexed by ``station_id``.  ``NaN`` entries are
            treated as missing and excluded automatically.  Any station IDs
            not present in the snapped-stations table are silently ignored.

        Returns
        -------
        pd.Series
            Corrected values with the same index and dtype as *model_series*.
            When no valid observations are available the method returns an
            unchanged copy of *model_series*.
        """
        valid_obs = observations.dropna()
        if valid_obs.empty:
            return model_series.copy()

        # Compute the residual (obs − model) at each available station's
        # assigned channel-end.
        residuals: dict[str, float] = {}
        for station_id, obs_value in valid_obs.items():
            if station_id not in self._snapped.index:
                continue
            row = self._snapped.loc[station_id]
            ce_key = f"{row['chan_no']}-{row['location']}"
            if ce_key in model_series.index:
                residuals[station_id] = float(obs_value) - float(model_series[ce_key])

        if not residuals:
            return model_series.copy()

        # Accumulate corrections into a dict keyed by channel-end label.
        corrections: dict[str, float] = {}

        for ce_key, ce_weights in self._weights.items():
            if ce_key not in model_series.index:
                continue

            # Exact-match: the observation node is the same as this channel-end
            # node.  Use the mean of all such exact-match residuals (edge case:
            # multiple stations at the same physical node).
            exact_residuals = [
                residuals[sid]
                for sid, w in ce_weights.items()
                if sid in residuals and np.isinf(w)
            ]
            if exact_residuals:
                corrections[ce_key] = float(np.mean(exact_residuals))
                continue

            # Standard IDW: weighted sum of available station residuals.
            w_sum = 0.0
            wr_sum = 0.0
            for sid, w in ce_weights.items():
                if sid in residuals and np.isfinite(w) and w > 0.0:
                    w_sum += w
                    wr_sum += w * residuals[sid]

            if w_sum > 0.0:
                corrections[ce_key] = wr_sum / w_sum

        if not corrections:
            return model_series.copy()

        correction_series = pd.Series(corrections).reindex(
            model_series.index, fill_value=0.0
        )
        return model_series + correction_series


class NetworkOICorrector(NetworkCorrector):
    """Correct DSM2 channel-end values via Optimal Interpolation (OI).

    OI finds the minimum-variance linear unbiased estimator given a background
    error covariance **B** and an observation error covariance **R**::

        correction = B_x_obs @ (B_obs + R)^{-1} @ residuals

    Unlike :class:`NetworkIDWCorrector`, OI:

    * Automatically **de-weights redundant stations** that are close to each
      other (through the ``B_obs`` matrix inversion).
    * Uses an **undirected** shortest-path graph so corrections can spread in
      both directions along any channel.
    * Accepts a pluggable **correlation kernel** via the *corr_fn* parameter.

    The ``B_obs`` and ``B_x_all`` matrices are pre-computed at construction
    time; the per-step :meth:`correct` call does only a small matrix solve
    (O(n_obs³) with n_obs ≤ ~20).

    Parameters
    ----------
    channels_df : pd.DataFrame
        CHANNEL table.  Same format as :class:`NetworkIDWCorrector`.
    snapped_stations_df : pd.DataFrame
        Station-to-channel-end mapping from :func:`snap_stations_to_channel_ends`.
    sigma_b : float, optional
        Background error standard deviation (µS/cm).  Scales the B matrices.
        Default ``1.0`` (relative units; only the ratio sigma_b / sigma_obs
        matters for the weights).
    sigma_obs : float, optional
        Observation error standard deviation (µS/cm).  Typical EC sensor
        accuracy is 5–20 µS/cm.  Default ``10.0``.
    corr_fn : callable or None, optional
        Correlation kernel produced by :func:`exponential_kernel` or
        :func:`channel_direction_kernel`.  When ``None`` (default)
        :func:`exponential_kernel` with an auto-estimated length scale is used.
    length_scale : float or None, optional
        Overrides the length scale of the default exponential kernel.
        Ignored when *corr_fn* is provided explicitly.

    Examples
    --------
    >>> corrector = NetworkOICorrector(channels_df, snapped,
    ...                                sigma_obs=10.0)
    >>> obs = pd.Series({"RSAC075": 820.0, "RSAN007": float("nan")})
    >>> corrected_row = corrector.correct(model_df.iloc[0], obs)
    """

    def __init__(
        self,
        channels_df,
        snapped_stations_df,
        sigma_b: float = 1.0,
        sigma_obs: float = 10.0,
        corr_fn=None,
        length_scale: "float | None" = None,
    ) -> None:
        self._snapped = snapped_stations_df
        self._sigma_b = float(sigma_b)
        self._sigma_obs = float(sigma_obs)

        norm = _normalise_channels(channels_df)

        # Resolve length_scale and kernel --------------------------------
        if corr_fn is None:
            L = length_scale if length_scale is not None else _auto_length_scale(norm)
            corr_fn = exponential_kernel(length_scale=L)
        else:
            # If the supplied kernel has length_scale=None, resolve it now.
            stored_L = getattr(corr_fn, "_length_scale", None)
            if stored_L is None:
                L = length_scale if length_scale is not None else _auto_length_scale(norm)
                kind = getattr(corr_fn, "_kind", "exponential")
                resistance = getattr(corr_fn, "_resistance", 3.0)
                if kind == "channel_direction":
                    corr_fn = channel_direction_kernel(length_scale=L, resistance=resistance)
                else:
                    corr_fn = exponential_kernel(length_scale=L)
        self._corr_fn = corr_fn
        self._is_directional = getattr(corr_fn, "_kind", "exponential") == "channel_direction"

        # channel-end key → node id ----------------------------------------
        self._channel_end_to_node: dict[str, int] = {}
        for _, row in norm.iterrows():
            chan = str(row["chan_no"])
            self._channel_end_to_node[f"{chan}-upstream"]   = int(row["upnode"])
            self._channel_end_to_node[f"{chan}-downstream"] = int(row["downnode"])

        # Build directed + undirected graphs --------------------------------
        self._digraph = nx.from_pandas_edgelist(
            norm,
            source="upnode",
            target="downnode",
            edge_attr=["chan_no", "length"],
            create_using=nx.MultiDiGraph,
        )
        self._ugraph = self._digraph.to_undirected(reciprocal=False)
        # For the undirected graph, keep the minimum-length edge between any
        # two nodes (relevant when parallel channels exist).
        self._ugraph = nx.Graph(
            (u, v, min(d.values(), key=lambda e: e["length"]))
            for u, v, d in (
                (u, v, {k: data for k, (u2, v2, data) in enumerate(
                    self._digraph.edges(u, data=True)
                ) if v2 == v})
                for u, v in nx.edges(self._ugraph)
            )
        )
        # Rebuild more cleanly: simple undirected graph, one edge per node pair.
        self._ugraph = nx.Graph()
        for u, v, data in self._digraph.edges(data=True):
            existing = self._ugraph.get_edge_data(u, v)
            if existing is None or data["length"] < existing["length"]:
                self._ugraph.add_edge(u, v,
                                      chan_no=data.get("chan_no", ""),
                                      length=data["length"])
        # Also handle reverse edges (DOWNNODE → UPNODE in directed graph)
        for u, v, data in self._digraph.reverse().edges(data=True):
            existing = self._ugraph.get_edge_data(u, v)
            if existing is None:
                self._ugraph.add_edge(u, v,
                                      chan_no=data.get("chan_no", ""),
                                      length=data["length"])

        # Observation node list (in index order of snapped_stations_df) -----
        self._station_ids: list = list(snapped_stations_df.index)
        self._obs_nodes: list[int] = [
            int(snapped_stations_df.loc[s, "node_id"]) for s in self._station_ids
        ]

        # Build channel-end list in consistent order -------------------------
        self._ce_keys: list[str] = list(self._channel_end_to_node.keys())
        self._ce_nodes: list[int] = [
            self._channel_end_to_node[k] for k in self._ce_keys
        ]

        # Pre-compute B_obs and B_x_all -------------------------------------
        self._B_obs, self._B_x_all = self._build_covariance_matrices(norm)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _path_segments(
        self, source_node: int, target_node: int, norm: "pd.DataFrame"
    ) -> list:
        """Return signed path segments ``[(chan_no, signed_length_ft), ...]``.

        Each segment has positive length if traversed UPNODE→DOWNNODE
        (with flow), negative if DOWNNODE→UPNODE (against flow).
        Returns an empty list if no path exists.
        """
        if source_node == target_node:
            return []
        try:
            path_nodes = nx.shortest_path(
                self._ugraph, source_node, target_node, weight="length"
            )
        except nx.NetworkXNoPath:
            return []

        # Build lookup: (u, v) → (chan_no, length, upnode, downnode)
        chan_lookup: dict = {}
        for _, row in norm.iterrows():
            up = int(row["upnode"])
            dn = int(row["downnode"])
            chan_no = str(row["chan_no"])
            L = float(row["length"])
            chan_lookup[(up, dn)] = (chan_no, L, True)   # with-flow direction
            chan_lookup[(dn, up)] = (chan_no, L, False)  # against-flow direction

        segments = []
        for u, v in zip(path_nodes[:-1], path_nodes[1:]):
            info = chan_lookup.get((u, v)) or chan_lookup.get((v, u))
            if info is None:
                continue
            chan_no, L, with_flow = info
            # Check actual traversal direction
            if (u, v) in chan_lookup:
                actual_with_flow = chan_lookup[(u, v)][2]
            else:
                actual_with_flow = not chan_lookup[(v, u)][2]
            signed_L = L if actual_with_flow else -L
            segments.append((chan_no, signed_L))
        return segments

    def _correlation(self, source_node: int, target_node: int, norm) -> float:
        """Return background error correlation B(source, target) / sigma_b^2."""
        if source_node == target_node:
            return 1.0
        try:
            total_len = nx.shortest_path_length(
                self._ugraph, source_node, target_node, weight="length"
            )
        except nx.NetworkXNoPath:
            return 0.0

        if self._is_directional:
            segs = self._path_segments(source_node, target_node, norm)
            if not segs:
                return 0.0
            return self._corr_fn(segs)
        else:
            return self._corr_fn(total_len)

    def _build_covariance_matrices(self, norm):
        """Pre-compute B_obs (n_obs × n_obs) and B_x_all (n_ce × n_obs).

        Returns
        -------
        B_obs : np.ndarray, shape (n_obs, n_obs)
        B_x_all : np.ndarray, shape (n_ce, n_obs)
        """
        n_obs = len(self._obs_nodes)
        n_ce  = len(self._ce_nodes)
        sb2 = self._sigma_b ** 2

        B_obs = np.zeros((n_obs, n_obs), dtype=float)
        for i, ni in enumerate(self._obs_nodes):
            for j, nj in enumerate(self._obs_nodes):
                B_obs[i, j] = sb2 * self._correlation(ni, nj, norm)

        B_x_all = np.zeros((n_ce, n_obs), dtype=float)
        for xi, xn in enumerate(self._ce_nodes):
            for j, nj in enumerate(self._obs_nodes):
                B_x_all[xi, j] = sb2 * self._correlation(nj, xn, norm)

        return B_obs, B_x_all

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def correct(self, model_series: pd.Series, observations: pd.Series) -> pd.Series:
        """Apply OI correction to one time step of channel-end model values.

        Parameters
        ----------
        model_series : pd.Series
            Channel-end model values indexed by ``"{chan_no}-{location}"``.
        observations : pd.Series
            Observed values indexed by ``station_id``.  ``NaN`` entries are
            treated as missing and excluded automatically.

        Returns
        -------
        pd.Series
            Corrected values with the same index and dtype as *model_series*.
        """
        valid_obs = observations.dropna()
        if valid_obs.empty:
            return model_series.copy()

        # --- available observation set S -----------------------------------
        avail_mask = [
            sid in valid_obs.index and sid in self._snapped.index
            for sid in self._station_ids
        ]
        avail_idx = [i for i, a in enumerate(avail_mask) if a]
        if not avail_idx:
            return model_series.copy()

        # --- residuals r_i = obs_i − model[ce_i] ----------------------------
        residuals = np.zeros(len(avail_idx), dtype=float)
        for k, i in enumerate(avail_idx):
            sid = self._station_ids[i]
            row = self._snapped.loc[sid]
            ce_key = f"{row['chan_no']}-{row['location']}"
            obs_val = float(valid_obs[sid])
            model_val = float(model_series.get(ce_key, obs_val))  # fallback=obs
            residuals[k] = obs_val - model_val

        # --- OI linear solve (once, shared across all channel-ends) ---------
        # W = B_x[:,S] @ (B_obs[S,S] + sigma_obs^2 * I)^{-1}
        # corrections = W @ residuals
        B_S  = self._B_obs[np.ix_(avail_idx, avail_idx)]
        B_xS = self._B_x_all[:, avail_idx]   # shape (n_ce, |S|)
        so2  = self._sigma_obs ** 2
        A    = B_S + so2 * np.eye(len(avail_idx))
        # Solve A^T x = B_xS^T  →  x = (A^{-1} B_xS^T)^T
        # i.e. W = B_xS @ A^{-1}  (n_ce × |S|)
        try:
            W = np.linalg.solve(A.T, B_xS.T).T      # shape (n_ce, |S|)
        except np.linalg.LinAlgError:
            # Fallback: pseudo-inverse (handles degenerate B_obs)
            W = B_xS @ np.linalg.pinv(A)

        ce_corrections = W @ residuals              # shape (n_ce,)

        # --- assemble into a Series with the same index as model_series ----
        corr_dict = {
            self._ce_keys[xi]: ce_corrections[xi]
            for xi in range(len(self._ce_keys))
            if self._ce_keys[xi] in model_series.index
        }
        if not corr_dict:
            return model_series.copy()

        correction_series = pd.Series(corr_dict).reindex(
            model_series.index, fill_value=0.0
        )
        return model_series + correction_series

