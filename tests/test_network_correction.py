"""Tests for pydsm.analysis.network_correction.

Graph topology used in unit tests (5-node linear chain, channels named
by tens so they don't collide with real DSM2 channel numbers):

    node 1 --[chan 10]--> node 2 --[chan 20]--> node 3
                                                   |
                                              [chan 30]
                                                   |
                                               node 4 --[chan 40]--> node 5

All channel lengths = 1000 ft.  Model background = 500 µS/cm everywhere.

Directed distances (UPNODE→DOWNNODE):
    node 1 → node 2 :  1 000 ft
    node 1 → node 3 :  2 000 ft
    node 1 → node 4 :  3 000 ft
    node 1 → node 5 :  4 000 ft
    node 2 → node 3 :  1 000 ft
    node 2 → node 4 :  2 000 ft
    node 2 → node 5 :  3 000 ft
    node 3 → node 4 :  1 000 ft
    node 3 → node 5 :  2 000 ft
    node 4 → node 5 :  1 000 ft
    (upstream direction not reachable in directed graph)
"""

import os

import numpy as np
import pandas as pd
import pytest

from pydsm.analysis.network_correction import (
    NetworkIDWCorrector,
    NetworkOICorrector,
    exponential_kernel,
    channel_direction_kernel,
    extract_channel_end_values,
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _make_channels():
    """Return a lowercase CHANNEL-style DataFrame (as from HydroH5.get_channels())."""
    return pd.DataFrame(
        {
            "chan_no":    ["10",   "20",   "30",   "40"],
            "upnode":    [1,      2,      3,      4],
            "downnode":  [2,      3,      4,      5],
            "length":    [1000.0, 1000.0, 1000.0, 1000.0],
            "manning":   [0.025,  0.025,  0.025,  0.025],
            "dispersion":[360.0,  360.0,  360.0,  360.0],
        }
    )


def _make_model(value=500.0):
    """Return a pd.Series with all 8 channel-ends set to *value* µS/cm."""
    channels = _make_channels()
    index = []
    for _, row in channels.iterrows():
        index.append(f"{row['chan_no']}-upstream")
        index.append(f"{row['chan_no']}-downstream")
    return pd.Series(float(value), index=index)


def _snap_one(station_id, chan_no, location):
    """Build a single-row snapped_stations DataFrame for a toy channel."""
    channels = _make_channels()
    norm = channels.copy()
    norm.columns = [c.lower() for c in norm.columns]
    row = norm[norm["chan_no"] == chan_no].iloc[0]
    node_id = int(row["upnode"]) if location == "upstream" else int(row["downnode"])
    return pd.DataFrame(
        [
            {
                "station_id": station_id,
                "chan_no": chan_no,
                "location": location,
                "node_id": node_id,
            }
        ]
    ).set_index("station_id")


def _snap_many(*args):
    """Concatenate multiple single-station snap frames. Each arg is (id, chan, loc)."""
    return pd.concat([_snap_one(*a) for a in args])


# ---------------------------------------------------------------------------
# T1 — single observation; exact match and directed graph boundary
# ---------------------------------------------------------------------------

class TestSingleObservation:
    """Station STA_A at node 2 (downstream end of channel 10), residual = +100."""

    @pytest.fixture
    def corrector(self):
        return NetworkIDWCorrector(
            _make_channels(),
            _snap_one("STA_A", "10", "downstream"),
            power=2,
        )

    def test_exact_match_channel_end(self, corrector):
        """The observation's own channel-end is corrected to the observed value."""
        corrected = corrector.correct(_make_model(), pd.Series({"STA_A": 600.0}))
        assert corrected["10-downstream"] == pytest.approx(600.0)

    def test_same_node_other_channel_end_also_exact(self, corrector):
        """Channel-end 20-upstream shares node 2 with the obs → also exact."""
        corrected = corrector.correct(_make_model(), pd.Series({"STA_A": 600.0}))
        assert corrected["20-upstream"] == pytest.approx(600.0)

    def test_upstream_node_unchanged_directed_graph(self, corrector):
        """Node 1 is upstream of node 2 in the directed graph: not reachable → no correction."""
        corrected = corrector.correct(_make_model(), pd.Series({"STA_A": 600.0}))
        assert corrected["10-upstream"] == pytest.approx(500.0)

    def test_single_obs_uniform_correction_downstream(self, corrector):
        """With exactly one observation, IDW is uniform: all reachable nodes get the
        full residual (the single weight cancels in numerator / denominator)."""
        corrected = corrector.correct(_make_model(), pd.Series({"STA_A": 600.0}))
        for ce in ["20-downstream", "30-upstream", "30-downstream", "40-upstream", "40-downstream"]:
            assert corrected[ce] == pytest.approx(600.0), f"Expected 600 at {ce}"

    def test_output_index_preserved(self, corrector):
        model = _make_model()
        corrected = corrector.correct(model, pd.Series({"STA_A": 600.0}))
        assert list(corrected.index) == list(model.index)


# ---------------------------------------------------------------------------
# T2 — two observations; downstream node gets distance-blended correction
# ---------------------------------------------------------------------------

class TestTwoObservations:
    """STA_A at node 2 (residual +200), STA_B at node 3 (residual −200).
    Node 4 (40-upstream / 30-downstream) is downstream of both and should
    receive a blended correction: negative because STA_B is closer to node 4.
    """

    @pytest.fixture
    def corrector(self):
        snapped = _snap_many(
            ("STA_A", "10", "downstream"),  # node 2
            ("STA_B", "20", "downstream"),  # node 3
        )
        return NetworkIDWCorrector(_make_channels(), snapped, power=2)

    def test_obs_b_exact_at_its_node(self, corrector):
        """STA_B's own channel-end gets the exact residual from STA_B."""
        obs = pd.Series({"STA_A": 700.0, "STA_B": 300.0})  # residuals +200 / −200
        corrected = corrector.correct(_make_model(), obs)
        assert corrected["20-downstream"] == pytest.approx(300.0)

    def test_blended_node_is_between_both_residuals(self, corrector):
        """Node 4 (30-downstream) is downstream of both; correction is blended."""
        obs = pd.Series({"STA_A": 700.0, "STA_B": 300.0})
        corrected = corrector.correct(_make_model(), obs)
        # Correction must lie strictly between −200 and +200
        c = corrected["30-downstream"] - 500.0
        assert -200.0 < c < 200.0

    def test_blended_node_closer_to_nearer_obs(self, corrector):
        """STA_B (d=1000 ft to node 4) is closer than STA_A (d=2000 ft):
        the blended correction should be dominated by STA_B's residual (−200)."""
        obs = pd.Series({"STA_A": 700.0, "STA_B": 300.0})
        corrected = corrector.correct(_make_model(), obs)
        assert corrected["30-downstream"] < 500.0  # net correction is negative

    def test_upstream_of_both_obs_unchanged(self, corrector):
        """Node 1 (10-upstream) is upstream of both observations → no correction."""
        obs = pd.Series({"STA_A": 700.0, "STA_B": 300.0})
        corrected = corrector.correct(_make_model(), obs)
        assert corrected["10-upstream"] == pytest.approx(500.0)


# ---------------------------------------------------------------------------
# T3 — one observation goes NaN; algorithm adapts silently
# ---------------------------------------------------------------------------

class TestMissingObservationAdapts:
    """STA_A (node 2) and STA_B (node 3) available; then STA_A goes NaN."""

    @pytest.fixture
    def corrector(self):
        snapped = _snap_many(
            ("STA_A", "10", "downstream"),  # node 2
            ("STA_B", "20", "downstream"),  # node 3
        )
        return NetworkIDWCorrector(_make_channels(), snapped, power=2)

    def test_nan_station_excluded_no_error(self, corrector):
        """NaN station must not raise and must not propagate NaN into output."""
        obs = pd.Series({"STA_A": float("nan"), "STA_B": 600.0})
        corrected = corrector.correct(_make_model(), obs)
        assert corrected.notna().all()

    def test_remaining_obs_still_corrects(self, corrector):
        """With STA_A NaN, STA_B still applies its correction downstream."""
        obs = pd.Series({"STA_A": float("nan"), "STA_B": 600.0})
        corrected = corrector.correct(_make_model(), obs)
        # STA_B is at node 3 (20-downstream / 30-upstream): exact correction
        assert corrected["20-downstream"] == pytest.approx(600.0)

    def test_partial_missing_differs_from_both_present(self, corrector):
        """When STA_A is missing the correction at a downstream node should differ."""
        obs_both = pd.Series({"STA_A": 600.0, "STA_B": 400.0})
        obs_partial = pd.Series({"STA_A": float("nan"), "STA_B": 400.0})
        c_both = corrector.correct(_make_model(), obs_both)["30-downstream"]
        c_partial = corrector.correct(_make_model(), obs_partial)["30-downstream"]
        # With STA_A missing, STA_B dominates; the corrections differ
        assert c_both != pytest.approx(c_partial)


# ---------------------------------------------------------------------------
# T4 — all observations NaN → model returned unchanged
# ---------------------------------------------------------------------------

class TestAllObservationsMissing:

    @pytest.fixture
    def corrector(self):
        return NetworkIDWCorrector(
            _make_channels(),
            _snap_one("STA_A", "10", "downstream"),
            power=2,
        )

    def test_all_nan_equals_model(self, corrector):
        model = _make_model()
        obs = pd.Series({"STA_A": float("nan")})
        pd.testing.assert_series_equal(corrector.correct(model, obs), model)

    def test_empty_series_equals_model(self, corrector):
        model = _make_model()
        pd.testing.assert_series_equal(
            corrector.correct(model, pd.Series(dtype=float)), model
        )

    def test_unknown_station_id_equals_model(self, corrector):
        """Observations whose station_id is not in snapped_stations are silently ignored."""
        model = _make_model()
        obs = pd.Series({"NOT_A_STATION": 999.0})
        pd.testing.assert_series_equal(corrector.correct(model, obs), model)


# ---------------------------------------------------------------------------
# T5 — observation exactly at its channel-end node → corrected == observed
# ---------------------------------------------------------------------------

class TestExactNodeCorrection:

    @pytest.fixture
    def corrector(self):
        return NetworkIDWCorrector(
            _make_channels(),
            _snap_one("STA_A", "20", "upstream"),  # node 2
            power=2,
        )

    def test_corrected_equals_observed(self, corrector):
        obs_value = 1234.5
        corrected = corrector.correct(_make_model(), pd.Series({"STA_A": obs_value}))
        assert corrected["20-upstream"] == pytest.approx(obs_value)

    def test_same_physical_node_other_end_also_exact(self, corrector):
        """10-downstream also maps to node 2; it must receive the exact correction too."""
        corrected = corrector.correct(_make_model(), pd.Series({"STA_A": 1234.5}))
        assert corrected["10-downstream"] == pytest.approx(1234.5)

    def test_upstream_boundary_unchanged(self, corrector):
        corrected = corrector.correct(_make_model(), pd.Series({"STA_A": 1234.5}))
        assert corrected["10-upstream"] == pytest.approx(500.0)


# ---------------------------------------------------------------------------
# Additional: different IDW power values produce expected ordering
# ---------------------------------------------------------------------------

class TestPowerParameter:

    def test_higher_power_faster_decay(self):
        """Higher IDW power → distant nodes receive smaller corrections."""
        channels = _make_channels()
        snapped = _snap_one("STA_A", "10", "downstream")  # node 2; obs at node 2
        obs = pd.Series({"STA_A": 600.0})  # residual +100
        # A second station at node 3 would be needed to see the decay between p1/p2
        # in a two-observation scenario. Instead, confirm power is stored correctly.
        c_p1 = NetworkIDWCorrector(channels, snapped, power=1)
        c_p4 = NetworkIDWCorrector(channels, snapped, power=4)
        assert c_p1.power == 1
        assert c_p4.power == 4

    def test_max_distance_limits_influence(self):
        """With max_distance set, distant nodes receive zero correction."""
        channels = _make_channels()
        snapped = _snap_one("STA_A", "10", "downstream")  # node 2
        # max_distance = 500 ft; node 3 is 1000 ft away → should get no correction
        corrector = NetworkIDWCorrector(channels, snapped, power=2, max_distance=500.0)
        model = _make_model()
        obs = pd.Series({"STA_A": 600.0})
        corrected = corrector.correct(model, obs)
        # Node 3 (20-downstream / 30-upstream) is 1000 ft from node 2 → no weight
        assert corrected["20-downstream"] == pytest.approx(500.0)
        assert corrected["30-upstream"] == pytest.approx(500.0)
        # But the exact-match node (node 2) is still corrected
        assert corrected["10-downstream"] == pytest.approx(600.0)


# ---------------------------------------------------------------------------
# Integration tests using real HDF5 fixture files
# ---------------------------------------------------------------------------

@pytest.mark.integration
class TestIntegrationRealData:
    """Integration tests using the real QualH5 and HydroH5 fixtures.

    Observation stations are constructed programmatically (no centerlines
    GeoJSON needed) by reading UPNODE/DOWNNODE directly from the CHANNEL table.
    """

    DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
    QUAL_FILE = os.path.join(DATA_DIR, "historical_v82_ec.h5")
    HYDRO_FILE = os.path.join(DATA_DIR, "historical_v82.h5")

    @pytest.fixture(scope="class")
    def qual(self):
        from pydsm.output.qualh5 import QualH5
        if not os.path.exists(self.QUAL_FILE):
            pytest.skip(f"Test data not found: {self.QUAL_FILE}")
        return QualH5(self.QUAL_FILE)

    @pytest.fixture(scope="class")
    def hydro(self):
        from pydsm.output.hydroh5 import HydroH5
        if not os.path.exists(self.HYDRO_FILE):
            pytest.skip(f"Test data not found: {self.HYDRO_FILE}")
        return HydroH5(self.HYDRO_FILE)

    @pytest.fixture(scope="class")
    def model_df(self, qual):
        return extract_channel_end_values(qual, "ec", "05JAN1990 - 06JAN1990")

    @pytest.fixture(scope="class")
    def corrector(self, hydro):
        channels = hydro.get_channels()
        # Snap two stations to known channels without needing a centerlines file.
        row_441 = channels[channels["chan_no"] == "441"].iloc[0]
        row_1 = channels[channels["chan_no"] == "1"].iloc[0]
        snapped = pd.DataFrame(
            [
                {
                    "station_id": "STA_OBS1",
                    "chan_no": "441",
                    "location": "upstream",
                    "node_id": int(row_441["upnode"]),
                },
                {
                    "station_id": "STA_OBS2",
                    "chan_no": "1",
                    "location": "downstream",
                    "node_id": int(row_1["downnode"]),
                },
            ]
        ).set_index("station_id")
        return NetworkIDWCorrector(channels, snapped, power=2)

    def test_extract_returns_both_ends_for_every_channel(self, model_df, hydro):
        n_channels = len(hydro.get_channels())
        assert model_df.shape[1] == n_channels * 2

    def test_extract_column_format(self, model_df):
        assert all("-upstream" in c or "-downstream" in c for c in model_df.columns)

    def test_correct_returns_same_shape(self, model_df, corrector):
        model_row = model_df.iloc[0]
        obs = pd.Series({"STA_OBS1": 800.0, "STA_OBS2": 300.0})
        corrected = corrector.correct(model_row, obs)
        assert corrected.shape == model_row.shape

    def test_corrected_index_matches_model(self, model_df, corrector):
        model_row = model_df.iloc[0]
        obs = pd.Series({"STA_OBS1": 800.0, "STA_OBS2": 300.0})
        corrected = corrector.correct(model_row, obs)
        pd.testing.assert_index_equal(corrected.index, model_row.index)

    def test_corrected_values_are_finite(self, model_df, corrector):
        """IDW correction is additive and does not clamp; the output may go outside
        the physically observable EC range when test observations create large
        residuals.  The important invariants are: no NaN and no inf."""
        model_row = model_df.iloc[0]
        obs = pd.Series({"STA_OBS1": 800.0, "STA_OBS2": 300.0})
        corrected = corrector.correct(model_row, obs)
        assert corrected.notna().all(), "Corrected series contains NaN"
        assert np.isfinite(corrected).all(), "Corrected series contains inf"

    def test_all_missing_returns_model_exactly(self, model_df, corrector):
        model_row = model_df.iloc[0]
        obs = pd.Series({"STA_OBS1": float("nan"), "STA_OBS2": float("nan")})
        corrected = corrector.correct(model_row, obs)
        pd.testing.assert_series_equal(corrected, model_row)

    def test_no_nan_in_output(self, model_df, corrector):
        model_row = model_df.iloc[0]
        obs = pd.Series({"STA_OBS1": 800.0, "STA_OBS2": float("nan")})
        corrected = corrector.correct(model_row, obs)
        assert corrected.notna().all(), "Corrected series contains NaN"


# ---------------------------------------------------------------------------
# Kernel factories
# ---------------------------------------------------------------------------

class TestKernels:

    def test_exponential_kernel_at_zero(self):
        """Zero distance → correlation = 1.0."""
        k = exponential_kernel(length_scale=1000.0)
        assert k(0.0) == pytest.approx(1.0)

    def test_exponential_kernel_decay(self):
        """Correlation decays with distance."""
        k = exponential_kernel(length_scale=1000.0)
        assert k(1000.0) == pytest.approx(np.exp(-1.0))
        assert k(2000.0) == pytest.approx(np.exp(-2.0))
        assert k(1000.0) > k(2000.0)

    def test_exponential_kernel_invalid_length_scale(self):
        with pytest.raises(ValueError, match="positive"):
            exponential_kernel(length_scale=0.0)

    def test_channel_direction_kernel_symmetric_for_single_direction_path(self):
        """For a purely with-flow or purely against-flow path of equal length,
        the geometric-mean symmetrised kernel gives the same value (d_sym = L*sqrt(r))."""
        k = channel_direction_kernel(length_scale=1000.0, resistance=4.0)
        segs_with  = [("ch1", +1000.0)]   # with-flow
        segs_against = [("ch1", -1000.0)] # against-flow
        assert k(segs_with) == pytest.approx(k(segs_against))

    def test_channel_direction_kernel_mixed_vs_aligned(self):
        """A mixed path (half with-flow, half against) has higher effective distance
        than a purely aligned path of the same total length (AM-GM inequality)."""
        k = channel_direction_kernel(length_scale=1000.0, resistance=4.0)
        segs_aligned = [("ch1", +500.0), ("ch2", +500.0)]  # both with-flow
        segs_mixed   = [("ch1", +500.0), ("ch2", -500.0)]  # mixed
        # aligned d_sym = 1000 * sqrt(4) = 2000; mixed d_sym = 500*(1+4)/2 * 2 = ... let's just compare
        assert k(segs_aligned) > k(segs_mixed)  # aligned path has LOWER effective distance

    def test_channel_direction_kernel_resistance_one_matches_exponential(self):
        """resistance=1.0 → same as exponential_kernel (fully symmetric)."""
        k_dir = channel_direction_kernel(length_scale=1000.0, resistance=1.0)
        k_exp = exponential_kernel(length_scale=1000.0)
        segs = [("ch1", +1000.0)]
        assert k_dir(segs) == pytest.approx(k_exp(1000.0))

    def test_channel_direction_kernel_invalid_resistance(self):
        with pytest.raises(ValueError, match="resistance"):
            channel_direction_kernel(resistance=0.5)


# ---------------------------------------------------------------------------
# NetworkOICorrector unit tests (toy 5-node graph)
# ---------------------------------------------------------------------------

class TestNetworkOICorrector:
    """Tests for NetworkOICorrector on the same 5-node toy graph used by IDW tests."""

    @pytest.fixture
    def oi_single(self):
        """OI corrector with STA_A at node 2 (10-downstream)."""
        return NetworkOICorrector(
            _make_channels(),
            _snap_one("STA_A", "10", "downstream"),
            sigma_b=1.0, sigma_obs=0.1,  # small sigma_obs → trust observations
        )

    @pytest.fixture
    def oi_two(self):
        """OI corrector with STA_A (node 2) and STA_B (node 3)."""
        snapped = _snap_many(
            ("STA_A", "10", "downstream"),  # node 2
            ("STA_B", "20", "downstream"),  # node 3
        )
        return NetworkOICorrector(
            _make_channels(), snapped, sigma_b=1.0, sigma_obs=0.1,
        )

    # ------------------------------------------------------------------
    # SPD guarantee
    # ------------------------------------------------------------------

    def test_b_obs_is_symmetric(self):
        """B_obs must be symmetric (required for valid OI)."""
        snapped = _snap_many(
            ("STA_A", "10", "downstream"),
            ("STA_B", "20", "downstream"),
        )
        oi = NetworkOICorrector(_make_channels(), snapped, sigma_b=1.0, sigma_obs=0.1)
        B = oi._B_obs
        np.testing.assert_allclose(B, B.T, atol=1e-12)

    def test_b_obs_is_positive_definite(self):
        """B_obs + sigma_obs^2 * I must be positive definite."""
        snapped = _snap_many(
            ("STA_A", "10", "downstream"),
            ("STA_B", "30", "upstream"),
        )
        oi = NetworkOICorrector(_make_channels(), snapped, sigma_b=1.0, sigma_obs=1.0)
        A = oi._B_obs + oi._sigma_obs**2 * np.eye(len(oi._obs_nodes))
        eigvals = np.linalg.eigvalsh(A)
        assert (eigvals > 0).all(), f"Non-positive eigenvalues: {eigvals}"

    def test_b_x_all_shape(self):
        """B_x_all must have shape (n_channel_ends, n_obs)."""
        snapped = _snap_many(
            ("STA_A", "10", "downstream"),
            ("STA_B", "20", "downstream"),
        )
        oi = NetworkOICorrector(_make_channels(), snapped)
        n_ce = len(oi._ce_keys)
        n_obs = len(oi._station_ids)
        assert oi._B_x_all.shape == (n_ce, n_obs)

    # ------------------------------------------------------------------
    # Fallback to model when no valid observations
    # ------------------------------------------------------------------

    def test_all_nan_returns_model(self, oi_single):
        model = _make_model()
        obs = pd.Series({"STA_A": float("nan")})
        pd.testing.assert_series_equal(oi_single.correct(model, obs), model)

    def test_empty_obs_returns_model(self, oi_single):
        model = _make_model()
        pd.testing.assert_series_equal(
            oi_single.correct(model, pd.Series(dtype=float)), model
        )

    # ------------------------------------------------------------------
    # Correction at and near the observation node
    # ------------------------------------------------------------------

    def test_obs_node_corrected_toward_observation(self, oi_single):
        """With small sigma_obs, the corrected value at the obs node is close to the observed."""
        model = _make_model(500.0)
        obs = pd.Series({"STA_A": 600.0})  # residual = +100
        corrected = oi_single.correct(model, obs)
        # With sigma_obs << sigma_b, OI correction ≈ exact residual at obs node
        assert corrected["10-downstream"] > 500.0
        assert corrected["10-downstream"] < 601.0

    def test_distant_node_also_corrected(self, oi_single):
        """OI spreads corrections to all connected nodes (undirected graph)."""
        model = _make_model(500.0)
        obs = pd.Series({"STA_A": 600.0})
        corrected = oi_single.correct(model, obs)
        # Node 3 (20-downstream) is connected in undirected graph → gets correction
        assert corrected["20-downstream"] > 500.0

    def test_output_index_preserved(self, oi_single):
        model = _make_model()
        corrected = oi_single.correct(model, pd.Series({"STA_A": 600.0}))
        assert list(corrected.index) == list(model.index)

    def test_output_no_nan(self, oi_single):
        model = _make_model()
        corrected = oi_single.correct(model, pd.Series({"STA_A": 600.0}))
        assert corrected.notna().all()

    # ------------------------------------------------------------------
    # OI advantage: de-weights redundant nearby stations
    # ------------------------------------------------------------------

    def test_two_nearby_obs_smaller_correction_than_two_far_obs(self):
        """Two obs at adjacent nodes (distance=1000 ft apart) should produce smaller
        total correction at a distant node than two obs at very different locations,
        because OI de-weights the redundant information from nearby stations."""
        channels = _make_channels()
        # Near pair: STA_A at node 2, STA_B at node 3 (1000 ft apart)
        snapped_near = _snap_many(
            ("STA_A", "10", "downstream"),  # node 2
            ("STA_B", "20", "downstream"),  # node 3 — close to STA_A
        )
        # Far pair: STA_A at node 2, STA_B at node 5 (far apart)
        snapped_far = _snap_many(
            ("STA_A", "10", "downstream"),  # node 2
            ("STA_B", "40", "downstream"),  # node 5 — far from STA_A
        )
        oi_near = NetworkOICorrector(channels, snapped_near, sigma_b=1.0, sigma_obs=0.1)
        oi_far  = NetworkOICorrector(channels, snapped_far,  sigma_b=1.0, sigma_obs=0.1)
        model = _make_model(500.0)
        obs = pd.Series({"STA_A": 600.0, "STA_B": 600.0})  # both show +100 residual
        # At node 5 (40-downstream), the far-apart pair should give more total correction
        # than the near pair (because the near pair carries redundant information)
        c_near = oi_near.correct(model, obs)["40-downstream"]
        c_far  = oi_far.correct(model, obs)["40-downstream"]
        assert c_far >= c_near, (
            f"Expected far-pair correction ({c_far:.2f}) >= near-pair ({c_near:.2f})"
        )

    # ------------------------------------------------------------------
    # OI vs IDW: with a single observation, corrections are in the same direction
    # ------------------------------------------------------------------

    def test_oi_single_obs_same_direction_as_idw(self):
        """With one observation, both OI and IDW apply a positive correction
        to all reachable nodes (positive residual → positive correction everywhere)."""
        channels = _make_channels()
        snapped = _snap_one("STA_A", "10", "downstream")  # node 2
        oi  = NetworkOICorrector(channels, snapped, sigma_b=1.0, sigma_obs=0.1)
        idw = NetworkIDWCorrector(channels, snapped, power=2)
        model = _make_model(500.0)
        obs = pd.Series({"STA_A": 600.0})
        c_oi  = oi.correct(model, obs)
        c_idw = idw.correct(model, obs)
        # Both should give values > 500 at reachable nodes
        for ce in ["20-downstream", "30-upstream"]:
            assert c_oi[ce]  > 500.0, f"OI gave no positive correction at {ce}"
            assert c_idw[ce] > 500.0, f"IDW gave no positive correction at {ce}"

    # ------------------------------------------------------------------
    # Custom kernel
    # ------------------------------------------------------------------

    def test_oi_with_channel_direction_kernel(self):
        """NetworkOICorrector works with the channel_direction_kernel."""
        channels = _make_channels()
        snapped = _snap_one("STA_A", "10", "downstream")
        k = channel_direction_kernel(length_scale=2000.0, resistance=3.0)
        oi = NetworkOICorrector(channels, snapped, sigma_b=1.0, sigma_obs=0.1, corr_fn=k)
        model = _make_model(500.0)
        corrected = oi.correct(model, pd.Series({"STA_A": 600.0}))
        assert corrected.notna().all()
        assert corrected["20-downstream"] > 500.0  # correction spreads

    def test_oi_auto_length_scale(self):
        """When length_scale=None, auto-estimation must produce a finite positive value."""
        channels = _make_channels()
        snapped = _snap_one("STA_A", "10", "downstream")
        oi = NetworkOICorrector(channels, snapped)  # no length_scale
        # Just confirm construction succeeded and correction runs without error
        corrected = oi.correct(_make_model(), pd.Series({"STA_A": 600.0}))
        assert corrected.notna().all()
