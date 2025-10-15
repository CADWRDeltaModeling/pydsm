import os
import pytest
from pydsm.qualh5 import QualH5, dsm2h5
import numpy as np
import pandas as pd


class TestGTMH5:
    @pytest.fixture(scope="module")
    def gtm(self):
        filename = os.path.join(
            os.path.dirname(__file__), "data", "hist_v82_mss2_extran_gtm.h5"
        )
        return QualH5(filename)

    def test_open_non_gtm_file(self):
        try:
            filename = os.path.join(
                os.path.dirname(__file__), "data", "historical_v82.h5"
            )
            QualH5(filename)
            pytest.fail(
                f"This should not have been successful as {filename} is not a gtm tidefile"
            )
        except:
            pass

    def test_get_input_tables(self, gtm):
        tables = gtm.get_input_tables()
        assert len(tables) >= 9
        for t in tables:
            df = gtm.get_input_table(t)
            assert df is not None

    def test_get_data_tables(self, gtm):
        tables = gtm.get_data_tables()
        assert "channel concentration" in tables

    def test_channels(self, gtm):
        channels = gtm.get_channels()
        assert channels[0][0] == "1"
        assert gtm.channel_number2index["441"] == 416

    def test_channel_locations(self, gtm):
        channel_locs = gtm.get_channel_locations()
        assert not channel_locs.empty
        assert channel_locs[0][0] == "upstream"
        assert channel_locs[0][1] == "downstream"

    def test_get_reservoirs(self, gtm):
        reservoirs = gtm.get_reservoirs()
        assert not reservoirs.empty
        assert reservoirs.iloc[0].iloc[0] == "bethel"

    def test_read_attributes(self, gtm):
        tflow = gtm.h5.get("/output/channel concentration")
        attrs = dsm2h5.read_attributes_from_table(tflow)
        assert attrs == {
            "interval": "60min",
            "model": "GTM",
            "model_version": "8.2",
            "start_time": pd.to_datetime("15JAN2020 0000"),
        }

    def test_is_sequence_like(self):
        assert dsm2h5.is_sequence_like([])
        assert dsm2h5.is_sequence_like(np.array(5))

    def test_get_channel_concentration_441(self, gtm):
        df = gtm.get_channel_concentration("ec", "441", "upstream")
        assert len(df) > 100
        fname = os.path.join(os.path.dirname(__file__), "data", "gtm_ec_441_up.pkl")
        # --- regression saves for compare
        df.to_pickle(fname)
        return
        dfr = pd.read_pickle(fname)
        pd.testing.assert_frame_equal(dfr, df)

    def test_get_channel_concentration_all(self, gtm):
        df = gtm.get_channel_concentration("ec", "all", "upstream")
        assert len(df) > 100
        fname = os.path.join(os.path.dirname(__file__), "data", "gtm_ec_all_up.pkl")
        # --- regression saves for compare
        df.to_pickle(fname)
        return
        dfr = pd.read_pickle(fname)
        pd.testing.assert_frame_equal(dfr, df)

    def xtest_get_channel_avg_concentration(self, gtm):
        df = gtm.get_channel_avg_concentration("ec", "441")
        assert len(df) > 100
        fname = os.path.join(os.path.dirname(__file__), "data", "gtm_ec_avg_441.pkl")
        # --- regression saves for compare
        df.to_pickle(fname)
        return
        dfr = pd.read_pickle(fname)
        pd.testing.assert_frame_equal(dfr, df)

    def xtest_get_channel_avg_concentration_tw(self, gtm):
        df = gtm.get_channel_avg_concentration("ec", "441", "15JAN2020 - 31JAN2020")
        assert len(df) > 100
        df2 = gtm.get_channel_concentration(
            "ec", 1, "upstream", "16JAN2020 - 31JAN2020"
        )
        assert len(df) > 10

    def test_get_reservoir_concentration(self, gtm):
        df = gtm.get_reservoir_concentration("ec", "liberty")
        assert df is not None
        fname = os.path.join(os.path.dirname(__file__), "data", "gtm_ec_liberty.pkl")
        # --- regression saves for compare
        df.to_pickle(fname)
        return
        dfr = pd.read_pickle(fname)
        pd.testing.assert_frame_equal(dfr, df)

    def test_get_reservoir_concentration_tw(self, gtm):
        df = gtm.get_reservoir_concentration("ec", "liberty", "15JAN2020 - 31JAN2020")
        assert len(df) > 100
