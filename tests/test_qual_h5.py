import os
import pytest
from pydsm.qualh5 import QualH5, dsm2h5
import numpy as np
import pandas as pd


class TestQualH5:
    @pytest.fixture(scope="module")
    def qual(self):
        filename = os.path.join(os.path.dirname(__file__), 'data', 'historical_v82_ec.h5')
        return QualH5(filename)

    def test_open_non_qual_file(self):
        try:
            filename = os.path.join(os.path.dirname(__file__), 'data', 'historical_v82.h5')
            QualH5(filename)
            pytest.fail(
                f'This should not have been successful as {filename} is not a qual tidefile')
        except:
            pass

    def test_get_input_tables(self, qual):
        tables = qual.get_input_tables()
        assert len(tables) > 10
        for t in tables:
            df = qual.get_input_table(t)
            assert df is not None

    def test_get_data_tables(self, qual):
        tables = qual.get_data_tables()
        assert 'channel concentration' in tables

    def test_channels(self, qual):
        channels = qual.get_channels()
        assert channels[0][0] == '1'
        assert qual.channel_number2index['441'] == 420

    def test_channel_locations(self, qual):
        channel_locs = qual.get_channel_locations()
        assert not channel_locs.empty
        assert channel_locs[0][0] == 'upstream'
        assert channel_locs[0][1] == 'downstream'

    def test_get_reservoirs(self, qual):
        reservoirs = qual.get_reservoirs()
        assert not reservoirs.empty
        assert reservoirs.iloc[0][0] == 'bethel'

    def test_read_attributes(self, qual):
        tflow = qual.h5.get('/output/channel concentration')
        attrs = dsm2h5.read_attributes_from_table(tflow)
        assert attrs == {'interval': '60T',
                         'model': 'Qual',
                         'model_version': '8.2',
                         'start_time': pd.to_datetime('04JAN1990 0000')}

    def test_is_sequence_like(self):
        assert dsm2h5.is_sequence_like([])
        assert dsm2h5.is_sequence_like(np.array(5))

    def test_get_channel_concentration_441(self, qual):
        df = qual.get_channel_concentration('ec', '441', 'upstream')
        assert len(df) > 100
        fname = os.path.join(os.path.dirname(__file__), 'data', 'ec_441_up.pkl')
        # --- regression saves for compare
        #df.to_pickle(fname)
        #return
        dfr = pd.read_pickle(fname)
        pd.testing.assert_frame_equal(dfr, df)

    def test_get_channel_avg_concentration(self,qual):
        df = qual.get_channel_avg_concentration('ec','441')
        assert len(df) > 100
        fname = os.path.join(os.path.dirname(__file__), 'data', 'ec_avg_441.pkl')
        # --- regression saves for compare
        #df.to_pickle(fname)
        #return
        dfr = pd.read_pickle(fname)
        pd.testing.assert_frame_equal(dfr, df)


    def test_get_channel_avg_concentration_tw(self, qual):
        df = qual.get_channel_avg_concentration('ec','441','05JAN1990 - 11JAN1990')
        assert len(df) > 100
        df2 = qual.get_channel_concentration('ec', 1,'upstream','08JAN1990 - 10JAN1990')
        assert len(df) > 10

    def test_get_reservoir_concentration(self, qual):
        df = qual.get_reservoir_concentration('ec','liberty')
        assert df is not None
        fname = os.path.join(os.path.dirname(__file__), 'data', 'ec_liberty.pkl')
        # --- regression saves for compare
        #df.to_pickle(fname)
        #return
        dfr = pd.read_pickle(fname)
        pd.testing.assert_frame_equal(dfr, df)
    
    def test_get_reservoir_concentration_tw(self, qual):
        df = qual.get_reservoir_concentration('ec','bethel','11JAN1990 - 23JAN1990')
        assert len(df) > 100