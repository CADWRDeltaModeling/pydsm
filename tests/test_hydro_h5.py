import os
import pytest
from pydsm import dsm2h5
from pydsm.hydroh5 import HydroH5
import numpy as np
import pandas as pd


class TestHydroH5:
    @pytest.fixture(scope="module")
    def hydro(self):
        filename = os.path.join(os.path.dirname(__file__), 'data', 'historical_v82.h5')
        return HydroH5(filename)

    def test_open_non_hydro_file(self):
        try:
            filename = os.path.join(os.path.dirname(__file__), 'data', 'historical_v82_ec.h5')
            HydroH5(filename)
            pytest.fail(
                f'This should not have been successful as {filename} is not a hydro tidefile')
        except:
            pass

    def test_is_hydro_file(self, hydro):
        assert hydro.h5['/hydro'] != None

    def test_get_input_tables(self, hydro):
        tables = hydro.get_input_tables()
        assert len(tables) > 10
        for t in tables:
            df = hydro.get_input_table(t)
            assert df is not None

    def test_get_geometry_tables(self, hydro):
        tables = hydro.get_geometry_tables()
        assert len(tables) > 10
        for t in tables:
            df = hydro.get_geometry_table(t)
            assert df is not None

    def test_get_data_tables(self, hydro):
        tables = hydro.get_data_tables()
        assert 'channel flow' in tables

    def test_channels(self, hydro):
        channels = hydro.get_channels()
        assert channels[0][0] == '1'
        assert hydro.channel_number2index['441'] == 420

    def test_channel_locations(self, hydro):
        channel_locs = hydro.get_channel_locations()
        assert not channel_locs.empty
        assert channel_locs[0][0] == 'upstream'

    def test_get_reservoirs(self, hydro):
        reservoirs = hydro.get_reservoirs()
        assert not reservoirs.empty
        assert reservoirs.iloc[0][0] == 'bethel'
        assert hydro.reservoir_node_connections['res_name'][0].strip(
        ) == 'bethel'

    def test_get_qext(self, hydro):
        qext = hydro.get_qext()
        df = qext[qext['name'] == 'sac']
        assert len(df) == 1
        df.iloc[0]['attach_obj_name'] == '330'
        assert not qext.empty

    def test_read_attributes(self, hydro):
        tflow = hydro.h5.get('/hydro/data/channel flow')
        attrs = dsm2h5.read_attributes_from_table(tflow)
        assert attrs == {'interval': '30T',
                         'model': 'Hydro',
                         'model_version': '8.2',
                         'start_time': pd.to_datetime('02JAN1990 0000')}

    def test_is_sequence_like(self, hydro):
        assert dsm2h5.is_sequence_like([])
        assert dsm2h5.is_sequence_like(np.array(5))

    def test_get_channel_flow_441(self, hydro):
        f441 = hydro.get_channel_flow('441', 'upstream')
        assert f441.count()[0] == 1393

    def test_get_channel_flow(self, hydro):
        flow4up = hydro.get_channel_flow('4', 'upstream')
        flow4down = hydro.get_channel_flow('4', 'downstream')
        assert flow4down.count()[0] == 1393
        assert flow4down.mean()[0] == pytest.approx(1248.9728)
        flow4_5up = hydro.get_channel_flow(['4', '5'], 'upstream')
        assert len(flow4_5up.columns) == 2
        assert flow4_5up.mean()[0] == pytest.approx(1244.4728)
        assert flow4_5up.mean()[1] == pytest.approx(1248.0035)
        # --- regression saves for compare
        # flow4up.to_pickle('flow4up.pkl')
        # return
        fname = os.path.join(os.path.dirname(__file__), 'data', 'flow4up.pkl')
        cflow4up = pd.read_pickle(fname)
        pd.testing.assert_frame_equal(cflow4up, flow4up)
        # --- get with time window
        flow4uptw = hydro.get_channel_flow(
            '4', 'upstream', '05JAN1990 - 07JAN1990')
        assert flow4uptw.index[0] == pd.to_datetime('05JAN1990')
        assert flow4uptw.index[-1] == pd.to_datetime('06JAN1990 2330')
        assert flow4up.loc['05JAN1990 2300'][0] == flow4uptw.loc['05JAN1990 2300'][0]

    def test_get_channel_area(self, hydro):
        area4up = hydro.get_channel_area('4', 'upstream')
        # --- regression saves for compare
        # area4up.to_pickle('area4up.pkl')
        # return
        fname = os.path.join(os.path.dirname(__file__), 'data', 'area4up.pkl')
        carea4up = pd.read_pickle(fname)
        pd.testing.assert_frame_equal(carea4up, area4up)

    def test_get_channel_stage(self, hydro):
        stage4up = hydro.get_channel_stage('4', 'upstream')
        # --- regression saves for compare
        # stage4up.to_pickle('stage4up.pkl')
        # return
        fname = os.path.join(os.path.dirname(__file__), 'data', 'stage4up.pkl')
        cstage4up = pd.read_pickle(fname)
        pd.testing.assert_frame_equal(cstage4up, stage4up)

    def test_get_channel_stage_asint(self, hydro):
        """For makeing sure integer channel ids are supported.

        Parameters
        ----------
        hydro : [type]
            [description]
        """
        stage4up = hydro.get_channel_stage(4, 'upstream')
        # --- regression saves for compare
        # stage4up.to_pickle('stage4up.pkl')
        # return
        fname = os.path.join(os.path.dirname(__file__), 'data', 'stage4up.pkl')
        cstage4up = pd.read_pickle(fname)
        pd.testing.assert_frame_equal(cstage4up, stage4up)

    def test_get_channel_avg_area(self, hydro):
        area441 = hydro.get_channel_avg_area('441')
        fname = os.path.join(os.path.dirname(__file__), 'data', 'area441.pkl')
        # --- regression saves for compare
        #area441.to_pickle(fname)
        #return
        carea441 = pd.read_pickle(fname)
        pd.testing.assert_frame_equal(carea441, area441)

    def test_get_reservoir_flow(self, hydro):
        rflow = hydro.get_reservoir_flow('franks_tract')
        assert not rflow.empty
        assert rflow.values.shape == (1393, 6)

    def test_get_reservoir_height(self, hydro):
        rht = hydro.get_reservoir_height('mildred')
        assert not rht.empty

    def test_get_qext_flow(self, hydro):
        qn = hydro.get_qext()
        assert not qn[qn.name == 'calaveras'].empty
        qf = hydro.get_qext_flow('calaveras')
        assert not qf.empty

    def test_get_transfer_flow(self, hydro):
        try:
            tflow = hydro.get_transfer_flow('0')
            pytest.fail('There should be no transfer flows')
        except:
            pass

    def test_channel_bottom(self, hydro):
        channels=['1','331','441']
        df = hydro.get_channel_bottom(channels)
        assert len(df) == 3
        assert pytest.approx(3.502402, df.loc['1','upstream'])