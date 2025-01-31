#!/usr/bin/env python
# coding: utf-8

import click
import numpy as np
import pandas as pd

import geopandas
import shapely

# PTM Animator
#
# This scripts starts a PTM animation can be done with numpy, pandas, geopandas, shapely and holoviews.
#
import holoviews as hv
from holoviews import opts
import hvplot.pandas
import panel as pn

import pydsm
from pydsm import ptm
from pydsm import hydroh5

hv.extension("bokeh")


def multindex_iloc(df, index):
    label = df.index.levels[0][index]
    return label, df.iloc[df.index.get_loc(label)]


def get_chan_geometry(channel_id, dsm2_chan_geom_map):
    return dsm2_chan_geom_map[channel_id]


def interpolate_positions(dfn, dsm2_chan_geom_map):
    vals = np.empty(len(dfn), dtype="object")
    i = 0
    for r in dfn.iterrows():
        try:
            if r[1].cid > 0:
                geom = get_chan_geometry(r[1].cid, dsm2_chan_geom_map)
                val = geom.interpolate(1.0 - r[1].x / 100.0, normalized=True)
                vals[i] = val
            i = i + 1
        except:
            print("Exception for row: ", i, r)
            raise
    return vals


def get_particle_geopositions(time_index, dfall):
    dt, dfn = multindex_iloc(dfall, time_index)
    # dfn=dfn.droplevel(0) # drop level as time is the slice
    dfn = dfall.loc[dt]
    # dfn['geometry'] = interpolate_positions(dfn) # add new column to copy of full slice so warning is ok
    dfn.assign(geometry=interpolate_positions(dfn))
    dfp = dfn[dfn.cid > 0]
    gdf_merc = geopandas.GeoDataFrame(dfp, geometry="geometry", crs="EPSG:3857")
    # gdf_merc=gdf.to_crs(epsg=3857)
    x = np.fromiter((p.x for p in gdf_merc.geometry.values), dtype=np.float32)
    y = np.fromiter((p.y for p in gdf_merc.geometry.values), dtype=np.float32)
    dfp_xy = dfp.join([pd.DataFrame({"easting": x}), pd.DataFrame({"northing": y})])
    return dfp_xy


def cache_calcs(df, dsm2_chan_geom_map):
    apos = interpolate_positions(df, dsm2_chan_geom_map)
    # df_xy=df.assign('geometry',apos) # do we need the geometry column ?
    x = np.fromiter((p.x if p else None for p in apos), dtype=np.float32)
    y = np.fromiter((p.y if p else None for p in apos), dtype=np.float32)
    df_xy = df.assign(easting=x, northing=y)
    # save dfall_xy to cached parquet format or feather format
    return df_xy


# dfall_xy.to_feather(ptm_file+'.feather') # failed to serialize multiindex
# dfall_xy.to_parquet(ptm_file+'.parquet') # executed in 2.01s, 30 MB
# dfall_xy.to_csv(ptm_file+'.csv') # executed in 29.6s, 348 MB
# dfall_xy.to_pickle(ptm_file+'.pickle') # executed in 469ms, 138 MB
# display(map)


@click.command()
@click.argument("ptm_file", type=click.Path(exists=True))
@click.argument("hydro_file", type=click.Path(exists=True))
@click.argument("flowlines_shape_file", type=click.Path(exists=True))
def ptm_animate(ptm_file, hydro_file, flowlines_shape_file):
    # Tiles always in pseudo mercator epsg=3857
    # Load PTM animation file
    # ptm_file = 'D:/delta/dsm2_v8.2.0b1/studies/historical/output/anim_db.bin'
    ptm_data = ptm.load_anim_data(ptm_file)
    # print('PTM Animation file has %d records' % len(ptm_data))
    # print('Animation starts at %s %04d' %
    #      (ptm_data[0].model_date.decode("utf-8"), ptm_data[0].model_time))
    # print('Animation ends at %s %04d' %
    #      (ptm_data[-1].model_date.decode("utf-8"), ptm_data[-1].model_time))
    # print('There are %d particles in this animation' % ptm_data[0].nparticles)
    dfall = ptm.create_frame_for_anim_data(ptm_data)
    # load hydrodynamic information for PTM
    # hydro_file = 'D:/delta/dsm2_v8.2.0b1/studies/historical/output/historical_v82.h5'
    hydro = hydroh5.HydroH5(hydro_file)
    chan_int2ext = pd.DataFrame(
        hydro.channel_index2number.items(), columns=["internal", "external"]
    )
    chan_int2ext = chan_int2ext.fillna(-1)  # Fill NaN values with -1
    chan_int2ext = chan_int2ext.astype({"internal": np.int32, "external": np.int32})

    chan_int2ext = chan_int2ext.assign(internal=chan_int2ext.internal + 1)
    chan_int2ext.index = chan_int2ext.internal
    dfallj = dfall.join(chan_int2ext, on="cid")
    dfallj = dfallj.fillna({"internal": -1, "external": -1})
    dfallj = dfallj.astype({"internal": np.int32, "external": np.int32})
    # dsm2_chans=geopandas.read_file('maps/v8.2/DSM2_Channels.shp').to_crs(epsg=3857)
    # flowlines_shape_file = 'D:/delta/maps/v8.2/DSM2_Flowline_Segments.shp'
    dsm2_chans = geopandas.read_file(flowlines_shape_file).to_crs(epsg=3857)
    dsm2_chan_geom_map = {}
    for r in dsm2_chans.iterrows():
        chan = r[1]
        chan_index = hydro.channel_number2index[str(chan.channel_nu)]
        dsm2_chan_geom_map[chan_index + 1] = chan.geometry
    # uncomment below to recalculate cached
    try:
        dfall_xy = pd.read_pickle(ptm_file + ".pickle")  # executed in 195ms
        # pd.read_parquet(ptm_file+'.parquet') # executed in 870 ms
        # pd.read_csv(ptm_file+'.csv') # executed in 4.30s,
    except:
        print("Failed to load cached calculated particle positions. Recalculating ...")
        dfall_xy = cache_calcs(dfall, dsm2_chan_geom_map)
        dfall_xy.to_pickle(ptm_file + ".pickle")  # executed in 469ms
    # map from index+1 (numbers 1--> higher to )
    b = dsm2_chans.to_crs(epsg=3857).geometry.bounds
    extents = (b.minx.min(), b.miny.min(), b.maxx.max(), b.maxy.max())
    map = hv.element.tiles.OSM().opts(width=800, height=600)
    map.extents = extents
    #

    def particle_map(ti):
        tlabel = dfall_xy.index.levels[0][ti]
        dfp_xy = dfall_xy.loc[tlabel]
        ov = map * dfp_xy.hvplot.points(
            x="easting", y="northing", hover_cols=["id", "cid"]
        ).opts(title="Time: %s" % tlabel, framewise=False)
        return ov

    #
    dmap = hv.DynamicMap(particle_map, kdims=["ti"])
    time_index = dfall_xy.index.levels[0]
    anim_pane = dmap.redim.range(ti=(0, len(ptm_data) - 1)).opts(framewise=True)
    p = pn.panel(anim_pane)
    titlePane = pn.pane.Markdown(
        """ # Particle Animation: 
    Move the slider to the time desired or select the slider and use the left/right arrow keys to run animation {back/for}wards"""
    )
    anim_panel = pn.Column(pn.Row(titlePane, *p[1]), p[0])
    server = anim_panel.show()


# dsm2_chans.hvplot.line()
# anim_pane.select(ti=1500)
# pl1=dfall_xy[dfall_xy.id==373].cid.hvplot()
# pl2=dfall_xy[dfall_xy.id==151].cid.hvplot()
# pl1*pl2
# anim_pane.select(ti=500)
# from holoviews.streams import Selection1D
# sel = Selection1D(source=anim_pane[0].Points.I)
# print(anim_pane)
# hv.DynamicMap(lambda index: dfall_xy[dfall_xy.index==id].hvplot(), streams=[sel])


if __name__ == "__main__":
    import sys

    sys.exit(ptm_animate())
