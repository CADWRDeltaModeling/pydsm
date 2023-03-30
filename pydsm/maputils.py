import geopandas as gpd

def read_dsm2_chans(dsm2_chans_centerline_shapefile):
    dsm2_chans = gpd.read_file(dsm2_chans_centerline_shapefile)
    return dsm2_chans.to_crs('EPSG:4326')