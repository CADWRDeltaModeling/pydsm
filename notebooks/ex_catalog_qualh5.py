# %%
import pydsm
from pydsm import qualh5
import pandas as pd

# %%

tidefile = "data/historical_v82_ec.h5"
qualt = qualh5.QualH5(tidefile)
# %%


# %%
def create_catalog(dfc, variable, unit, updown=True, prefix="CHAN_", id_column=0):
    catalog = dfc.copy()
    catalog = catalog.rename(columns={id_column: "id"})
    catalog["id"] = prefix + catalog["id"].astype(str)
    catalog["variable"] = variable.upper()
    catalog["unit"] = unit
    catalog["filename"] = tidefile
    if updown:
        catalog_up = catalog.copy()
        catalog_down = catalog.copy()
        catalog_up["id"] = catalog_up["id"] + "_UP"
        catalog_down["id"] = catalog_down["id"] + "_DOWN"
        catalog = pd.concat([catalog_up, catalog_down])
    return catalog.reset_index(drop=True)


# %%
dfc = qualt.get_channels()
dfc
# %%
dfr = qualt.get_reservoirs()
dfr
# %%
dfcon = qualt.get_constituents()
dfcon
# %%
pd.concat(
    [create_catalog(dfc, r["constituent_names"], "mg/L") for _, r in dfcon.iterrows()]
)
# %%
pd.concat(
    [
        create_catalog(
            dfr, r["constituent_names"], "mg/L", prefix="RES_", id_column="name"
        )
        for _, r in dfcon.iterrows()
    ]
)

# %%


cat_flow = create_catalog(dfc, "flow", "ft/s")
cat_area = create_catalog(dfc, "avg_area", "ft^2")
cat_avg_area = create_catalog(dfc, "avg_area", "ft^2", updown=False)
cat_stage = create_catalog(dfc, "stage", "ft")
# %%
# %%
dfr = hydro.get_reservoirs()
cat_res_height = create_catalog(
    dfr, "height", "ft", updown=False, prefix="RES_", id_column="name"
)
# %%
dfrn = hydro.get_reservoir_node_connections()
hydro.get_input_table("/hydro/input/reservoir_connection")
dfrn["id"] = (
    "RES_"
    + dfrn["res_name"].astype(str).str.upper()
    + "_NODE_"
    + dfrn["ext_node_no"].astype(str)
)
cat_res_node_flow = create_catalog(
    dfrn, "flow", "ft^3/s", updown=False, id_column="id", prefix=""
)
# %%
dfq = hydro.get_qext()
cat_qext_flow = create_catalog(
    dfq, "flow", "ft^3/s", updown=False, prefix="QEXT_", id_column="name"
)
# %%
dft = hydro.get_transfer_names()
cat_transfer_flow = create_catalog(
    dft, "flow", "ft^3/s", updown=False, id_column=0, prefix="TRANSFER_"
)
# %%
catalog = pd.concat(
    [
        cat_flow,
        cat_area,
        cat_avg_area,
        cat_stage,
        cat_res_height,
        cat_res_node_flow,
        cat_qext_flow,
        cat_transfer_flow,
    ]
)
# %%
catalog
# %%
