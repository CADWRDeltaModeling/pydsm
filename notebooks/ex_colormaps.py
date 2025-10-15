# %%
import colorcet as cc

import panel as pn

pn.extension()

# Get sorted colormap names
sorted_palettes = {key: cc.palette[key] for key in sorted(cc.palette.keys())}

cmap_selector = pn.widgets.ColorMap(options=sorted_palettes)

pn.panel(cmap_selector).show()
# %%
