# Displays points on map
# Points can be selected and it shows a view for given selectino
import panel as pn
import param
import hvplot.pandas
import pandas as pd
import holoviews as hv
from holoviews import opts, dim
hv.extension('bokeh')
pn.extension()


class MapSelectionViewer(param.Parameterized):
    """Shows view  associated with points displayed on map
    Requires a dataframe with Latitude and Longitude columns for points

    """
    selected = param.List(
        default=[0], doc='Selected point indices for view')

    def __init__(self, stations, **kwargs):
        super().__init__(**kwargs)
        self.stations = stations
        self.points_map = self.stations.hvplot.points('Longitude', 'Latitude', geo=True, tiles='CartoLight',
                                                      frame_height=400, frame_width=300,
                                                      fill_alpha=0.9, line_alpha=0.4,
                                                      hover_cols=['index', 'ID', 'Station'])
        self.points_map = self.points_map.opts(opts.Points(tools=['tap', 'hover'], size=10,
                                                           selection_color='green',
                                                           nonselection_color='red', nonselection_alpha=0.3,
                                                           active_tools=['wheel_zoom']))
        # create a selection and add it to a dynamic map calling back show_ts
        self.select_stream = hv.streams.Selection1D(source=self.points_map, index=[0])
        self.select_stream.add_subscriber(self.set_selected)

    def set_selected(self, index):
        """ sets selected with indicies when selection changes 
        Use @param.depends('selected') on methods on this class that you want to update
        """
        if index is None or len(index) == 0:
            pass  # keep the previous selections
        else:
            self.selected = index

    @param.depends('selected')
    def selection_view(self):
        return pn.widgets.DataFrame(self.stations.iloc[self.selected, :])
        #return pn.pane.Markdown(f'Selected: {self.selected}')

    def view(self):
        return pn.Row(pn.Column(self.points_map), pn.Column(self.selection_view))
