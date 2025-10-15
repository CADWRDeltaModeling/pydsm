"""
Qual H5 reader.

"""

import h5py
import pandas as pd
import numpy as np

from . import dsm2h5


class QualH5:
    """
    DSM2 Qual HDF5
    ---------------
    DSM2 outputs water quality constituents in HDF5 format.

    The outputs are at:

    - output:

     - /constiuent_names
     - /channel concentration (channel id, upstream/downstream)
     - /channel avg concentration  (channel id)
     - /reservoir concentration (reservoir name)


    """

    def __init__(self, filename, filemode="r"):
        """
        opens a handle to the filename in read mode
        """
        self.filename = filename
        self.h5 = h5py.File(filename, filemode)
        if (
            not dsm2h5.get_model(self.h5) == "qual"
            and not dsm2h5.get_model(self.h5) == "gtm"
        ):
            raise ValueError(
                f"{filename} is not a qual tidefile: Could be "
                + dsm2h5.get_model(self.h5)
                + " ?"
            )
        # initialization of tables needed
        self.get_constituents()
        self.get_channels()
        self.get_channel_numbers()
        self.get_channel_locations()
        self.get_reservoirs()

    def __del__(self):
        """
        closes file as cleanup
        """
        self.h5.close()

    def is_gtm(self):
        return dsm2h5.get_model(self.h5) == "gtm"

    def get_start_end_dates(self):
        """
        return the start and end dates of the simulation
        """
        return dsm2h5.get_start_end_dates(self, scalar_table="/input/scalar")

    def get_output_interval(self):
        attr = dsm2h5.read_attributes_from_table(
            self.h5.get("/output/channel concentration")
        )
        return attr["interval"]

    def get_output_timedelta(self):
        """
        Return the output interval as pandas.Timedelta
        """
        raw = self.get_output_interval()
        try:
            return pd.to_timedelta(raw)
        except ValueError:
            # fallback simple normalization (e.g. '60MIN', '1HOUR', etc.)
            norm = (
                raw.lower()
                .replace("hours", "h")
                .replace("hour", "h")
                .replace("mins", "min")
                .replace("minutes", "min")
                .replace("minute", "min")
            )
            return pd.to_timedelta(norm)

    def get_output_freq(self):
        """
        Return a pandas offset/frequency object usable for date_range
        """
        return pd.tseries.frequencies.to_offset(self.get_output_timedelta())

    def get_input_tables(self):
        return dsm2h5.get_paths_for_group_path(self.h5, "/input")

    def get_input_table(self, table_path):
        """See get_input_tables for a list of table paths
        Returns a dataframe for the contents of the table at the path"""
        return dsm2h5.read_table_as_df(self.h5, table_path)

    def get_data_tables(self):
        return [
            "channel avg concentration",
            "channel concentration",
            "reservoir concentration",
        ]

    def get_constituents(self):
        """
        get list of constituents
        """
        df = dsm2h5.read_table_as_df(self.h5, "output/constituent_names")
        df.columns = ["constituent_names"]
        self.constituents = {v: k for k, v in df["constituent_names"].to_dict().items()}
        return df

    def get_channels(self):
        """
        return pandas DataFrame of channel ids as indexed
        """
        channels = pd.DataFrame(self.h5.get("/output/channel_number"), dtype=str)
        self.channel_index2number = channels[0].to_dict()
        self.channel_number2index = {
            value: key for key, value in self.channel_index2number.items()
        }
        return channels

    def get_channel_locations(self):
        """
        return pandas DataFrame of channel locations ( upstream or downstream)
        """
        self.channel_locs = pd.DataFrame(self.h5.get("/output/channel_location"))
        self.channel_locs = pd.DataFrame(
            self.channel_locs.iloc[:, 0].str.decode("utf-8")
        )
        self.channel_location2number = self.channel_locs[0].to_dict()
        self.channel_location2index = {
            value: key for key, value in self.channel_location2number.items()
        }
        return self.channel_locs

    def get_reservoirs(self):
        """
        return pandas DataFrame of reservoirs
        """
        self.reservoirs = pd.DataFrame(
            self.h5.get("/output/reservoir_names"), columns=["name"]
        )
        # Before the assignment, convert the values to the correct dtype
        return pd.DataFrame(
            self.reservoirs.iloc[:, 0].str.decode("utf-8"), columns=["name"]
        )
        return self.reservoirs

    def get_channel_numbers(self):
        """
        get index to channel number table
        """
        df = pd.DataFrame(self.h5.get("output/channel_number"))
        df.columns = ["channel_number"]
        return df

    def create_catalog(self):
        dfc = self.get_channels()
        dfr = self.get_reservoirs()
        dfcon = self.get_constituents()
        chan_cat = pd.concat(
            [
                dsm2h5.create_catalog_entry(
                    self.filename,
                    dfc,
                    r["constituent_names"],
                    "mg/L",
                )
                for _, r in dfcon.iterrows()
            ]
        )
        if dsm2h5.get_model(self.h5) == "qual":  # no avg concentration for gtm
            chan_avg_cat = pd.concat(
                [
                    dsm2h5.create_catalog_entry(
                        self.filename,
                        dfc,
                        r["constituent_names"],
                        "mg/L",
                        updown=False,
                        prefix="CHAN_",
                    )
                    for _, r in dfcon.iterrows()
                ]
            )
        res_chat = pd.concat(
            [
                dsm2h5.create_catalog_entry(
                    self.filename,
                    dfr,
                    r["constituent_names"],
                    "mg/L",
                    updown=False,
                    prefix="RES_",
                    id_column="name",
                )
                for _, r in dfcon.iterrows()
            ]
        )
        if dsm2h5.get_model(self.h5) == "qual":
            return pd.concat([chan_cat, chan_avg_cat, res_chat])
        else:
            return pd.concat([chan_cat, res_chat])

    def get_data_for_catalog_entry(self, catalog_entry, time_window=None):
        """
        Get the data for a catalog entry
        """
        idfields = catalog_entry["id"].split("_")
        objtype = idfields[0]
        objid = idfields[1]
        if len(idfields) > 2:
            objlocid = idfields[2].lower() + "stream"
        else:
            objlocid = None
        if objtype == "CHAN":
            if objlocid is None:
                return self.get_channel_avg_concentration(
                    catalog_entry["variable"], objid, time_window
                )
            else:
                return self.get_channel_concentration(
                    catalog_entry["variable"],
                    objid,
                    objlocid,
                    timewindow=time_window,
                )
        elif objtype == "RES":
            return self.get_reservoir_concentration(
                catalog_entry["variable"], objid, timewindow=time_window
            )
        else:
            raise ValueError("Unknown type: " + catalog_entry["type"])

    def _names_to_constituent_indices(self, constituent_name):
        if isinstance(constituent_name, str):
            return self.constituents[constituent_name]
        elif dsm2h5.is_sequence_like(constituent_name):
            return [self.constituents[id] for id in constituent_name]
        else:
            raise RuntimeError(
                f"constituent_name should be string or sequence of strings: Called with {constituent_name!r} of type {type(constituent_name)}"
            )

    def _channel_ids_to_sequence(self, channel_id_slice):
        """
        convert a slice of channel ids to a slice of channel indices into data table
        """
        if isinstance(channel_id_slice, str):
            return [channel_id_slice]
        else:
            return channel_id_slice

    def _channel_ids_to_indicies(self, channel_id_slice):
        """
        convert a slice of channel ids to a slice of channel indices into data table
        """
        # Support special keyword 'all' to select all channels
        if isinstance(channel_id_slice, str):
            if channel_id_slice.lower() == "all":
                # Return all indices in order
                # channel_index2number maps index->number; keys are indices
                return list(self.channel_index2number.keys())
            return self.channel_number2index[channel_id_slice]
        elif dsm2h5.is_sequence_like(channel_id_slice):
            return [self.channel_number2index[id] for id in channel_id_slice]
        elif isinstance(channel_id_slice, slice):
            return channel_id_slice
        else:
            raise RuntimeError(
                f"Channel id should be string, sequence of strings or slice: Called with: {channel_id_slice!r} of type {type(channel_id_slice)}"
            )

    def _channel_locations_to_indicies(self, channel_location_slice):
        """
        convert a slice of channel ids to a slice of channel indices into data table
        """
        if isinstance(channel_location_slice, str):
            return self.channel_location2index[channel_location_slice]
        elif dsm2h5.is_sequence_like(channel_location_slice):
            return [self.channel_location2index[id] for id in channel_location_slice]
        elif isinstance(channel_location_slice, slice):
            return channel_location_slice
        else:
            raise RuntimeError(
                f"Channel location should be string, sequence of strings or slice: Called with: {channel_location_slice!r} of type {type(channel_location_slice)}"
            )

    def _get_channel_ts(
        self,
        table_path,
        constituent_names,
        channels,
        location="upstream",
        timewindow=None,
    ):
        """
        return a pandas DataFrame of time series from the table for the given
            constituent name(s)
            list of channels
            channel_location ('upstream' or 'downstream')
            timewindow in the format of <<start time str>> - <<end time str>>, e.g. 01JAN1990 - 05JUL1992

        Parameters
        ----------
        table_path : str
            HDF5 dataset path.
        constituent_names : str | list[str]
            Constituent(s) whose concentrations are requested.
        channels : str | list[str]
            Channel identifier(s). May be a single channel number, a list of
            numbers, or the special keyword "all" (case-insensitive) which
            expands to every channel in the tide file in index order.
        location : str | None, default "upstream"
            Either "upstream" or "downstream"; ignored (pass None) for
            datasets without the location dimension (e.g. avg concentration).
        timewindow : str | None
            Optional DSM2 style time window "START-END". If provided the
            result is limited to that interval.

        Returns
        -------
        pandas.DataFrame
            Time-indexed data with one column per requested channel (and
            location suffix when applicable).
        """
        constituent_indices = self._names_to_constituent_indices(constituent_names)
        if isinstance(channels, list):
            channels = [str(c) for c in channels]
        else:
            channels = str(channels)
        # Expand 'all' into the full ordered channel list for proper column naming
        if isinstance(channels, str) and channels.lower() == "all":
            # Reconstruct ordered list of channel numbers by index order
            channels = [
                self.channel_index2number[i]
                for i in sorted(self.channel_index2number.keys())
            ]
        channel_indices = self._channel_ids_to_indicies(channels)
        if location:
            location_indices = self._channel_locations_to_indicies(location)
            df = dsm2h5.read_time_indexed_table(
                self.h5,
                table_path,
                timewindow,
                constituent_indices,
                channel_indices,
                location_indices,
            )
            df.columns = [
                f"{id}-{location}" for id in self._channel_ids_to_sequence(channels)
            ]
        else:
            df = dsm2h5.read_time_indexed_table(
                self.h5, table_path, timewindow, constituent_indices, channel_indices
            )
            df.columns = [f"{id}" for id in self._channel_ids_to_sequence(channels)]
        return df

    def _get_reservoir_ts(
        self, table_path, constituent_names, reservoirs_names, timewindow=None
    ):
        """ """
        constituent_indices = self._names_to_constituent_indices(constituent_names)
        res = self.get_reservoirs()
        res_names = dsm2h5.normalize_to_slice(reservoirs_names)
        indices = res[res.name.isin(res_names)].index.values
        df = dsm2h5.read_time_indexed_table(
            self.h5, table_path, timewindow, constituent_indices, indices
        )
        df.columns = list(res_names)
        return df

    def get_channel_concentration(
        self, constituent_name, channel_id, location_id="upstream", timewindow=None
    ):
        """Return channel concentration time series for a constituent.

        Parameters
        ----------
        constituent_name : str
            Constituent identifier (e.g. 'ec').
        channel_id : str | int | list[str|int]
            Channel identifier(s) or the special keyword "all" for every
            channel. Lists can mix ints/strings.
        location_id : str, default "upstream"
            Channel end: "upstream" or "downstream".
        timewindow : str | None
            Optional DSM2 style window "START-END" (e.g. "15JAN2020 - 31JAN2020").

        Returns
        -------
        pandas.DataFrame
            Time-indexed concentrations with one column per requested channel
            (location suffix included when applicable).
        """
        return self._get_channel_ts(
            "/output/channel concentration",
            constituent_name,
            channel_id,
            location_id,
            timewindow,
        )

    def get_channel_avg_concentration(
        self, constituent_name, channel_id, timewindow=None
    ):
        """Return channel average concentration time series (no location dimension).

        Parameters
        ----------
        constituent_name : str
            Constituent identifier.
        channel_id : str | int | list[str|int]
            Channel identifier(s) or "all".
        timewindow : str | None
            Optional DSM2 style window.
        """
        return self._get_channel_ts(
            "/output/channel avg concentration",
            constituent_name,
            channel_id,
            None,
            timewindow,
        )

    def get_reservoir_concentration(
        self, constituent_name, reservoir_name, timewindow=None
    ):
        """
        get reservoir concentration
        """
        return self._get_reservoir_ts(
            "/output/reservoir concentration",
            constituent_name,
            reservoir_name,
            timewindow,
        )
