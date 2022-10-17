# -*- coding: utf-8 -*-

from operator import itemgetter

import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype

from . import spatial, utils
from .config import defaults


class Normalize_functions(object):

    """
    Mix ins of normalization functions of feed class
    """

    def normalize(
        self,
        agency_name=None,
        route_name_length=50,
        group_distance=25,
        group_share=0.65,
        nb_share={0: 10, 1: 10, 2: 10, 3: 3},
        default_day=1,
        one_year=True,
        overwrite_shapes=False,
        coordinates=6,
    ):
        """
        apply normalization steps and clean un-needed ids
        
        Args :
            - one_year : boolean to filter to only one year
            - agency_name : if None, do not replace agency name
                            if False, replace by feed name,
                            if True, replace by most common agency_name
            - group_distance : distance between stops for route grouping
            - group_share : share of common stops to group routes
            - nb_share : minimum nb of common stops
            - default_day : day used for calculation
            - coordinates : number of decimals for longitude and latitude or None
        """

        # ----------------------------
        # defaults and data cleaning

        # single agency for each folder, optionaly replace agency name
        self.simple_agency(agency_name=agency_name)

        # defaults
        if not all([v for k, v in self.has_defaults().items()]):
            self.set_defaults()

        # modify route_names
        self.optimize_route_names(route_name_length=route_name_length)

        # ----------------------------
        # normalize stops

        # some feeds mix stop_code and stop_id
        self.fix_invalid_stopids()

        # correct bad coordinates and duplicated stop_names
        self.drop_bad_coordinates()

        # decrease spatial precision
        if coordinates is not None:
            self.compress_coordinates(decimals=coordinates)

        # -----------------------------
        # normalize trips

        # unique route_id for each trip_id / service_id
        if not self.has_simple_tripids():
            self.simplify_routes_on_tripids()

        # check max arrival_time larger than min departure_time
        self.drop_non_increasing_stoptimes()

        # unique trip_ids
        if not self.has_normalized_tripids():
            self.normalize_trips()

        # fill missing arrival and departure times
        if not self.has_filled_times():
            self.fill_times()

        # ----------------------------
        # normalize shapes and add basic shapes from stop to stop
        self.simple_shapes(overwrite_shapes)

        # ----------------------------
        # group routes

        self.set_groupid(
            distance=group_distance,
            share=group_share,
            nb_share=nb_share,
            day=default_day,
        )

        self.prune_ids(step_text="final cleaning")

        return None

    # --------------------------------------------------------------------------
    # normalisation functions

    def simplify_routes_on_tripids(self):
        """
        drop all routes if at least one trip_id is duplicated,
        except if duplicated for different service_ids
        """

        # remove duplicate trip_id/service_id/route_id
        self.trips = self.trips.drop_duplicates(
            ["trip_id", "service_id", "route_id"], keep="first"
        )

        rids = self._routeids_for_duplicated_tripids()

        self.routes = self.routes.loc[~self.routes.route_id.isin(rids)].copy()

        self.prune_ids(step_text="routes with duplicated trips")

        return None

    def _routeids_for_duplicated_tripids(self):
        """
        return list of route_ids when at least one trip_id is duplicated
        """
        # drop if
        # all important id are duplicated
        mask1 = self.trips.duplicated(
            subset=["trip_id", "service_id", "route_id", "direction_id"], keep=False
        )

        # or
        # trip_id is duplicated except if it is not trip_id and route_id
        # (route_id-trip_id are duplicated if differente service_id) according to mask4

        mask2 = self.trips.duplicated("trip_id", keep=False)
        mask3 = ~self.trips.duplicated(["trip_id", "route_id"], keep=False)

        # and trip_id / service_id is duplicated
        mask4 = self.trips.duplicated(["trip_id", "service_id"], keep=False)

        rids = self.trips.loc[(mask1) | (((mask2) & (mask3)) | (mask4))]
        rids = rids["route_id"].drop_duplicates()

        return rids

    def optimize_route_names(self, route_name_length=None):
        """
        if non unique route_short_name and no na in route_long_name,
        replace by route_long_name
        delete route_long_name if same as route_short_name
        compress routes with same route_short_name to same route_id
        """
        l = self.routes.shape[0]
        na_longs = len(self.routes.loc[self.routes.route_long_name.isna()])
        if len(self.routes.route_short_name.drop_duplicates()) != l and na_longs == 0:
            self.routes.route_short_name = self.routes.route_long_name

        # if all route_short_names = route_long_name, drop route_long_name
        b = self.routes.route_short_name == self.routes.route_long_name
        if b.all():
            self.routes = self.routes.drop(columns=["route_long_name"])

        # make route_short_name unique by merging same routes
        if (
            len(self.routes.drop_duplicates(subset=["route_short_name", "route_type"]))
            != l
        ):
            df = self.routes.copy()
            df = df.assign(
                unique_rid=df.groupby(["route_short_name", "route_type"])[
                    "route_id"
                ].transform("first")
            )

            # update route_id in trips and fare_rules
            df2 = pd.merge(self.trips, df[["route_id", "unique_rid"]], on="route_id")
            df2 = df2.drop(columns="route_id").rename(
                columns={"unique_rid": "route_id"}
            )
            self.trips = df2

            if self.fare_rules is not None:
                df2 = pd.merge(
                    self.fare_rules, df[["route_id", "unique_rid"]], on="route_id"
                )
                df2 = df2.drop(columns="route_id").rename(
                    columns={"unique_rid": "route_id"}
                )
                self.fare_rules = df2

            # drop duplicated routes
            df = df.drop_duplicates(subset=["unique_rid", "route_type"])
            df = df.drop(columns="route_id").rename(columns={"unique_rid": "route_id"})
            self.routes = df

        # trim length of route_short_name
        if (
            route_name_length is not None
            and self.routes.route_short_name.dtype == "str"
        ):
            self.routes.route_short_name = self.routes.route_short_name.str.slice(
                0, route_name_length
            )

        return None

    def drop_non_increasing_stoptimes(self, exclude_last_stop=True):
        """
        drop trips where max arrival_time == min departure_time,
        exclude_last_stop : boolean, dont take into account last stop
        """

        df = self.stop_times.copy()

        if exclude_last_stop:
            df["max_seq"] = df.groupby("trip_id")["stop_sequence"].transform("max")
            df["stops"] = df.groupby("trip_id")["stop_sequence"].transform("count")
            df = df.loc[(df.stops <= 5) | (df.stop_sequence != df.max_seq)]

        df = df.groupby("trip_id").agg(
            min_dep=("departure_time", min), max_arr=("arrival_time", max)
        )

        if df.shape[0] > 0:
            df = df.loc[df.min_dep == df.max_arr].reset_index()

            self.stop_times = self.stop_times.loc[
                ~self.stop_times.trip_id.isin(df.trip_id)
            ]

            self.prune_ids(step_text="routes with non increasing stop_times")

        return None

    def normalize_trips(self):
        """
        this step must be done after expanding calendars

        conform trip_ids to restricted form :
            - no duplicated unique trip_id in trips and one trip_id for each consecutive
              list of stop_sequence in stop_times
            - regroup all service_ids for one trip_ids to one service_ids, then group identical service_ids
            - convert frequencies to new stop_times (TODO)
            - trip_id are re-build as integers
            - duplicate and regroup service_ids in calendar_dates, service_ids are re-build as integers
        """

        # expand frequencies.txt
        if self.frequencies is not None:
            raise ValueError("Implement frequencies expansion")
        else:
            self.expand_frequencies()

        # renumber stop_times on continuous sequence_id
        self.stop_times["sequence_id"] = self._sequence_ids()

        # replace trip_id in trips
        self.trips = pd.merge(
            self.trips,
            self.stop_times.drop_duplicates(subset=["trip_id"])[
                ["trip_id", "sequence_id"]
            ],
            on="trip_id",
            how="left",
        )

        self.trips = self.trips.drop(columns=["trip_id"]).rename(
            columns={"sequence_id": "trip_id"}
        )

        self.stop_times = self.stop_times.drop(columns=["trip_id"]).rename(
            columns={"sequence_id": "trip_id"}
        )

        # if multiple service_id for one trip_id,
        # duplicate calendar_dates and rebuilt service_id

        return self._rebuild_serviceids()

    def _rebuild_serviceids(self):
        """
        make service_ids and update calendar, calendar_dates and trips,
        only works if calendar are expanded
        service_ids are minimized to share same list of dates and
        can be shared by trips
        services_ids are integer
        """

        # difference with smallest day, for faster calendar comparison
        df = self.calendar_dates
        df["diff"] = (df.date - df.date.min()).dt.days

        # duplicate calendar_dates if multiple service_ids for one trip_id
        cals = pd.merge(
            self.trips[["trip_id", "service_id"]], df, on="service_id", how="outer"
        )

        # group service_id with same list of date differences ints
        cals = cals.sort_values(["trip_id", "diff"])
        cals = pd.merge(
            cals,
            cals.groupby("trip_id").agg(days=("diff", tuple)),
            left_on="trip_id",
            right_index=True,
        )
        cals["new_servid"] = cals.groupby("days").ngroup()

        # copy back new service_id values in calendar_dates
        self.calendar_dates = cals[
            ["new_servid", "date", "exception_type"]
        ].drop_duplicates(["date", "new_servid"])
        self.calendar_dates = self.calendar_dates.rename(
            columns={"new_servid": "service_id"}
        )

        # copy back new service_id values in trips
        self.trips = pd.merge(
            self.trips,
            cals[["trip_id", "service_id", "new_servid"]],
            on=["trip_id", "service_id"],
            how="left",
        )
        self.trips = self.trips.drop(columns="service_id").rename(
            columns={"new_servid": "service_id"}
        )
        self.trips = self.trips.drop_duplicates(["trip_id", "service_id"], keep="first")

        return None

    def expand_frequencies(self):
        """
        transform frequencies.txt to stop_times
        """

        # TODO

        return None

    def _sequence_ids(self):
        """
        create a sequence_id column when bewteen 2 consecutive rows :
            the value of stop_sequence decreases
            or the trip_id changes
        return ids for stop_times
        """

        df_shift = self.stop_times[["stop_sequence", "trip_id"]].shift(1)

        mask_seq = self.stop_times.stop_sequence < df_shift.stop_sequence
        mask_trip = self.stop_times.trip_id != df_shift.trip_id

        df_change = self.stop_times.loc[(mask_seq) | (mask_trip)][["stop_sequence"]]

        # sequence_number
        df_change["_id"] = 1
        df_change["sequence_id"] = df_change["_id"].cumsum()

        return df_change.reindex(index=self.stop_times.index, method="ffill")[
            "sequence_id"
        ]

    def fill_times(self):
        """
        fill departure_time and arrival_time with values
        if neither exists and between 2 stops, interpolate with shape_dist_traveled or mean
        else use existing value
        """

        # one column is not empty
        self.stop_times = self.stop_times.fillna(
            value={
                "arrival_time": self.stop_times.departure_time,
                "departure_time": self.stop_times.arrival_time,
            }
        )

        # arrival and departure are empty

        # first stop, delete one minute
        self.stop_times.loc[
            (self.stop_times.trip_id != self.stop_times.shift(1).trip_id)
            & (self.stop_times.arrival_time.isna()),
            "arrival_time",
        ] = self.stop_times.arrival_time - pd.Timedelta(seconds=60)

        self.stop_times.loc[
            (self.stop_times.trip_id != self.stop_times.shift(1).trip_id)
            & (self.stop_times.departure_time.isna()),
            "departure_time",
        ] = self.stop_times.departure_time - pd.Timedelta(seconds=60)

        # last stop, add one minute
        self.stop_times.loc[
            (self.stop_times.trip_id != self.stop_times.shift(-1).trip_id)
            & (self.stop_times.arrival_time.isna()),
            "arrival_time",
        ] = self.stop_times.arrival_time + pd.Timedelta(seconds=60)

        self.stop_times.loc[
            (self.stop_times.trip_id != self.stop_times.shift(-1).trip_id)
            & (self.stop_times.departure_time.isna()),
            "departure_time",
        ] = self.stop_times.departure_time + pd.Timedelta(seconds=60)

        # middle stop, mean of after and before
        df = (
            self.stop_times.departure_time.shift(1)
            + self.stop_times.departure_time.shift(-1)
        ) / 2
        self.stop_times.loc[
            (self.stop_times.departure_time.isna()) & (df.notna()), "departure_time"
        ] = df

        df = (
            self.stop_times.arrival_time.shift(1)
            + self.stop_times.arrival_time.shift(-1)
        ) / 2
        self.stop_times.loc[
            (self.stop_times.arrival_time.isna()) & (df.notna()), "arrival_time"
        ] = df

        # Last case : multiple continous rows with na arrival_times and departure_times
        # TODO : drop all_trip_ids

        return None

    def _set_default(self, attribute, column, value, fill_value, as_type):
        """
        set defaults of columns of attribute to value, create if column doesn't exists
        attribute : attribute name of self
        column : column name
        value : default value or column name
        as_value : boolean True if value, False if column name
        """

        df = getattr(self, attribute)

        if df is not None:
            if fill_value:
                default = value
            else:
                default = df[value]

            if column not in df:
                df[column] = default
            else:
                df = df.fillna({column: default})

            if as_type is not None:
                df[column] = df[column].astype(as_type)

            setattr(self, attribute, df)

        return None

    def set_defaults(self, defaults=defaults.defaults):
        """
        set defaults values
        defaults is list of (file, column to set, value or other column,
        True = Value False = another column)
        set a random color if route_color is not filled with values
        """

        for f, col, val, fill_value, as_type in defaults:
            self._set_default(f, col, val, fill_value=fill_value, as_type=as_type)

        # set route colors
        if (
            "route_color" not in self.routes.columns
            or self.routes.loc[self.routes.route_color.isna()].shape[0] > 0
        ):
            self.routes["route_color"] = pd.Series(
                [utils.random_color() for x in range(self.routes.shape[0])],
                index=self.routes.index,
            )

        return None

    def drop_bad_coordinates(self, max_speed=None):
        """
        for each stop times, speed to previous stop, minimum time to 1 minute
        max speed by route_type, filter above value for each route_type
        if None, use defaults in defaults
        """

        # drop stops if coordinates == 0 or missing coordinates
        range_lat = self.stops.stop_lat.between(-90, 90)
        range_lon = self.stops.stop_lon.between(-180, 180)
        not_0 = (self.stops.stop_lon != 0) & (self.stops.stop_lat != 0)
        not_na = (~self.stops.stop_lon.isna()) & (~self.stops.stop_lat.isna())

        self.stops = self.stops.loc[(range_lat) & (range_lon) & (not_0)].copy()

        self.prune_ids(step_text="empty, nil or not in range coordinates")

        # check speed

        df = self.stop_times[
            ["arrival_time", "departure_time", "trip_id", "stop_id", "stop_sequence"]
        ].copy()

        df = df.set_geometry(
            self.stops_as_stoptimes()["geometry"], crs=self.stops.geometry.crs
        )
        df["dist"] = spatial.dist_traveled(df, "trip_id", accumulate=False)

        df["time"] = (
            df["departure_time"].iloc[1:].dt.seconds
            - df["arrival_time"].shift(1).iloc[1:].dt.seconds
        )
        df.loc[df.trip_id != df.shift(1).trip_id, "time"] = 0
        df["time"] = df["time"].fillna(60).div(60).clip(lower=1)
        df = df.eval("speed = 60 * dist / (time * 1000)")


        # add route_type to select max_speed
        df_t = self.trips[["trip_id", "route_id"]].copy()
        df_t["route_type"] = self.routes_as_trips()["route_type"]
        df_t = df_t.drop_duplicates(["trip_id", "route_id"])

        df = pd.merge(df, df_t, on="trip_id", how="left")

        df = df.groupby("stop_id").agg(
            speed=("speed", "min"),
            route_type=("route_type", "first"),
        )

        # add maxspeed

        if max_speed is None:
            max_speed = defaults.max_speed

        df = pd.merge(
            df,
            pd.Series(max_speed).to_frame("max_speed"),
            left_on="route_type",
            right_index=True,
            how="left",
        )
                
        # filter when speed above max_speed km/h or
        df = df.loc[(df.speed < df.max_speed)]
        
        # clean columns

        self.stops = self.stops.loc[self.stops.stop_id.isin(df.index)].copy()
        self.prune_ids(step_text="impossible speed")

        return None

    def simple_agency(self, agency_name):
        """
        one agency for the GTFS, set route/agency_id if missing, and set to 1
            if agency_name is None, do not change name
            if agency_name is False, agency_name with most routes
            if agency_name is True, agency name if directory name
        """

        if self.agency.shape[0] > 1 and type(agency_name) == bool and agency_name:

            # count routes per agency and sort by size
            df = self.routes.groupby("agency_id").size()
            df = df.sort_values(ascending=False)
            name = self.agency.agency_name.head(1).to_list()[0]

        elif type(agency_name) == bool:
            name = self.name
        else:
            name = self.name

        # reduce to 1 agency
        self.agency = self.agency.head(1)
        self.agency["agency_name"] = name
        self.agency["agency_id"] = 1
        self.routes["agency_id"] = 1

        if self.fare_attributes is not None:
            self.fare_attributes["agency_id"] = 1

        return None

    def fix_invalid_stopids(self):
        """
        stop_ids and stop_codes may be mismatched
        if more stop_codes are in stop_times than stop_ids, replace stop_id by stop_code
        """

        if "stop_code" in self.stops.columns:

            ids = self.stops.stop_code.astype("str").unique()
            n_stopcode = self.stop_times.loc[self.stop_times.stop_id.isin(ids)].shape[0]

            ids = self.stops.stop_id.unique()
            n_stopid = self.stop_times.loc[self.stop_times.stop_id.isin(ids)].shape[0]

            if n_stopcode > n_stopid:

                self.stops["stop_code"] = self.stops["stop_code"].astype("str")
                self.stops = self.stops.drop(columns="stop_id").rename(
                    columns={"stop_code": "stop_id"}
                )

        return None

    def compress_coordinates(self, decimals):
        """
        compress stops coordinates to decimals, 6 decimals = aprox. 1 meter precision
        """

        self.stops.stop_lat = self.stops.stop_lat.round(decimals)
        self.stops.stop_lon = self.stops.stop_lon.round(decimals)

        if self.shapes is not None:
            self.shapes.shape_pt_lat = self.shapes.shape_pt_lat.round(decimals)
            self.shapes.shape_pt_lon = self.shapes.shape_pt_lon.round(decimals)

        return None

    # -------------------------------------------------------------------------
    # find groups of routes sharing stops

    def _group_routes(self, row):
        """
        return a tuple of route_ids ordered by decreasing trips
        """
        r = list(row["grproutes"]) + [row["route_id_l"]]
        t = list(row["grptrips"]) + [row["trips"]]
        l = list(zip(r, t))
        l.sort(key=itemgetter(1, 0), reverse=True)
        l = [x[0] for x in l]

        return l[0]

    def set_groupid(self, distance=20, share=0.65, nb_share=10, day=1):
        """
        add a group_id to routes by grouping routes sharing many stops
        group_id is the route_id of the route with most trips with a group exists

        arguments:
            - distance : maximum distance between stops of routes, in projected_crs
            - share : float between 0 and 1, percentage of shared stops, or dict route_type:float
            - nb_share : int, minimum number of shared, or dict route_type:int

            one of share or nb_share considered shall be True to group routes

            - day : number between 0 and 6, day to to filter stops

        results : update self by adding a group_id column to routes
        """

        # prepare data
        df = (
            self.trips.groupby("trip_id")
            .agg(
                trips=("trip_id", "count"),
                route_id=("route_id", "first"),
                direction_id=("direction_id", "first"),
            )
            .reset_index()
        )

        df = pd.merge(
            df,
            self.routes[["route_id", "route_type", "route_short_name"]],
            on="route_id",
            how="left",
        )

        df2 = self.stop_times[["stop_id", "trip_id"]].copy()
        df2 = df2.set_geometry(
            self.stops_as_stoptimes()["geometry"], crs=self.stops.geometry.crs
        )

        df = pd.merge(df2, df, on="trip_id", how="left").drop(columns="trip_id")
        df = df.drop_duplicates(["route_id", "direction_id", "stop_id"])

        # count stops by route_id
        stops = df.groupby(["route_id", "direction_id"]).size().to_frame("stops")

        # make pairs by distance
        df = spatial.query_pairs(df, distance=distance, self_pairs=False)
        df = df.loc[df.route_id_l != df.route_id_r]

        # drop duplicated pairs
        df = df.drop_duplicates(
            [
                "route_id_l",
                "direction_id_l",
                "stop_id_l",
                "route_id_r",
                "direction_id_r",
            ]
        )
        df = df.drop_duplicates(
            [
                "route_id_l",
                "direction_id_l",
                "route_id_r",
                "direction_id_r",
                "stop_id_r",
            ]
        )

        grp = df.groupby(
            ["route_id_l", "direction_id_l", "route_id_r", "direction_id_r"]
        ).agg(
            name_l=("route_short_name_l", "first"),
            name_r=("route_short_name_r", "first"),
            trips_l=("trips_l", min),
            trips_r=("trips_r", min),
            route_type_l=("route_type_l", min),
            route_type_r=("route_type_r", min),
        )

        # groups must be of same route_type
        grp = grp.loc[grp.route_type_l == grp.route_type_r]

        grp["shared"] = df.groupby(
            ["route_id_l", "direction_id_l", "route_id_r", "direction_id_r"]
        ).size()

        grp = grp.reset_index()

        # merge number of stops by route and direction
        grp = pd.merge(
            grp,
            stops.reset_index(),
            left_on=["route_id_l", "direction_id_l"],
            right_on=["route_id", "direction_id"],
            how="left",
        ).drop(columns=["route_id", "direction_id"])

        # reverse pair must exist
        grp = pd.merge(
            grp,
            grp[
                [
                    "route_id_r",
                    "direction_id_r",
                    "route_id_l",
                    "direction_id_l",
                    "stops",
                ]
            ],
            left_on=["route_id_l", "direction_id_l", "route_id_r", "direction_id_r"],
            right_on=["route_id_r", "direction_id_r", "route_id_l", "direction_id_l"],
            suffixes=("", "_rev"),
            how="left",
        ).drop(
            columns=[
                "route_id_r_rev",
                "direction_id_r_rev",
                "route_id_l_rev",
                "direction_id_l_rev",
            ]
        )

        # filter by share of stops

        if type(share) is float:
            grp["_share"] = share
        elif type(share) is dict:
            grp["_share"] = grp["route_type_l"].replace(share).fillna(1)
        else:
            raise ValueError("share must be a float or a dict")
        mask1 = grp.shared > grp[["stops", "stops_rev"]].max(axis=1) * grp._share

        # filter by minimum number of common stops
        if type(nb_share) is float:
            mask2 = grp.shared > nb_share
        elif type(nb_share) is dict:
            m = max(nb_share.values())
            mask2 = grp.shared > grp["route_type_l"].replace(nb_share).fillna(m)
        else:
            raise ValueError("nb_share must be a dict or an int")

        grp = grp.loc[mask1 | mask2]
        grp = grp.drop(columns=["_share"])

        # grp may be empty after filtering
        if len(grp) == 0:
            self.routes["group_id"] = self.routes["route_id"]
            self.routes["group_short_name"] = self.routes["route_short_name"]
            return None

        # drop duplicates and keep the one with most trips
        grp = grp.sort_values("trips_l", ascending=False).drop_duplicates(
            ["route_id_l", "route_id_r"]
        )

        # create list of route_id in same group and list of other number of trips
        grp = (
            grp.groupby("route_id_l")
            .agg(
                grproutes=("route_id_r", tuple),
                grptrips=("trips_r", tuple),
                trips=("trips_l", min),
            )
            .reset_index()
        )

        # select route_id with most trips as route_id
        grp = grp.assign(group_id=grp.apply(self._group_routes, axis=1))[
            ["route_id_l", "group_id"]
        ]

        # add route_short name to group_id
        grp = pd.merge(
            grp,
            self.routes[["route_id", "route_short_name"]],
            left_on="group_id",
            right_on="route_id",
            how="left",
        )

        grp = grp.rename(columns={"route_short_name": "group_short_name"})
        grp = grp.drop(columns="route_id")

        # add to self_routes
        df = pd.merge(
            self.routes,
            grp,
            left_on="route_id",
            right_on="route_id_l",
            how="left",
        )
        df = df.drop(columns=["route_id_l"])
        df["group_id"] = df["group_id"].fillna(df["route_id"])
        df["group_short_name"] = df["group_short_name"].fillna(df["route_short_name"])

        if is_numeric_dtype(df["group_short_name"]):
            df["group_short_name"] = df["group_short_name"].astype(int)

        self.routes = df

        return None

    # --------------------------------------------------------------------

    # normalization tests

    def has_unique_uids(self):
        """
        test if some uids are duplicated
        uids : agency_id, route_id, stop_id except trip_ids
        return dict uid:boolean True if unique
        """
        results = {}
        l = [
            ("agency", "agency_id"),
            ("routes", "route_id"),
            ("stops", "stop_id"),
        ]

        for file, idname in l:
            df = getattr(self, file)
            results[idname] = df.loc[df[idname].duplicated(), idname].shape[0] == 0

        return results

    def has_unique_route_names(self):
        """
        check that route_short_names are unique, so that they can be used as ids
        """
        return self.routes.loc[self.routes.route_short_name.duplicated()].shape[0] == 0

    def has_simple_tripids(self):
        """
        True if tripids are unique on duplicated only for different serviceids
        """
        return self._routeids_for_duplicated_tripids().shape[0] == 0

    def has_defaults(self):
        """
        test if all necessary columns exists
        agency_id
        optional columns
        """
        b = {}

        for f, col, val, as_value, as_type in defaults.defaults:
            df = getattr(self, f)
            if df is not None and col not in df.columns:
                b[(f, col)] = False
            elif df is not None:
                b[(f, col)] = df.loc[df[col].isna()].shape[0] == 0

        # route_color
        df = getattr(self, "routes")
        if "route_color" not in df.columns:
            b[("routes", "route_color")] = False
        else:
            b[("routes", "route_color")] = (
                df.loc[df["route_color"].isna()].shape[0] == 0
            )

        return b

    def has_normalized_tripids(self):
        """
        tripids are normalized if they are unique in trips.txt and
        correspond to a unique list of continuous stop_sequences
        and trip_id is an int
        """
        if self.trips.loc[self.trips.trip_id.duplicated()].shape[0] > 0:
            return False
        t1 = self.trips.trip_id.drop_duplicates().sort_values().to_numpy()
        t2 = self.stop_times.trip_id.drop_duplicates().sort_values().to_numpy()

        if t1.shape != t2.shape:
            return False

        return all(t1 == t2)

    def has_filled_times(self):
        """
        True if no arrival_time, departure_time are na
        """
        df = self.stop_times

        return (
            df.loc[(df.arrival_time.isna()) | (df.departure_time.isna())].shape[0] == 0
        )

    def has_valid_coordinates(self):
        """
        if list of bad coordinates in stops is larger than 0
        """
        # TODO
        return True

    def has_no_orphans(self, id=None):
        """
        test if id uid has orphans
        """
        # TODO
        return True

    def has_coherent_shapes(self):
        """
        shape.txt exists, has shape_dist_traveled,
        stop_times has a shape_dist_traveled column not empty
        """

        b1 = (
            (self.shapes is None)
            & ("shape_dist_traveled" not in self.stop_times.columns)
            & ("shape_id" not in self.trips.columns)
        )
        b2 = (self.shapes is not None) & ("shape_id" in self.trips.columns)

        return b1 or b2

    def is_normalized(self, details=False):
        b = {}
        b["valid_coordinates"] = self.has_valid_coordinates()
        b["unique uids"] = all([v for k, v in self.has_unique_uids().items()])
        b["simple trip_ids"] = self.has_simple_tripids()
        b["defaults"] = all([v for k, v in self.has_defaults().items()])
        b["normalised tripids"] = self.has_normalized_tripids()
        b["filled times"] = self.has_filled_times()
        b["no orphans"] = self.has_no_orphans()
        b["shapes"] = self.has_coherent_shapes()
        b["unique_route_names"] = self.has_unique_route_names()

        if details:
            return b
        else:
            return all([v for k, v in b.items()])
