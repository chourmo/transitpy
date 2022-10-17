# -*- coding: utf-8 -*-

import pandas as pd

from transitpy.spatial import pt_in_bounds


class Filter_functions(object):

    """
    filter feed and return a new feed
    """

    # ------------------------------------------------------------
    # Modal and spatial filtering functions

    def modal_filter(self, modes):
        """
        filter to one or a list of modes

        params :
            modes : a mode number or list of mode numbers, see gtfs specification for details
        """

        fd = self.copy()

        if type(modes) is not list:
            m = [modes]
        else:
            m = modes

        fd.routes = fd.routes.loc[fd.routes.route_type.isin(m)]
        fd.prune_ids(step_text="modal filter")

        return fd

    def spatial_filter(self, limits):
        """
        filter stops by coordinates or polygon
        limits : tuple of mininimum longitude, minimum latitude,
        maximum longitude, maximum latitude or GeoSeries
        """

        fd = self.copy()

        if limits is None:
            return None

        if type(limits) is tuple:
            bounds = limits
        else:
            try:
                l = limits.geom_type.drop_duplicates().to_list()
                if "Polygon" in l or "MultiPolygon" in l:
                    bounds = limits.total_bounds
            except:
                raise ValueError(
                    "Limit but be either a tuple of coordinates or a shapely polygon"
                )

        # copy coordinates in stop_times
        df = fd.stops_as_stoptimes()[["stop_lon", "stop_lat", "geometry"]]

        # find stop_ids in limits
        df = pt_in_bounds(df, bounds)
        df = df[["trip_id", "stop_sequence"]]

        # count stops and valid stops
        df = df.groupby("trip_id").agg(
            trip_id=("trip_id", "first"),
            stops=("stop_sequence", "count"),
        )

        df = df.loc[df.stops >= 2]["trip_id"].drop_duplicates()

        # find corresponding route_ids
        rids = fd.trips.loc[~fd.trips.trip_id.isin(df), "route_id"].drop_duplicates()
        fd.routes = fd.routes.loc[fd.routes.route_id.isin(rids)]

        fd.prune_ids(step_text="spatial filter")

        return fd

    # --------------------------------------------------------------
    # Temporal filtering functions

    def week_filter(self, week=None):
        """
        simplify calendars to a single week, find closest week after date

        Parameters:
            week : tuple (week number, year)
            if week = None or week not in feed, filter to week with most trips
        """

        if self.calendar is not None:
            raise ValueError("week_filter needs feed nomalization")

        fd = self.copy()

        # get list of weeks in feed

        df = fd.valid_weeks().to_frame("trips")

        if week is None or (week[1], week[0]) not in df.index:
            w = df.sort_values(
                by=["trips", "week", "year"], ascending=[False, True, True]
            )
            y, w = w.head(1).index.values[0]
        else:
            w, y = week

        fd.calendar_dates = fd.calendar_dates.loc[
            (fd.calendar_dates.date.dt.isocalendar().week == w)
            & (fd.calendar_dates.date.dt.year == y)
        ]

        sids = fd.calendar_dates.service_id.drop_duplicates()

        fd.prune_ids(step_text="week filter")

        return fd

    def day_filter(self, day=0):
        """
        filter feed to one type of day, must be done after week_filter

        parameters :
            day:day number starting as monday = 0
        """

        fd = self.copy()

        # days of each calendar_dates
        df = fd.unified_calendars(trips=True)

        # filter to specific day, capitalize to match string format
        df = df.loc[df.date.dt.weekday == day]
        if df.shape[0] == 0:
            raise ValueError("No day in feed")

        # keep most used day
        daydate = df.groupby("date")["trips"].sum().sort_index(ascending=False)
        daydate = daydate.head(1).index.values[0]

        df = df.loc[df.date == daydate]
        sids = df.service_id.drop_duplicates()

        fd.calendar_dates = fd.calendar_dates.loc[
            fd.calendar_dates.service_id.isin(sids)
        ]
        fd.trips = fd.trips.loc[fd.trips.service_id.isin(sids)]

        fd.prune_ids(step_text="day filter")

        return fd

    def year_filter(self, year=None):
        """
        limit calendar_dates to one year,
        if year is None, keep the year with most trips, else year must be an integer
        """

        if self.calendar is not None:
            raise ValueError("year_filter needs feed nomalization")

        fd = self.copy()
        df = self.calendar_dates.copy()

        trips = self.trips.drop_duplicates(["trip_id", "service_id"])
        trips = trips.groupby(["service_id"]).size().to_frame("trips")
        df = pd.merge(df, trips, on=["service_id"], how="left")

        y = df.groupby(by=df["date"].dt.year)["trips"].sum()
        
        if year is None:
            y = y.sort_values().index.values[-1]
        else:
            y = year
            
        fd.calendar_dates = df.loc[df.date.dt.year == y].drop(columns=["trips"])

        return fd
