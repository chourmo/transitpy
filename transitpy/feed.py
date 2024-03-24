# -*- coding: utf-8 -*-
import os
import zipfile
from doctest import DocFileCase

import geopandas as gpd
import pandas as pd
from pandas.api.types import is_string_dtype

from transitpy.config.gtfs_def import (gtfs_all_files, gtfs_extra_files,
                                       gtfs_optional_files,
                                       gtfs_required_files)
from transitpy.filters import Filter_functions
from transitpy.normalize import Normalize_functions
from transitpy.shapes import Shapes_functions


def is_gtfs_path(path):
    """ test if path has all necessary files for GTFS """
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path, "r") as zipObj:
            files = set([os.path.basename(x) for x in zipObj.namelist()])
    elif os.path.isdir(path):
        files = set(os.listdir(path))
    else:
        return False

    # test files content to match GTFS specification
    required_files = set(gtfs_required_files().keys())

    if not required_files.issubset(files):
        return False

    # check that calendar or calendar_dates exists
    if "calendar.txt" not in files and "calendar_dates.txt" not in files:
        return False

    return True


class Feed(Normalize_functions, Filter_functions, Shapes_functions
):
    """

    Object storing a GTFS feed with reorganised values to simplify analysis
    each file is in an instance variable, None if optional and not in file

    GTFS specifications is restricted to :
        - all non unique id (agency_id, route_id, stop_id, service_id
        in calendar only, fare_id) values are dropped in
        the feed dropped variable
        - all ids are set to string and kept as original value, except trip_id
        - calendar is always None
        - all valid dates are in calendar_dates, no exception_type = 2
        - all departure_times and arrival_times are set and interpolated if missing
        - for a trip_id, the maximum arrival_time must be larger
          than the minimum of departure_time
        - optional variables are set to default values, and a random color is set routes
        - compress stop latitude and longitude to 6 decimals
        - add a geometry column in stops and shapes, crs = projected_crs
        - group_id : a route_id value common for routes with many common stops

    special attributes :
        - name : name of directory or zip file
        - projected_crs : crs used for projected distance
        - dropped : list of all deleted ids (value in id column,
                    type in type column)
                    and reason (step column)
    """

    def __init__(self, path=None, year=None, crs=4326):
        """
        Import a gtfs zip file, folder or url in a new object with paramaters for
        each file, optionaly set to None

        paramaters :
            path_string of directory path or zip file
            year : filter data to a year, if None filter to the most year with most trips
            used in normalization
            crs : optional projected crs for distance calculation
        """

        # set special attributes not in GTFS definition
        self.dropped = pd.DataFrame(columns=["step", "type", "id", "name"])
        self.projected_crs = crs
        self.name = None

        if path is None:
            self._open_empty()
            return None

        if not os.path.exists(path):
            raise ValueError("{0} doesnt exist".format(path))

        # check path is a valid GTFS file or directory
        if not is_gtfs_path(path):
            raise ValueError("{0} file is not valid GTFS data".format(self.name))

        # set feed name and change path if zip
        is_zip = zipfile.is_zipfile(path)
        if is_zip:
            fname = os.path.splitext(os.path.basename(path))[0]
            self.name = fname
            path = zipfile.ZipFile(path)
        else:
            self.name = os.path.splitext(os.path.basename(path))[0]

        # open files
        for file_name, spec in gtfs_all_files().items():
            df = self._open_file(path, file_name, is_zip, spec)
         
            # set geometries
            if file_name == "stops.txt" and df is not None:
                df = self._stop_geometries(df, crs=crs)
            elif file_name == "shapes.txt" and df is not None:
                df = self._shape_geometries(df, crs=crs)
            
            # store as attribute
            setattr(self, file_name[:-4], df)
                
        # set default value to agency_id
        self.default_agencyid()
       
        # force stop_times order
        self.stop_times = self.stop_times.sort_values(
            ["trip_id", "stop_sequence"], ascending=True
        ).reset_index(drop=True)

        # normalize calendars
        self.merge_calendars(year)

        # replace stops in stations by stations
        self.simplify_stations()

        # drop unused values
        self.prune_ids(step_text="unused")

        self._flat = None

        return None

    def _open_empty(self):
        """ Fill attributes as None"""
        self._flat = None
        for file_name in gtfs_required_files().keys():
            setattr(self, file_name[:-4], None)
        return None

    def _column_dtypes(self, path, spec):

        header = set(pd.read_csv(path, nrows=0))
        dtypes = {k: v for k, v in spec["required"].items()}
        req_set = set(dtypes.keys())

        # check required columns
        if not req_set.issubset(header):
            raise ValueError("Invalid GTFS : required columns are missing from {0}".format(path))

        if "optional" in spec:
            for k, v in spec["optional"].items():
                if k in header:
                    dtypes[k] = v

        return dtypes


    def _stop_geometries(self, stops, crs=None):
        """Remove stop types without geometries, convert to GeoDataframe"""

        # normalize default columns
        if not "location_type" in stops.columns:
            stops["location_type"] = 0
        stops["location_type"] = stops["location_type"].fillna(0)
        if "parent_station" not in stops.columns:
            stops["parent_station"] = "no"

        # keep stops and stations only
        stops = stops.loc[stops.location_type < 2]

        # all coordinates must be set for stops and stations
        if len(stops.loc[stops.stop_lon.isna()]) > 0:
            raise ValueError("Invalid GTFS : All stop coordinates must be set for stops.txt")

        # set geometry
        stops["geometry"] = gpd.points_from_xy(stops["stop_lon"], stops["stop_lat"], crs=4326)
        stops = stops.set_geometry("geometry", crs=4326)

        if crs is not None:
            stops = stops.to_crs(crs)

        return stops


    def _shape_geometries(self, shapes, crs=None):
        """Remove stop types without geometries, convert to GeoDataframe"""

        shapes["geometry"] = gpd.points_from_xy(
            shapes["shape_pt_lon"], shapes["shape_pt_lat"], crs=4326
        )
        shapes = shapes.set_geometry("geometry", crs=4326)
        return shapes


    def _open_file(self, path, file, is_zip, spec):
        """Open a file according to spec and add to self, return None"""

        # check if file exists, otherwise set object attribute to None and return
        if is_zip:
            f_list = {os.path.basename(x): x for x in path.namelist()}
            if file in f_list:
                f = path.open(f_list[file])
                dtypes = self._column_dtypes(f, spec)
                f = path.open(f_list[file])           # reopen file
            else:
                return None

        elif os.path.exists(os.path.join(path, file)):
            f = os.path.join(path, file)
            dtypes = self._column_dtypes(f, spec)
        else:
            return None    
       
        df = pd.read_csv(
            f,
            dtype=dtypes,
            usecols=list(dtypes.keys()),
            encoding="utf-8",
            dtype_backend="pyarrow"
        )

        # convert dates
        for c in spec.get("dates", []):
            df[c] = pd.to_datetime(df[c], format="%Y%m%d")
        
        # fix missing or duplicated uid
        if "uid" in spec:
            if spec["uid"] not in df.columns:
                df[spec["uid"]] = df[spec["name"]].copy()
            df = df.drop_duplicates(subset=spec["uid"])

        # convert timedelta columns
        if "timedelta" in spec:
            for col in spec["timedelta"]:
                df[col] = pd.to_timedelta(df[col])

        return df

    def to_feed(self, path, compress=False, csv_separator=","):
        """Save feed by feed name to path, may compress as zip folder"""

        if compress:

            dest_path = os.path.join(path, self.name + ".zip")

            with zipfile.ZipFile(dest_path, "w") as csv_zip:
                for f, v in gtfs_all_files().items():

                    df = None

                    if hasattr(self, f[:-4]):
                        df = getattr(self, f[:-4])

                    if "geometry" in df.columns:
                        del df["geometry"]

                    if df is not None:
                        csv_zip.writestr(f, df.to_csv(index=False))

            csv_zip.close()

        else:
            dest_path = os.path.join(path, self.name)

            os.mkdir(dest_path)

            for f, v in gtfs_all_files().items():

                df = None

                if hasattr(self, f[:-4]):
                    df = getattr(self, f[:-4])

                if "geometry" in df.columns:
                    del df["geometry"]

                if df is not None:
                    df.to_csv(
                        os.path.join(dest_path, f), index=False, sep=csv_separator
                    )

    def copy(self):
        """
        Return a copy of self
        """
        newfeed = Feed()
        for key in [k for k in vars(self).keys() if k != "self"]:
            value = getattr(self, key)
            if isinstance(value, pd.DataFrame):
                value = value.copy()
            setattr(newfeed, key, value)

        return newfeed

    def add_feed(self, feed):

        # TODO

        return None

    # -----------------------------------------------------------------------------
    # ids coherency

    def _add_dropped(self, ids, type_text, step_text=None):
        """
        add ids to dropped,

        args :
            ids : Series of ids
            type_text : name of id (stop_id...)
            step_text : optional, calculation step name
        """

        if len(ids) == 0:
            return None

        df = ids.to_frame("id")
        df["type"] = type_text

        if step_text is not None:
            df["step"] = step_text

        if type_text == "stop_id":
            names = self.stops.loc[self.stops.stop_id.isin(ids), "stop_name"]
            df.loc[names.index, "name"] = names
        elif type_text == "route_id":
            names = self.routes.loc[self.routes.route_id.isin(ids), "route_short_name"]
            df.loc[names.index, "name"] = names

        df = pd.concat([self.dropped, df], ignore_index=True, sort=False)
        self.dropped = df.reset_index(drop=True)

        return None

    def _prune(self, unique=None, others=None, optionals=None, step_text=None):
        """
        prune ids columns values, drop non unique
        id values must be in both unique and others
        id values may be in optionals
        unique and others can not both be None

        arguments:
            unique : optionnal, tuple of file, id column (stops, stop_id...),
                     values in column must be unique
            others : optionnal, list of tuples (file, id_name)
            optionals : optional, list of tuples (file, id_name)
            drop_text : text to comment in dropped data
        """

        if others is None and unique is None:
            raise ValueError("unique and others must not be both None")
        if unique is not None and unique[0] == "stop_times":
            raise ValueError("stop_times cannot be unique")

        # drop duplicates in unique
        if unique is not None and getattr(self, unique[0]) is not None:

            df = getattr(self, unique[0])

            self._add_dropped(
                ids=df.loc[df.duplicated(), unique[1]],
                type_text=unique[1],
                step_text="duplicated",
            )

            setattr(self, unique[0], df.loc[~df.duplicated(keep=False)])

        # find minimum common set of id_names values
        if others is not None:
            l = [(f, n) for f, n in others if getattr(self, f) is not None]
            ids = [set(getattr(self, f)[n].to_list()) for f, n in l]
        else:
            ids = []

        # add ids in unique
        if unique is not None and getattr(self, unique[0]) is not None:
            ids.append(set(getattr(self, unique[0])[unique[1]].to_list()))

        ids = set.intersection(*ids)
        len_ids = len(ids)

        # drop missing ids in unique, if stop_times, drop full sequence

        if unique is not None and getattr(self, unique[0]) is not None:
            df = getattr(self, unique[0])

            if len_ids != len(df[unique[1]]):

                mask = df[unique[1]].isin(ids)

                self._add_dropped(
                    ids=df.loc[~mask, unique[1]],
                    type_text=unique[1],
                    step_text=step_text,
                )

                setattr(self, unique[0], df.loc[mask])

        # drop missing ids in others or optional, if stop_times, drop full sequence

        if optionals is not None:
            l.extend([(f, n) for f, n in optionals if getattr(self, f) is not None])

        for f, n in l:
            if f == "stop_times":
                df = getattr(self, f)
                i = df.loc[~df[n].isin(ids), "sequence_id"].drop_duplicates()
                if len(i) != 0:
                    setattr(self, f, df.loc[~df.sequence_id.isin(i)])

            else:
                df = getattr(self, f)
                setattr(self, f, df.loc[df[n].isin(ids)])

        return None

    def _min_stop_times_length(self):
        """
        drop stop_times rows if only one stop
        """
        if "sequence_id" not in self.stop_times.columns:
            self.stop_times["sequence_id"] = self._sequence_ids()

        df = self.stop_times.groupby("sequence_id")["sequence_id"].transform("count")

        self._add_dropped(
            ids=self.stop_times.loc[df == 1, "trip_id"].drop_duplicates(),
            type_text="trip_id",
            step_text="trips with one stop",
        )

        self.stop_times = self.stop_times.loc[df > 1].copy()

        return None

    def prune_ids(self, step_text=None):
        """
        prune bad ids :
            missing ids,
            duplicated ids that should be unique
            not used stop_id, service_id or route_id
        all consecutive stops are drops in stop_times if one id is missing
        """

        l = self.all_gtfs_lengths() + 1

        # add sequence_ids to stop_times
        self.stop_times["sequence_id"] = self._sequence_ids()

        while l > self.all_gtfs_lengths():

            l = self.all_gtfs_lengths()

            # stop_times must have at least 2 stops in a sequence
            self._min_stop_times_length()

            # prune each id

            self._prune(
                unique=("stops", "stop_id"),
                others=[("stop_times", "stop_id")],
                optionals=[("transfers", "from_stop_id"), ("transfers", "to_stop_id")],
                step_text=step_text,
            )

            self._prune(
                unique=("agency", "agency_id"),
                others=[("routes", "agency_id")],
                step_text=step_text,
            )

            self._prune(
                unique=("routes", "route_id"),
                others=[("trips", "route_id")],
                optionals=[("fare_rules", "route_id")],
                step_text=step_text,
            )

            self._prune(
                others=[("trips", "service_id"), ("calendar_dates", "service_id")],
                step_text=step_text,
            )

            self._prune(
                others=[("trips", "trip_id"), ("stop_times", "trip_id")],
                step_text=step_text,
            )

        # drop sequence_ids from stop_times
        self.stop_times = self.stop_times.drop(columns=["sequence_id"])

        # prune shape_ids
        if self.shapes is not None:
            self.shapes = self.shapes.loc[self.shapes.shape_id.isin(self.trips.shape_id.drop_duplicates())]

        # invalidate flat cache
        self._flat = None

        return None

    # -------------------------------------------------------------------------
    # minimal normalization of dataframes
    
    def default_agencyid(self):
        """add an agency_id and drop duplicates if missing column"""

        if "agency_id" in self.agency.columns:
            return None

        self.agency["agency_id"] = self.agency["agency_name"].copy()
        return None

    def merge_calendars(self, year):
        """Calendar data is unified in calendar_dates"""
        
        self._drop_invalid_calendar_dates()

        if self.calendar is None:
            return None
        if len(self.calendar) == 0:
            self.calendar = None
            return None

        days = [
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
        ]

        df = self.calendar.copy()
       
        df["repeats"] = df["end_date"] - df["start_date"]
        df["repeats"] = df["repeats"].dt.days + 1

        df = df.reindex(index=df.index.repeat(df["repeats"]))
        service_count = df.groupby("service_id").cumcount()
        service_count = pd.to_timedelta(service_count, unit='day')
        df["date"] = df["start_date"] + service_count
        df["day"] = df["date"].dt.day_name().str.lower()

        # keep only if value is 1
        for d in days:
            df = df.loc[(df.day != d) | (df[d] == 1)]

        df = df[["service_id", "date"]].copy()

        # merge calendar_dates
        if self.calendar_dates is not None:
            df = pd.merge(
                df, self.calendar_dates, on=["service_id", "date"], how="outer"
            )
            df['exception_type'] = df['exception_type'].fillna(1)
            df = df.loc[df.exception_type != 2]

        self.calendar_dates = df.copy()
        self.calendar = None

        # max duration to one_year
        fd = self.year_filter(year)
        self.calendar_dates = fd.calendar_dates.copy()

        return None

    def _drop_invalid_calendar_dates(self):
        """
        drop calendars if start_date > end_date
        """

        if self.calendar is None:
            return None

        df = self.calendar.loc[self.calendar.start_date > self.calendar.end_date]
        if len(df) == 0:
            return None

        self.calendar = self.calendar.loc[
            self.calendar.start_date <= self.calendar.end_date
        ]

        if self.calendar_dates is not None:
            self.calendar_dates = self.calendar_dates.loc[
                ~self.calendar_dates.service_id.isin(df.service_id)
            ]

        return None

    def simplify_stations(self):
        """Replace stops in stations by station, drop entrances, generic nodes and boarding areas"""

        # normalize default columns
        if "location_type" not in self.stops.columns:
            self.stops["location_type"] = 0
        self.stops["location_type"] = self.stops["location_type"].fillna(0)
        if "parent_station" not in self.stops.columns:
            self.stops["parent_station"] = "no"
        elif not is_string_dtype(self.stops.parent_station):
            self.stops.parent_station = self.stops.parent_station.astype("str")

        # keep stops and stations only
        self.stops = self.stops.loc[self.stops.location_type < 2]

        # regroup stations and map stopids in stop_times
        if len(self.stops.loc[self.stops.location_type == 1]) > 0:
            mask = (self.stops.location_type == 0) & (~self.stops.parent_station.isna())
            mapper = self.stops.loc[mask, ["parent_station", "stop_id"]]
            mapper = mapper.set_index("stop_id")["parent_station"].to_dict()

            self.stops = self.stops.loc[~mask].copy()
            self.stop_times["stop_id"] = self._map_id(
                self.stop_times["stop_id"], mapper
            )

            if self.transfers is not None:
                self.transfers["from_stop_id"] = self._map_id(
                    self.transfers["from_stop_id"], mapper
                )
                self.transfers["to_stop_id"] = self._map_id(
                    self.transfers["to_stop_id"], mapper
                )

        return None

    def _map_id(self, df, mapper):

        return df.map(mapper).fillna(df)

    # -------------------------------------------------------------------------
    # cross-dataframe data

    def stops_as_stoptimes(self):
        """
        return a dataframe aligned to stoptimes with stop content
        """

        st_ix = self.stop_times.set_index("stop_id").index
        df = self.stops.copy()
        df = df.set_index("stop_id").reindex(st_ix).reset_index()
        df.index = self.stop_times.index

        return df

    def routes_as_trips(self):
        """
        return routes data aligned on trips
        """

        st_ix = self.trips.set_index("route_id").index
        df = self.routes.set_index("route_id")

        df = pd.DataFrame(
            data=df.reindex(st_ix).values,
            index=self.trips.index.values,
            columns=df.columns,
        )

        return df

    def flat(self):
        """
        returns and cache a flat representation of all services in a week

        returns a geodataframe with columns :
            - all columns in stop_times
            - all ids including direction_id, without service_id
            - from stops : stop_name, stop_lat, stop_lon, wheelchair_boarding
            - from trips : wheelchair_accessible, bikes_allowed
            - from routes : route_short_name, agency_id, route_type
            - from agency : agency_name
            - spacing : distance between shape_dist_traveled
            - time : time from first stop in trip in seconds
            - day : weekday starting from monday = 0
            - first_seq : first stop_sequence value in trip
            - last_seq : last stop_sequence value in trip
            - geometry in projected coordinates
        """

        # return cached flat dataframe
        if self._flat is not None:
            return self._flat

        # limit to one week
        if len(self.valid_weeks()) > 0:
            fd = self.week_filter()
        else:
            fd = self.copy()

        # expand trips by calendar
        df = fd.trips[
            [
                "service_id",
                "trip_id",
                "route_id",
                "direction_id",
                "wheelchair_accessible",
                "bikes_allowed",
                "shape_id",
            ]
        ]

        df = pd.merge(
            df,
            fd.calendar_dates[["service_id", "date"]],
            on="service_id",
            how="outer",
        )

        # add stop data to stop_times and add expand to trips
        df2 = fd.stop_times.copy()
        cols = [
            "stop_id",
            "stop_name",
            "stop_lat",
            "stop_lon",
            "wheelchair_boarding",
            "geometry",
        ]
        df2[cols] = fd.stops_as_stoptimes()[cols]
        df = pd.merge(df, df2, on="trip_id", how="outer")

        # add route data
        l = ["route_id", "route_short_name", "agency_id", "route_type"]
        if "group_id" in fd.routes.columns:
            l.extend(["group_id", "group_short_name"])
        df = pd.merge(df, fd.routes[l], on="route_id", how="left")

        # add agency data
        df = pd.merge(
            df, fd.agency[["agency_name", "agency_id"]], on="agency_id", how="left"
        )

        # spacing
        df["spacing"] = df["shape_dist_traveled"] - df.shift(1)[
            "shape_dist_traveled"
        ].fillna(0)
        df.loc[df.shape_dist_traveled == 0, "spacing"] = 0
        df["spacing"] = df["spacing"].astype(int)

        # first and last stop_sequence
        df["first_seq"] = df.groupby(["trip_id"])["stop_sequence"].transform("min")
        df["last_seq"] = df.groupby(["trip_id"])["stop_sequence"].transform("max")

        # time from first stop in trip
        start = df[["trip_id", "departure_time"]].drop_duplicates("trip_id")
        start = start.rename(columns={"departure_time": "time"})
        df = pd.merge(df, start, on="trip_id", how="left")
        df["time"] = df["arrival_time"] - df["time"]
        df["time"] = df["time"].dt.total_seconds()

        df = df.set_geometry("geometry", crs=self.projected_crs)

        df["day"] = df["date"].dt.dayofweek

        # put in cache
        self._flat = df

        return df

    # --------------------------------------------------------------
    # utilities
    
    def trips_by_date(self):
        """
        returns a DataFrame with service_id and pd.datetime.date of one day as value
        if trips is True, add number of trips in a trips column
        """
        if self.calendar is not None:
            raise ValueError("unified calendars needs feed nomalization")

        df = self.calendar_dates.copy()
        if df is not None and "exception_type" in df.columns:
            del df["exception_type"]
        
        trips = self.trips.drop_duplicates(["trip_id", "service_id"])
        trips = trips.groupby(["service_id"]).size().to_frame("trips")
        df = pd.merge(df, trips, on=["service_id"], how="left")

        return df.drop_duplicates(subset=["service_id", "date"]).reset_index(drop=True)
    
    def valid_weeks(self):
        """return a list (week, year) with service"""

        df = self.trips_by_date()
        df["week"] = df.date.dt.isocalendar().week
        df["year"] = df.date.dt.year
        df = df.groupby(["year", "week"])["trips"].sum()

        return df

    def all_gtfs_lengths(self):
        return sum(
            [
                getattr(self, x[:-4]).shape[0]
                for x in gtfs_all_files().keys()
                if getattr(self, x[:-4]) is not None
            ]
        )

    def description(self):

        description = {}
        for k in gtfs_all_files().keys():
            df = getattr(self, k[:-4])
            if df is not None:
                description[k[:-4]] = df.shape[0]
            else:
                description[k[:-4]] = 0

        return pd.Series(description)
