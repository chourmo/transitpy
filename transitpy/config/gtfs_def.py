# name : name if specified
# uid : name of unique identifier
# uid_default : if uid is missing, add with default value
# rid : list of reference identifiers, if not used, drop corresponding uid
# oid : list of optional identifiers, may not be set
# id_alias : dict {column: uid}
# dates : columns to be imported as dates
# timedelta : columns to be imported as timedelta
# optional : boolean
# geometry : create a geometry with columns in list, first as x, second as y
# parent_station is a special file extracted from stops
# requ_cols : list of required columns
# opt_cols : list of possible columns

base = {
    "agency.txt": {
        "name": "agency_name",
        "uid": "agency_id",
        "optional": False,
        "uid_default": "1",
        "requ_cols": ["agency_timezone", "agency_name", "agency_url"],
        "opt_cols": [
            "agency_id",
            "agency_lang",
            "agency_phone",
            "agency_fare_url",
            "agency_email",
        ],
    },
    "routes.txt": {
        "name": "route_short_name",
        "uid": "route_id",
        "rid": ["agency_id"],
        "optional": False,
        "requ_cols": ["route_id", "route_type"],
        "opt_cols": [
            "agency_id",
            "route_short_name",
            "route_long_name",
            "route_desc",
            "route_url",
            "route_color",
            "route_text_color",
            "route_sort_order",
            "continuous_pickup",
            "continuous_drop_off",
        ],
    },
    "trips.txt": {
        "rid": ["trip_id", "route_id", "service_id"],
        "oid": ["shape_id"],
        "optional": False,
        "requ_cols": ["route_id", "service_id", "trip_id"],
        "opt_cols": [
            "trip_headsign",
            "trip_short_name",
            "direction_id",
            "block_id",
            "shape_id",
            "wheelchair_accessible",
            "bikes_allowed",
        ],
    },
    "stop_times.txt": {
        "rid": ["trip_id", "stop_id"],
        "timedelta": ["departure_time", "arrival_time"],
        "optional": False,
        "requ_cols": ["trip_id", "stop_id", "stop_sequence"],
        "opt_cols": [
            "arrival_time",
            "departure_time",
            "stop_headsign",
            "pickup_type",
            "drop_off_type",
            "continuous_pickup",
            "continuous_drop_off",
            "shape_dist_traveled",
            "timepoint",
        ],
    },
    "stops.txt": {
        "name": "stop_name",
        "uid": "stop_id",
        "optional": False,
        "geometry": ["stop_lon", "stop_lat"],
        "requ_cols": ["stop_id"],
        "opt_cols": [
            "stop_code",
            "stop_name",
            "tts_stop_name",
            "stop_desc",
            "stop_lat",
            "stop_lon",
            "zone_id",
            "stop_url",
            "location_type",
            "parent_station",
            "stop_timezone",
            "wheelchair_boarding",
            "level_id",
            "platform_code",
        ],
    },
    "calendar.txt": {
        "uid": "service_id",
        "dates": ["start_date", "end_date"],
        "optional": True,
        "requ_cols": [
            "service_id",
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
            "start_date",
            "end_date",
        ],
    },
    "calendar_dates.txt": {
        "oid": ["service_id"],
        "dates": ["date"],
        "optional": True,
        "requ_cols": ["service_id", "date", "exception_type"],
    },
    "fare_attributes.txt": {
        "uid": "fare_id",
        "rid": ["agency_id"],
        "optional": True,
        "requ_cols": [
            "fare_id",
            "price",
            "currency_type",
            "payment_method",
            "transfers",
        ],
        "opt_cols": ["agency_id", "transfer_duration"],
    },
    "fare_rules.txt": {
        "rid": ["fare_id", "route_id"],
        "optional": True,
        "requ_cols": ["fare_id"],
        "opt_cols": ["route_id", "origin_id", "destination_id", "contains_id"],
    },
    "shapes.txt": {
        "rid": ["shape_id"],
        "optional": True,
        "geometry": ["shape_pt_lon", "shape_pt_lat"],
        "requ_cols": ["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"],
        "opt_cols": ["shape_dist_traveled"],
    },
    "frequencies.txt": {
        "rid": ["trip_id"],
        "optional": True,
        "requ_cols": ["trip_id", "start_time", "end_time", "headway_secs"],
        "opt_cols": ["exact_times"],
    },
    "transfers.txt": {
        "rid": ["from_stop_id", "to_stop_id"],
        "id_alias": {"stop_id": ["from_stop_id", "to_stop_id"]},
        "optional": True,
        "requ_cols": ["from_stop_id", "to_stop_id", "transfer_type"],
        "opt_cols": ["min_transfer_time"],
    },
    "feed_info.txt": {
        "optional": True,
        "requ_cols": ["feed_publisher_name", "feed_publisher_url", "feed_lang"],
        "opt_cols": [
            "default_lang",
            "feed_start_date",
            "feed_end_date",
            "feed_version",
            "feed_contact_email",
            "feed_contact_url",
        ],
    },
}


# extra files, added on feed creation
extra = {"parent_stations.txt": {"uid": "parent_station", "optional": True}}

full = {**base, **extra}
