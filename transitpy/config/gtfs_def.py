str_dtype = str  # may be replaced by Arrow string

required_files = {
    "agency.txt": {
        "name": "agency_name",
        "uid": "agency_id",
        "required": {
            "agency_timezone": str_dtype,
            "agency_name": str_dtype,
            "agency_url": str_dtype,
        },
        "optional": {
            "agency_id": str_dtype,
            "agency_lang": str_dtype,
            "agency_phone": str_dtype,
            "agency_fare_url": str_dtype,
            "agency_email": str_dtype,
        },
    },
    "routes.txt": {
        "name": "route_short_name",
        "uid": "route_id",
        "rid": ["agency_id"],
        "required": {"route_id": str_dtype, "route_type": "UInt16"},
        "optional": {
            "agency_id": str_dtype,
            "route_short_name": str_dtype,
            "route_long_name": str_dtype,
            "route_desc": str_dtype,
            "route_url": str_dtype,
            "route_color": str_dtype,
            "route_text_color": str_dtype,
            "route_sort_order": "UInt16",
            "continuous_pickup": "UInt8",
            "continuous_drop_off": "UInt8",
        },
    },
    "trips.txt": {
        "rid": ["trip_id", "route_id", "service_id"],
        "oid": ["shape_id"],
        "required": {"route_id": str_dtype, "service_id": str_dtype, "trip_id": str_dtype},
        "optional": {
            "trip_headsign": str_dtype,
            "trip_short_name": str_dtype,
            "direction_id": "UInt8",
            "block_id": str_dtype,
            "shape_id": str_dtype,
            "wheelchair_accessible": "UInt8",
            "bikes_allowed": "UInt8",
        },
    },
    "stop_times.txt": {
        "rid": ["trip_id", "stop_id"],
        "timedelta": ["departure_time", "arrival_time"],
        "required": {"trip_id": str_dtype, "stop_id": str_dtype, "stop_sequence": "UInt32"},
        "optional": {
            "arrival_time": str_dtype,
            "departure_time": str_dtype,
            "stop_headsign": str_dtype,
            "pickup_type": "UInt8",
            "drop_off_type": "UInt8",
            "continuous_pickup": "UInt8",
            "continuous_drop_off": "UInt8",
            "shape_dist_traveled": float,
            "timepoint": "UInt8",
        },
    },
    "stops.txt": {
        "name": "stop_name",
        "uid": "stop_id",
        "required": {"stop_id": str_dtype, "stop_lat": float, "stop_lon": float},
        "optional": {
            "stop_code": str_dtype,
            "stop_name": str_dtype,
            "tts_stop_name": str_dtype,
            "stop_desc": str_dtype,
            "zone_id": str_dtype,
            "stop_url": str_dtype,
            "location_type": "UInt8",
            "parent_station": str_dtype,
            "stop_timezone": str_dtype,
            "wheelchair_boarding": "UInt8",
            "level_id": "UInt8",
            "platform_code": str_dtype,
        },
    },
}

optional_files = {
    "calendar.txt": {
        "uid": "service_id",
        "dates": ["start_date", "end_date"],
        "required": {
            "service_id": str_dtype,
            "monday": "UInt8",
            "tuesday": "UInt8",
            "wednesday": "UInt8",
            "thursday": "UInt8",
            "friday": "UInt8",
            "saturday": "UInt8",
            "sunday": "UInt8",
            "start_date": int,
            "end_date": int,
        },
    },
    "calendar_dates.txt": {
        "oid": ["service_id"],
        "dates": ["date"],
        "required": {"service_id": str_dtype, "date": int, "exception_type": "UInt8"},
    },
    "fare_attributes.txt": {
        "uid": "fare_id",
        "rid": ["agency_id"],
        "required": {
            "fare_id": str_dtype,
            "price": float,
            "currency_type": str_dtype,
            "payment_method": str_dtype,
            "transfers": str_dtype,
        },
        "optional": {"agency_id": str_dtype, "transfer_duration": "UInt16"},
    },
    "fare_rules.txt": {
        "rid": ["fare_id", "route_id"],
        "required": {"fare_id": str_dtype},
        "optional": {
            "route_id": str_dtype,
            "origin_id": str_dtype,
            "destination_id": str_dtype,
            "contains_id": str_dtype,
        },
    },
    "shapes.txt": {
        "rid": ["shape_id"],
        "geometry": ["shape_pt_lon", "shape_pt_lat"],
        "required": {
            "shape_id": str_dtype,
            "shape_pt_lat": float,
            "shape_pt_lon": float,
            "shape_pt_sequence": "UInt16",
        },
        "optional": {"shape_dist_traveled": float},
    },
    "frequencies.txt": {
        "rid": ["trip_id"],
        "required": {
            "trip_id": str_dtype,
            "start_time": str_dtype,
            "end_time": str_dtype,
            "headway_secs": "UInt32",
        },
        "optional": {"exact_times": str_dtype},
    },
    "transfers.txt": {
        "rid": ["from_stop_id", "to_stop_id"],
        "id_alias": {"stop_id": ["from_stop_id", "to_stop_id"]},
        "required": {
            "from_stop_id": str_dtype,
            "to_stop_id": str_dtype,
            "transfer_type": "UInt8",
        },
        "optional": {"min_transfer_time": "UInt16"},
    },
    "feed_info.txt": {
        "required": {
            "feed_publisher_name": str_dtype,
            "feed_publisher_url": str_dtype,
            "feed_lang": str_dtype,
        },
        "optional": {
            "default_lang": str_dtype,
            "feed_start_date": str_dtype,
            "feed_end_date": str_dtype,
            "feed_version": str_dtype,
            "feed_contact_email": str_dtype,
            "feed_contact_url": str_dtype,
        },
    },
}


# extra files, added on feed creation
extra_files = {"parent_stations.txt": {"uid": "parent_station", "optional": True}}


def gtfs_required_files():
    return required_files


def gtfs_optional_files():
    return optional_files


def gtfs_extra_files():
    return extra_files


def gtfs_all_files():
    return {**{**required_files, **optional_files}, **extra_files}