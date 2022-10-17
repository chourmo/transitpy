# -*- coding: utf-8 -*-
from datetime import timedelta

import pandas as pd

from . import spatial, utils


def find_transfers(
    pairs,
    pairs_time,
    trips,
    trips_time,
    max_wait,
    pair_ids=["route_u", "direction_id", "stop_u"],
    min_transfer="min_transfer",
    trip_ids=["trip_u", "direction_id", "stop_u"],
    seq_cols=("first_seq", "last_seq"),
    suffixes=("_l", "_r"),
    range_cols=("start", "end"),
    left_to_right=True,
    wait="wait",
    rev_wait=None,
):
    """
     find best transfer for pairs
     pairs : dataframe of pairs of stops to find transfer for
     trips : trips data to find best transfer from
     pair_ids : unique ids to filter on

    wait and optional reverse are converted to minutes, reverse wait minus the waiting_time
    """

    if left_to_right:
        source, target = suffixes
        seq_l, seq_r = seq_cols
        direction = "forward"
        rev_direction = "backward"
    else:
        target, source = suffixes
        seq_r, seq_l = seq_cols
        direction = "backward"
        rev_direction = "forward"

    trip_cols = list(set(pair_ids + [pairs_time] + trip_ids))
    res = pd.merge(
        trips.loc[trips.stop_sequence != trips[seq_l]][trip_cols].add_suffix(source),
        pairs,
        on=[x + source for x in pair_ids],
        how="inner",
    )

    res["_transfer_time"] = res[pairs_time + source] + res[min_transfer]

    # filter out of range transfers
    mask1 = (
        res[pairs_time + source] + timedelta(minutes=max_wait)
        >= res[range_cols[0] + target]
    )
    mask2 = res[pairs_time + source] <= res[range_cols[1] + target]
    res = res.loc[(mask1) & (mask2)]

    # make trips columns same type as corresponding pairs type
    # because merge_asof cant merge between different dtypes
    for i in pair_ids:
        res[i + target] = res[i + target].astype(trips[i].dtype)
    res["_transfer_time"] = res["_transfer_time"].astype(trips[trips_time].dtype)

    # select needed columns and sort
    trip_cols = list(set(pair_ids + [trips_time] + trip_ids))
    df = trips.loc[trips.stop_sequence != trips[seq_r]][trip_cols].add_suffix(target)
    df = df.sort_values(by=trips_time + target, ascending=True)
    res = res.sort_values(by="_transfer_time", ascending=True)

    res = pd.merge_asof(
        res,
        df,
        by=[x + target for x in pair_ids],
        left_on="_transfer_time",
        right_on=trips_time + target,
        direction=direction,
    ).dropna()

    res[wait] = res[trips_time + target] - res[pairs_time + source]
    res[wait] = res[wait].dt.total_seconds().floordiv(60).astype("Int64")

    remove_cols = ["_transfer_time", min_transfer, trips_time + target]

    if rev_wait is not None:
        tripid = [x for x in trip_ids if x not in set(pair_ids)][0]

        res = pd.merge_asof(
            res,
            df.rename(
                columns={
                    tripid + target: tripid + "_rev",
                    trips_time + target: trips_time + "_rev",
                }
            ),
            by=[x + target for x in pair_ids],
            left_on="_transfer_time",
            right_on=trips_time + "_rev",
            direction=rev_direction,
        )
        res[rev_wait] = (
            res[trips_time + "_rev"] - res[pairs_time + source] - res[min_transfer]
        ) / 60
        res[rev_wait] = res[rev_wait].dt.total_seconds().floordiv(60).astype("Int64")

        remove_cols.extend([tripid + "_rev", trips_time + "_rev"])

    # filter first left trip to each right_trip
    dup = [x + source for x in pair_ids] + [x + target for x in trip_ids]
    res = res.sort_values(wait, ascending=True).drop_duplicates(dup)
    res = res.rename(columns={pairs_time + source: "time"})

    return res.drop(columns=remove_cols)


def make_transfers(
    feed,
    max_distances,
    min_transfers=2,
    walk_speed=0.016667,
    max_wait=60,
    filter_groups=True,
    filter_agencies=False,
    reverse_transfers=False,
):
    """
    make a transfers dataframe from a dataframe from a gtfs feed

    params :
        max_distances : max distance for transfer in projection value or
              dict of route_type:distance, maximum distance of left and right route_types is used
        min_transfers : minimal transfer duration in minutes or
              dict of route_type:minutes, maximum transfer of left and right route_types is used
        max_wait : maximum time for transfer in minutes
        filter_groups : boolean, drop transfers between routes of same group of routes
        filter agencies : boolean, drop transfers between same agency
        reverse_wait : boolean to calculate wait time in backward direction,
        add a column with same name in results

    returns a dataframe with columns :
    """

    df = feed.flat()[
        [
            "agency_name",
            "route_id",
            "group_id",
            "direction_id",
            "trip_id",
            "stop_id",
            "route_short_name",
            "group_short_name",
            "route_type",
            "departure_time",
            "arrival_time",
            "first_seq",
            "last_seq",
            "geometry",
            "stop_sequence",
            "stop_name",
        ]
    ]

    # convert ids to integers to speed calculation and memory use
    # cache values and drop from original dataframe
    df, trid = utils.integer_id(
        df,
        "trip_id",
        "trip_u",
        "agency_name",
    )
    df, rid = utils.integer_id(
        df, "route_id", "route_u", "agency_name", keep_cols=["route_short_name"]
    )
    df, sid = utils.integer_id(
        df, "stop_id", "stop_u", "agency_name", keep_cols=["stop_name"]
    )

    stops = df.groupby(["stop_u", "route_u", "direction_id"]).agg(
        start=("departure_time", min),
        end=("arrival_time", max),
        geometry=("geometry", "first"),
        agency_name=("agency_name", "first"),
        route_type=("route_type", "first"),
        group_id=("group_id", "first"),
        group_short_name=("group_short_name", "first"),
    )
    stops = stops.set_geometry('geometry', crs=feed.projected_crs)
    stops = stops.reset_index()

    # add distance by route_type to stops
    if type(max_distances) is dict:
        stops = pd.merge(
            stops,
            pd.Series(data=max_distances).to_frame("max_distance"),
            left_on="route_type",
            right_index=True,
            how="left",
        )
        stops["max_distance"] = stops["max_distance"].fillna(
            max(max_distances.values())
        )
    else:
        stops["max_distance"] = max_distances

    # add minimum transfer by route_type to stops
    if type(min_transfers) is dict:
        m = pd.Series(data=min_transfers)
        m = pd.to_timedelta(m, unit="minutes")
        stops = pd.merge(
            stops,
            m.to_frame("min_transfer"),
            left_on="route_type",
            right_index=True,
            how="left",
        )
        stops["min_transfer"] = stops["min_transfer"].fillna(
            (pd.Timedelta(seconds=min(min_transfers.values())))
        )
    else:
        stops["min_transfer"] = timedelta(minutes=min_transfers)

    # find pairs by maximum distance
    dist = stops["max_distance"].max()
    pairs = spatial.query_pairs(stops, distance=dist, self_pairs=False)

    # filter maximum distance
    pairs["max_distance"] = pairs[["max_distance_l", "max_distance_r"]].max(axis=1)
    
    pairs = pairs.loc[pairs['distance'] <= pairs['max_distance']]

    # find minimum transfer time depending on route_types and distance and walk_speed
    pairs["min_transfer"] = pairs[["min_transfer_l", "min_transfer_r"]].max(axis=1)
    pairs["distance"] = pd.to_timedelta(pairs["distance"] * walk_speed, unit="minutes")
    pairs["min_transfer"] = pairs[["min_transfer", "distance"]].max(axis=1)

    # filter pairs on conditions
    if filter_agencies:
        pairs = pairs.loc[pairs.agency_name_l != pairs.agency_name_r]

    if filter_groups:
        pairs = pairs.loc[pairs.group_id_l != pairs.group_id_r]
    else:
        pairs = pairs.loc[pairs.route_u_l != pairs.route_u_r]

    # filter start end intervals
    pairs = pairs.loc[pairs.end_l + timedelta(minutes=max_wait) >= pairs.start_r]
    pairs = pairs.loc[pairs.start_l - timedelta(minutes=max_wait) <= pairs.end_r]

    pairs = pairs.sort_values("distance", ascending=True)

    pairs = pairs.drop(
        columns=[
            "group_id_l",
            "group_id_r",
            "agency_name_l",
            "agency_name_r",
            "min_transfer_l",
            "min_transfer_r",
            "max_distance_l",
            "max_distance_r",
            "max_distance",
            "distance",
            "route_type_l",
            "route_type_r",
        ]
    )

    if reverse_transfers:
        rev_wait = "reverse_wait"
    else:
        rev_wait = None

    # -------------------------------------------------------------
    # find transfers from left to right
    # keep only first transfer from one left route to each right trip

    df_l2r = pairs.drop_duplicates(
        ["stop_u_l", "route_u_l", "direction_id_l", "route_u_r", "direction_id_r"]
    )

    df_l2r = find_transfers(
        pairs=df_l2r,
        trips=df,
        max_wait=max_wait,
        pairs_time="arrival_time",
        trips_time="departure_time",
        left_to_right=True,
        rev_wait=rev_wait,
    )

    res = df_l2r.loc[df_l2r.wait < max_wait]

    # -----------------------------------------------------------
    # format results

    # import back original values
    res = pd.merge(
        res, trid.add_suffix("_l"), left_on="trip_u_l", right_index=True, how="left"
    )
    res = pd.merge(
        res, trid.add_suffix("_r"), left_on="trip_u_r", right_index=True, how="left"
    )

    rid = rid.drop(columns=["agency_name"])
    res = pd.merge(
        res, rid.add_suffix("_l"), left_on="route_u_l", right_index=True, how="left"
    )
    res = pd.merge(
        res, rid.add_suffix("_r"), left_on="route_u_r", right_index=True, how="left"
    )

    sid = sid.drop(columns=["agency_name"])
    res = pd.merge(
        res, sid.add_suffix("_l"), left_on="stop_u_l", right_index=True, how="left"
    )
    res = pd.merge(
        res, sid.add_suffix("_r"), left_on="stop_u_r", right_index=True, how="left"
    )

    res = res.drop(
        columns=[
            "start_l",
            "end_l",
            "start_r",
            "end_r",
            "route_u_l",
            "route_u_r",
            "trip_u_l",
            "trip_u_r",
            "stop_u_l",
            "stop_u_r",
        ]
    )

    return res



