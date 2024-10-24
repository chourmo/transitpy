# -*- coding: utf-8 -*-
import pandas as pd

from transitpy.utils import format_timedelta, simple_list


def route_stats(feed, group_directions=False, by_hour=True, max_arrival_hour=3):
    """
    Statistics for each route, with optional details by day or by hour

    Args :
        feed: a gtfs feed
        group_directions : boolean, if True one row for each route_id,
        else one row per route_id and direction_id
        by_hour : boolean, if True decompose stats by hour add hour_min and hour_max columns
        max_arrival_hour : if arrival_time is after value next day, set arrival to 0

    Returns :
        a dataframe with one row for route, direction and day,
        optionnaly a pair of hour_min and hour_max columns,
        statistics are in columns :
        - stops : max # of stops
        - departure / arrival : first departure and last arrival
        - length : max shape_dist_traveled on stop_times
        - trips : # of trips on a day
        - speed : speed median
        - wheelchair : max value of wheelchair_accessible value in trips, 0 not set
        - bikes : max value of bike_allowed value in trips, 0 not set
        - agency_name
        - route_name : from route_short_name
        - direction_id
        - mode
    """

    df = feed.flat()

    df["hour"] = df.departure_time.dt.seconds // 3600

    # Pandas 1.0 has no mode aggregation function
    # find most_used hour by trip_id and day
    df_H = df.groupby(["trip_id", "day", "hour"]).size().to_frame("size").reset_index()
    df_H = df_H.sort_values("size", ascending=False).drop_duplicates(["trip_id", "day"])

    # statistics by trip
    df = (
        df.groupby(["trip_id", "day"])
        .agg(
            agency_name=("agency_name", "first"),
            route_id=("route_id", "first"),
            direction_id=("direction_id", "first"),
            route_short_name=("route_short_name", "first"),
            group_name=("group_short_name", "first"),
            route_type=("route_type", "first"),
            stops=("stop_sequence", "count"),
            departure=("departure_time", "min"),
            arrival=("arrival_time", "max"),
            length=("shape_dist_traveled", "max"),
            spacing=("spacing", "median"),
            wheelchair_accessible=("wheelchair_accessible", "first"),
            bikes_allowed=("bikes_allowed", "first"),
        )
        .reset_index()
    )

    df = pd.merge(
        df, df_H[["trip_id", "day", "hour"]], on=["trip_id", "day"], how="left"
    )
    df["time"] = df.arrival - df.departure
    df["time"] = df.time.dt.total_seconds()
    df["speed"] = df.length / df.time * 3.6

    # statistics by route
    if group_directions:
        grp_list = ["route_id", "day"]
    else:
        grp_list = ["route_id", "day", "direction_id"]

    if by_hour:
        grp_list.append("hour")

    # filter arrival next day after 3:00 to
    df.loc[
        (df["arrival"].dt.days > 0)
        & (df["arrival"].dt.seconds > max_arrival_hour * 3600),
        "arrival",
    ] = pd.Timedelta(seconds=0)

    df = (
        df.groupby(grp_list)
        .agg(
            agency_name=("agency_name", "first"),
            group_name=("group_name", "first"),
            route_short_name=("route_short_name", "first"),
            route_types=("route_type", "first"),
            trips=("trip_id", "count"),
            stops=("stops", "max"),
            departure=("departure", "min"),
            arrival=("arrival", "max"),
            length=("length", "sum"),
            spacing=("spacing", "median"),
            wheelchair=("wheelchair_accessible", "max"),
            bikes=("bikes_allowed", "max"),
            time=("time", "median"),
            speed=("speed", "median"),
        )
        .reset_index()
    ).drop(columns=["route_id"])

    # convert values

    df["time"] = df["time"] // 60
    df["time"] = df["time"].astype(int)
    df["spacing"] = df["spacing"].astype(int)
    df["speed"] = df["speed"].astype(int)

    # Mean length by trip
    df["length"] = df["length"] / df["trips"]
    df["length"] = df["length"].astype(int)
    df["wheelchair"] = df["wheelchair"].astype(int)
    df["bikes"] = df["bikes"].astype(int)

    if "direction_id" in df.columns:
        df["direction_id"] = df["direction_id"].astype(int)

    df["departure"] = format_timedelta(df["departure"])
    df["arrival"] = format_timedelta(df["arrival"])

    return df


def stop_stats(feed, max_arrival_hour=3):
    """
    Statistics for each stop

    Args :
        feed: a GTFS feed
        max_arrival_hour : if arrival_time is after value next day, set arrival to 0

    Returns :
        a dataframe with one row for each stop of each day, route and direction,
        statistics are in columns :
        - name : stop_name
        - route_id
        - direction_id
        - route_name : from route_short_name
        - position : stop_sequence
        - departure / arrival : first departure and last arrival
        - trips : # of trips on a day
        - agency_name
        - latitude, longitude
        - stop_sequence
        - wheelchair_boarding
    """

    df = feed.flat()

    # filter arrival next day after 3:00 to
    df["arrival"] = df["arrival_time"].copy()
    df.loc[
        (df["arrival"].dt.days > 0)
        & (df["arrival"].dt.seconds > max_arrival_hour * 3600),
        "arrival",
    ] = pd.Timedelta(seconds=0)

    # statistics by day, stop, route and direction
    df = (
        df.groupby(["route_id", "direction_id", "stop_id", "day"])
        .agg(
            agency_name=("agency_name", "first"),
            route_name=("route_short_name", "first"),
            route_types=("route_type", "first"),
            group_name=("group_short_name", "first"),
            stop_name=("stop_name", "first"),
            position=("stop_sequence", "mean"),
            trips=("trip_id", "count"),
            departure=("departure_time", "min"),
            arrival=("arrival", "max"),
            wheelchair=("wheelchair_boarding", "max"),
            bikes=("bikes_allowed", "max"),
            longitude=("stop_lon", "first"),
            latitude=("stop_lat", "first"),
        )
        .reset_index()
    ).drop(columns=["route_id", "stop_id"])

    # convert values

    df["position"] = df["position"].round(1).astype(int)
    df["wheelchair"] = df["wheelchair"].astype(int)
    df["bikes"] = df["bikes"].astype(int)

    df["departure"] = format_timedelta(df["departure"])
    df["arrival"] = format_timedelta(df["arrival"])

    if "direction_id" in df.columns:
        df["direction_id"] = df["direction_id"].astype(int)

    return df


def transfer_route_stats(transfers, max_text_length=200):
    """
    generate transfer statistics from a transfer file by route and hour

    generate transfer statistics from a transfer file
    max_reverse_wait : maximum alternative wait time before arrival time
    """

    df = transfers.copy()

    # hour
    df["hour"] = df["time"].dt.components.hours

    # wait time less than 10 and 20 minutes
    df["transf_10"] = df.wait <= 10
    df["transf_20"] = (df.wait > 10) & (df.wait <= 20)

    # convert to int to sum
    df["transf_10"] = df["transf_10"].astype(int)
    df["transf_20"] = df["transf_20"].astype(int)

    stats = df.groupby(["agency_name_l", "route_short_name_l", "hour"]).agg(
        agency_nb_out=("agency_name_r", "nunique"),
        agencies_out=("agency_name_r", lambda x: simple_list(x, max_text_length)),
        group_name_out=(
            "group_short_name_l",
            lambda x: simple_list(x, max_text_length),
        ),
        route_nb_out=("route_id_r", "nunique"),
        routes_out=(
            "route_short_name_r",
            lambda x: simple_list(x, max_text_length),
        ),
        wait_out=("wait", "median"),
        transf_10_out=("transf_10", 'sum'),
        transf_20_out=("transf_20", 'sum'),
        transfers_out=("trip_id_l", "size"),
    )

    stats.index = stats.index.set_names(["agency_name", "route_short_name", "hour"])

    # incoming statistics, from right to left

    stats_in = df.groupby(["agency_name_r", "route_short_name_r", "hour"]).agg(
        agency_nb_in=("agency_name_l", "nunique"),
        agencies_in=("agency_name_l", lambda x: simple_list(x, max_text_length)),
        group_name_in=(
            "group_short_name_l",
            lambda x: simple_list(x, max_text_length),
        ),
        route_nb_in=("route_id_l", "nunique"),
        routes_in=(
            "route_short_name_l",
            lambda x: simple_list(x, max_text_length),
        ),
        wait_in=("wait", "median"),
        transf_10_in=("transf_10", 'sum'),
        transf_20_in=("transf_20", 'sum'),
        transfers_in=("trip_id_r", "size"),
    )

    stats_in.index = stats_in.index.set_names(
        ["agency_name", "route_short_name", "hour"]
    )

    stats = pd.merge(stats, stats_in, left_index=True, right_index=True, how="outer")

    # simplify agency, agency_nb, routes, route_nb, take out by default
    stats.loc[
        (stats.agency_nb_out.isna()) | (stats.agency_nb_out < stats.agency_nb_in),
        "agency_nb_out",
    ] = stats["agency_nb_in"]

    stats.loc[
        (stats.agencies_out.isna())
        | (stats.agencies_out.str.len() < stats.agencies_in.str.len()),
        "agencies_out",
    ] = stats["agencies_in"]

    stats.loc[
        (stats.route_nb_out.isna()) | (stats.route_nb_out < stats.route_nb_in),
        "route_nb_out",
    ] = stats["route_nb_in"]
    stats.loc[
        (stats.routes_out.isna())
        | (stats.routes_out.str.len() < stats.routes_in.str.len()),
        "routes_out",
    ] = stats["routes_in"]

    stats.loc[
        (stats.group_name_out.isna())
        | (stats.group_name_out.str.len() < stats.group_name_in.str.len()),
        "group_name_out",
    ] = stats["group_name_in"]

    stats = stats.rename(
        columns={
            "agency_nb_out": "agency_nb",
            "agencies_out": "agencies",
            "route_nb_out": "route_nb",
            "routes_out": "routes",
            "group_name_out": "group_name",
        }
    )
    fillcols = {"wait_in":0,
                "wait_out":0 ,
                "route_nb_in":0,
                "route_nb":0,
                "transfers_in":0,
                "transfers_out":0,
                "transf_10_out":0,
                "transf_20_out":0,
                "transf_10_in":0,
                "transf_20_in":0,
                "agency_nb_in":0,
                "routye_nb_in":0}
    stats = stats.fillna(fillcols)

    stats["transfers"] = stats["transfers_in"] + stats["transfers_out"]
    stats["wait"] = (stats["wait_in"] + stats["wait_out"]) / 2
    stats["transf_10"] = stats["transf_10_in"] + stats["transf_10_out"]
    stats["transf_20"] = stats["transf_20_in"] + stats["transf_20_out"]

    stats = stats.drop(
        columns=[
            "agency_nb_in",
            "agencies_in",
            "route_nb_in",
            "routes_in",
            "group_name_in",
            "transfers_in",
            "transfers_out",
            "wait_in",
            "wait_out",
            "transf_10_in",
            "transf_10_out",
            "transf_20_in",
            "transf_20_out",
        ]
    )

    cols = ["agency_nb", "route_nb", "transfers", "wait", "transf_10", "transf_20"]
    stats[cols] = stats[cols].astype(int)

    return stats.reset_index()


def td_from_str(t):
    """
    return a python timedelta from a string of time H:M
    """

    from datetime import datetime, timedelta

    t = datetime.strptime(t, "%H:%M")
    return timedelta(hours=t.hour, minutes=t.minute)


def transfer_stop_stats(
    transfers,
    min_HPM="06:30",
    max_HPM="8:30",
    min_HPS="16:30",
    max_HPS="18:00",
):
    """
    generate transfer statistics from a transfer file
    """

    df = transfers.copy()

    # time in peakhour

    df.loc[
        ((df.time >= td_from_str(min_HPM)) & (df.time <= td_from_str(max_HPM)))
        | ((df.time >= td_from_str(min_HPS)) & (df.time <= td_from_str(max_HPS))),
        "wait_hp",
    ] = df["wait"]

    # wait time less than 10 and 20 minutes
    df["transf_10"] = df.wait <= 10
    df["transf_20"] = (df.wait > 10) & (df.wait <= 20)

    # convert to int to sum
    df["transf_10"] = df["transf_10"].astype(int)
    df["transf_20"] = df["transf_20"].astype(int)

    # outgoing statistics, from left to right
    stats = df.sort_values("wait", ascending=True).drop_duplicates(
        [
            "agency_name_l",
            "route_short_name_l",
            "stop_name_l",
            "direction_id_l",
            "trip_id_l",
            "route_short_name_r",
        ]
    )

    stats = stats.groupby(
        [
            "agency_name_l",
            "route_short_name_l",
            "stop_name_l",
            "direction_id_l",
            "agency_name_r",
            "route_short_name_r",
        ]
    ).agg(
        wait_out=("wait", "median"),
        wait_hp_out=("wait_hp", "median"),
        transf_10_out=("transf_10", 'sum'),
        transf_20_out=("transf_20", 'sum'),
        transfers_out=("trip_id_l", "size"),
    )

    stats.index = stats.index.set_names(
        [
            "agency_name",
            "route_short_name",
            "stop_name",
            "direction_id",
            "agency_corr",
            "route_short_name_corr",
        ]
    )

    # incoming statistics, from right to left
    stats_in = df.sort_values("wait", ascending=True).drop_duplicates(
        [
            "agency_name_r",
            "route_short_name_r",
            "stop_name_r",
            "direction_id_r",
            "trip_id_r",
            "route_short_name_l",
        ]
    )

    stats_in = stats_in.groupby(
        [
            "agency_name_r",
            "route_short_name_r",
            "stop_name_r",
            "direction_id_r",
            "agency_name_l",
            "route_short_name_l",
        ]
    ).agg(
        wait_in=("wait", "median"),
        wait_hp_in=("wait_hp", "median"),
        transf_10_in=("transf_10", 'sum'),
        transf_20_in=("transf_20", 'sum'),
        transfers_in=("trip_id_r", "size"),
    )

    stats_in.index = stats_in.index.set_names(
        [
            "agency_name",
            "route_short_name",
            "stop_name",
            "direction_id",
            "agency_corr",
            "route_short_name_corr",
        ]
    )

    stats = pd.merge(stats, stats_in, left_index=True, right_index=True, how="outer")

    # simplify agency, agency_nb, routes, route_nb, take out by default
    stats["transf_10"] = stats["transf_10_in"] + stats["transf_10_out"]
    stats["transf_20"] = stats["transf_20_in"] + stats["transf_20_out"]

    # drop or rename columns

    stats = stats.drop(
        columns=[
            "transf_10_in",
            "transf_10_out",
            "transf_20_in",
            "transf_20_out",
        ]
    )

    stats = stats.fillna(0).reset_index()

    # convert to ints
    cols = [
        "transfers_in",
        "transfers_out",
        "wait_in",
        "wait_out",
        "wait_hp_in",
        "wait_hp_out",
        "transf_10",
        "transf_20",
        "direction_id",
    ]
    stats[cols] = stats[cols].astype(int)

    return stats
