# -*- coding: utf-8 -*-

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely as sh
import scipy.spatial as sp

from transitpy.utils import format_timedelta

# ------------------------------------------------------------------------------
# spatial functions

def feed_geometries(feed):
    shapes = feed.trips.drop_duplicates('shape_id')[['route_id', 'shape_id', "direction_id"]]
    shape_geom = feed.shape_geometries().to_frame('geometry')
    shape_geom['length'] = shape_geom.geometry.length

    shapes = pd.merge(shape_geom, shapes, left_index=True, right_on='shape_id', how='left')

    shapes = pd.merge(shapes, feed.routes[['route_id', 'agency_id', 'route_type']], on='route_id', how='left')

    shapes = pd.merge(shapes, feed.agency[['agency_name', 'agency_id']], on='agency_id', how='left')
    shapes = shapes.drop(columns=['agency_id', 'route_id']).set_index('shape_id')

    return shapes

def linemerge(gdf):
    geom = sh.multilinestrings(gdf.array, indices=gdf.index.to_numpy())
    geom = sh.line_merge(geom)
    index = gdf.index.drop_duplicates()
    return gpd.GeoSeries(data=geom, index=index, crs=gdf.crs)


def pt_in_bounds(points, bounds, buffer=0):
    """
    return a boolean series if a geoseries is within bounds of limits geometry
    bounds is a tuple of latmin, lonmin, latmax, lonmax
    """

    xmin, ymin, xmax, ymax = bounds
    xmin = xmin - buffer
    ymin = ymin - buffer
    xmax = xmax + buffer
    ymax = ymax + buffer

    return points.cx[xmin:xmax, ymin:ymax]


def linestring_coordinates(geometry, reindex=False):
    """
    Returns a dataframe of coordinates
    if reindex, index are integers starting at 0
    else, index is same as geometry index
    """

    coords, coords_index = sh.get_coordinates(geometry.array, return_index=True)
    pts = pd.DataFrame(data=coords, index=coords_index, columns=["x", "y"])

    if not reindex:
        mapper = geometry.index.to_series().reset_index(drop=True)
        pts.index = pts.index.map(mapper)

    return pts

def _shape_linestrings(x, y, indices):
    
    # indices mays not be valid
    valid_indices = _simple_ix(indices)
    
    return sh.linestrings(coords=x, y=y, indices=valid_indices)


def dist_traveled(geom, group, accumulate=False, as_integer=True):
    """
    return distance with previous row, set to 0 when value in group changes
    geom is a GeoSeries
    if accumulate is True, sum distances in same group
    if projected, the coordinates are in a projected crs,
    else consider as WGS84 coordinate space

    returns a Series with distance
    """
    g = geom.geometry.name
    df = geom.copy()

    shifted = _geo_shift(df, 1)

    df["_distance"] = df.iloc[1:][g].distance(shifted[g])

    df.loc[(df[group] != df[group].shift(1)), "_distance"] = 0
    if as_integer:
        df["_distance"] = df["_distance"].round(0).astype(int)

    if accumulate:
        df["_distance"] = df.groupby(group)["_distance"].transform("cumsum")

    return df["_distance"]


def query_pairs(
    points, distance, self_pairs=True, left_suffix="_l", right_suffix="_r", line=False
):
    """
    Create a GeoDataframe of pairs between points geometries

    Args :
        points : geodataframe, no interesting value in index
        distance : max_distance to make pairs
        self_pairs : add same point pairs
        left_suffix, right_suffix : suffix to differentiate from and to points
        line : if True, add a linestring column from left to right

    ---------
    Res : geodataframe of pairs of points, geomtry is left point if line is False else geometry is line
    """

    if len(points.loc[points.geometry.isna()]) > 0:
        raise ValueError("points must not contain empty geometries")

    # index as integer values
    df = points.reset_index(drop=True).copy()
    coords = points.geometry.x.to_frame("x")
    coords["y"] = points.geometry.y

    # paires avec index spatial
    tr = sp.cKDTree(coords.to_numpy())

    ix_l = "_ix" + left_suffix
    ix_r = "_ix" + right_suffix
    geom_l = points.geometry.name + left_suffix
    geom_r = points.geometry.name + right_suffix

    pairs = pd.DataFrame(
        data=tr.query_pairs(r=distance, output_type="ndarray"),
        columns=[ix_l, ix_r],
    )

    # result are from smaller id to bigger id
    # duplicate in other direction
    pairs = pd.concat(
        [pairs, pairs.rename(columns={ix_l: ix_r, ix_r: ix_l})],
        ignore_index=True,
        sort=False,
    )

    # add self pairs
    if self_pairs:
        self_pairs = pd.DataFrame(data={ix_l: df.index.values, ix_r: df.index.values})
        pairs = pd.concat([pairs, self_pairs], ignore_index=True, sort=False)

    # valeurs originales
    pairs = pairs.merge(
        df.add_suffix(left_suffix), left_on=ix_l, right_index=True, how="left"
    )

    pairs = pairs.merge(
        df.add_suffix(right_suffix), left_on=ix_r, right_index=True, how="left"
    )

    pairs = pairs.set_geometry(geom_l, crs=points.crs)
    pairs["distance"] = pairs[geom_l].distance(pairs[[geom_r]].set_geometry(geom_r, crs=points.crs))

    if line:
        pairs["geometry"] = sh.shortest_line(pairs[geom_l].array,
                                             pairs[geom_r].array,
        )
        pairs = pairs.set_geometry("geometry")

    return pairs.drop(columns=[ix_l, ix_r])


def match_to_grid(feed, grid, distance, min_default=True):
    """
    Connect a grid to the closest stop on a route and by direction in distance range

    Args :
        feed : a GTFS feed
        grid : a grid (or irregular zones) geodataframe, use index as unique id
        distance : a single distance or a dictionary of route_type : distance,
        min_default : if True, missing route type distance is the minimum distance, else maximum

    Returns:
        a geodataframe with grid index and geometry, agency_name, route_name, direction_id, stop_name, day, time (average stop time from start on a trip)
    """

    # check that grip index is unique
    if not grid.index.is_unique:
        raise ValueError("Grid index must be unique")

    if not feed.is_normalized():
        df = feed.normalize().flat()
    else:
        df = feed.flat()

    # simplify feed data
    df = (
        df.groupby(["route_id", "direction_id", "stop_id", "day"])
        .agg(
            agency_name=("agency_name", "first"),
            route_name=("route_short_name", "first"),
            route_type=("route_type", "first"),
            stop_name=("stop_name", "first"),
            trips=("trip_id", "size"),
            stop_geom=("geometry", "first"),
            time=("time", "max"),
            start=('departure_time', 'min'),
            end=('arrival_time', 'max'),
        )
        .reset_index()
    )
    df = df.set_geometry("stop_geom", crs=feed.projected_crs)

    # format timedelta to simple HH:MM text
    df['start'] = format_timedelta(df['start'])
    df['end'] = format_timedelta(df['end'])

    # spatial join on full feed : stop ids may change between trips
    # TODO : groupby trips with same sequence of stop ids
    #        or sjoin on feed.stops

    if isinstance(distance, int) or isinstance(distance, float):
        dist = distance
    elif isinstance(distance, dict):
        dist = df["route_type"].map(distance)
        if min_default:
            dist = dist.fillna(min(distance.values()))
        else:
            dist = dist.fillna(max(distance.values()))
    else:
        raise ValueError("distance must be a numeric or a dictionary")

    df["_buffer"] = df["stop_geom"].centroid.buffer(dist)
    df = df.set_geometry("_buffer", crs=feed.projected_crs)

    # create grid centroid
    df_grid = grid.to_crs(epsg=feed.projected_crs)
    df_grid = df_grid.rename_geometry("_gridgeom")
    df_grid["_centgeom"] = df_grid.centroid
    df_grid = df_grid.set_geometry("_centgeom", crs=feed.projected_crs)

    df = gpd.sjoin(df, df_grid, predicate="intersects")
    df["dist"] = df["stop_geom"].distance(df["_gridgeom"].centroid)

    # each grid cell is connected to one stop for a route and direction
    df = df.sort_values("dist").drop_duplicates(
        ["index_right", "route_id", "direction_id", "day"], keep="first"
    )

    if grid.index.name is None:
        gr_name = "grid_index"
    else:
        gr_name = grid.index.name

    df = df.rename(columns={"index_right": gr_name, "_gridgeom": grid.geometry.name})
    df = df.set_geometry(grid.geometry.name, crs=feed.projected_crs)

    return df.drop(columns=["stop_geom", "_buffer"])


def _geo_shift(gdf, shift_value):
    # shift is bugged in geopandas, lost crs
    
    #df_s = df[g].shift(shift_value).iloc[shift_value:]
    shifted = gdf.iloc[:-shift_value]
    shifted.index = gdf.index[shift_value:]

    return shifted

def _simple_ix(df):
    """Create an integer array from 0, value changes on array value change"""

    array = df.to_numpy()
    if array.shape[0] < 2:
        return np.array([0])
    int_ix = array[1:] != array[:-1]
    int_ix = np.concatenate([np.array([False]), int_ix], axis=0)
    return np.cumsum(int_ix)