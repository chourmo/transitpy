# -*- coding: utf-8 -*-

import pandas as pd
import geopandas as gpd
import pygeos as pg
import streetpy as st

from . import spatial


class Shapes_functions(object):

    """
    Shape functions
    """

    def simple_shapes(self, overwrite=False):
        """
        create shapes.txt from stop to stop if shapes.txt is absent or
        for missing shape_ids in trips
        update shape_dist_traveled in trips
        """

        if (not overwrite) & (self.has_coherent_shapes()) & (self.shapes is not None):
            return None

        paths = self.stop_times[["trip_id", "stop_sequence", "stop_id"]].copy()
        paths[["stop_lon", "stop_lat", "geometry"]] = self.stops_as_stoptimes()[
            ["stop_lon", "stop_lat", "geometry"]
        ]

        # find a shape id for each trip_id with same sequence of stops
        df = paths.groupby("trip_id")["stop_id"].agg(tuple).to_frame("shape_id")
        df["shape_id"] = df["shape_id"].astype("category").cat.codes

        # conform paths to shapes.txt content
        paths = pd.merge(paths, df, left_on="trip_id", right_index=True)
        paths = paths.drop_duplicates(["stop_id", "stop_sequence", "shape_id"])
        paths = paths.drop(columns=["trip_id", "stop_id"]).reset_index(drop=True)
        paths = paths.rename(
            columns={
                "stop_lon": "shape_pt_lon",
                "stop_lat": "shape_pt_lat",
                "stop_sequence": "shape_pt_sequence",
            }
        )

        paths = paths.set_geometry("geometry", crs=self.stops.geometry.crs)

        paths["shape_dist_traveled"] = spatial.dist_traveled(
            paths, "shape_id", accumulate=True, as_integer=True
        )

        if self.shapes is None:
            self.shapes = paths

        # add shape_ids to trips

        self.trips = self.trips.set_index("trip_id")
        self.trips.loc[df.index, "shape_id"] = df["shape_id"]
        self.trips = self.trips.reset_index()

        return None

    def shapes_on_graph(
        self,
        streets,
        rails,
        weight,
        distance,
        k_nearest=6,
        tolerance=None,
        street_graph=None,
        rail_graph=None,
    ):
        """
        find paths on a streetpy streets dataframe, updates self shapes and
        dist_traveled columns

        Args :
            streets: a streetpy single-mode directed road network
            rails: a streetpy single-mode directed rail network
            distance: distance to match stops and street
            weight: column of streets to minimize path between stops
            k_nearest: maximum number of stop/edge pairs in distance
              to consider
            tolerance: simplify geometries (see geopandas.simplify)
            street_graph: optional pandana graph extracted for buses
            rail_graph: optional pandana graph extracted for rail based modes

        Returns : a DataFrame with all streets used on each shape stop
            - shape_id : original shape_id
            - stop : stop number in original shape, starts at one
            - source and target names of streets netdataframe
            - geometry of the edge, cut at stop points
        """

        n = streets.net.name

        traj = self.modal_filter(modes=3)
        traj = st.match_trajectories(
            traj.shapes.set_index("shape_id")["geometry"],
            streets,
            weight=weight,
            distance=distance,
            k_nearest=k_nearest,
            graph=street_graph,
        )

        traj_rail = self.modal_filter(modes=[0, 1, 2, 5, 6, 7, 11, 12])
        traj_rail = st.match_trajectories(
            traj.shapes.set_index("shape_id")["geometry"],
            streets,
            weight=weight,
            distance=distance,
            k_nearest=k_nearest,
            graph=rail_graph,
        )

        traj = pd.concat([traj, traj_rail])

        # rename columns
        shapes = traj.drop(columns=[streets.net.name])
        shapes = shapes.rename(columns={"stop": "shape_pt_sequence"})
        shapes = shapes.to_crs(self.projected_crs)

        # simplify geometry
        if tolerance is not None:
            shapes.geometry = shapes.geometry.simplify(
                tolerance=tolerance, preserve_topology=True
            )

        # convert geometry to multipoint and explode to single points
        coords = spatial._linestring_coordinates(shapes.geometry, reindex=True)
        shapes = pd.merge(
            coords,
            shapes.drop(columns=shapes.geometry.name).reset_index(),
            left_index=True,
            right_index=True,
            how="left",
        )

        shapes = shapes.set_geometry(
            gpd.points_from_xy(shapes["x"], shapes["y"], crs=self.projected_crs)
        )

        shapes = shapes.reset_index(drop=True)
        shapes["shape_dist_traveled"] = spatial.dist_traveled(
            shapes, "shape_id", accumulate=True, as_integer=True
        )

        shapes = shapes.to_crs(4326)
        shapes["shape_pt_lon"] = shapes.geometry.x
        shapes["shape_pt_lat"] = shapes.geometry.y
        shapes = shapes.drop(columns=[shapes.geometry.name, "x", "y"])

        # add not projected shapes
        ids = shapes.shape_id.drop_duplicates().values
        df = self.shapes.loc[~self.shapes.shape_id.isin(ids)]

        shapes = pd.concat(shapes, df, ignore_index=True)
        shapes = shapes.sort_values(
            ["shape_id", "shape_pt_sequence", "shape_dist_traveled"]
        )

        self.shapes = shapes

        return traj

    def shape_geometries(self, projected_crs=True):
        """
        returns a GeoSeries of the shape geometries, shape_id as index
        if projected_crs is True, reproject in the feed projected crs
        """

        df = self.shapes[["shape_id", "shape_pt_lon", "shape_pt_lat"]].copy()
        df = df.groupby("shape_id").apply(
            lambda x: [x["shape_pt_lon"].values, x["shape_pt_lat"].values]
        )

        df = gpd.GeoSeries(
            df.apply(lambda x: pg.creation.linestrings(x[0], y=x[1])), crs=4326
        )

        if projected_crs:
            df = df.to_crs(self.projected_crs)

        return df


def _shape_pt_sequence(df):
    """Get shape_pt_sequence from an ordered shape_id Series"""
    res = df.reset_index(drop=True).to_frame("data")
    res["pos"] = res.index
    res["sub"] = res.groupby("data").transform(min)
    res = res["pos"] - res["sub"]
    res.index = df.index
    return res
