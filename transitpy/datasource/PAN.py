import pandas as pd
import geopandas as gpd

from .datasource import Datasource
from .data_utils import comparable_string, expand_json


class PAN_Datasource(Datasource):
    """Find and download gtfs data from point d'acces national (french repository) API"""

    def __init__(self):

        super().__init__(
            "https://transport.data.gouv.fr/api/datasets",
            "name",
            ["url", "original_url"],
        )
        self._parse_datasets()
        self._filter_datasets()
        self.data = self.data[
            [
                "dataset_id",
                "name",
                "type",
                "departement",
                "updated",
                "start",
                "end",
                "url",
                "original_url",
                "geometry",
            ]
        ]

    def _parse_datasets(self):

        df = pd.read_json(self.source_url)
        df = df.loc[
            df.type == "public-transit",
            ["aom", "resources", "covered_area", "publisher", "title"],
        ]
        df = df.rename(columns={"title": "dataset_title"}).reset_index(drop=True)

        # expand publisher
        df = expand_json(df, "publisher", "name")
        df = df.rename(columns={"name": "publisher_name"})

        # expand aom
        df = expand_json(df, "aom", "name").rename(columns={"name": "aom_name"})

        # merge geojson
        geo = gpd.read_file("https://transport.data.gouv.fr/api/aoms/geojson")
        df = pd.merge(
            df,
            geo[["geometry", "departement", "nom"]],
            left_on="aom_name",
            right_on="nom",
            how="left",
        ).drop(columns=["nom"])

        # expand covered_area
        df = expand_json(df, "covered_area", ["name", "type"])
        df = df.rename(columns={"name": "area_name"})

        # expand ressources
        cols = [
            "format",
            "datagouv_id",
            "title",
            "original_url",
            "url",
            "updated",
            "metadata.start_date",
            "metadata.end_date",
        ]
        df = expand_json(df, "resources", unstack=True)
        df = expand_json(df, "resources", subset=cols)
        df = df.rename(
            columns={"metadata.start_date": "start", "metadata.end_date": "end"}
        )

        df = df.loc[df.format == "GTFS"].drop(columns="format")

        cols = ["aom_name", "dataset_title", "url", "original_url"]
        self.missing_data = df.loc[df["start"].isna(), cols].copy()

        df = df.loc[~df["start"].isna()]
        df["start"] = pd.to_datetime(df["start"])
        df["end"] = pd.to_datetime(df["end"])
        df["updated"] = pd.to_datetime(df.updated.str[:10])

        df.index.name = "dataset_id"
        self.data = df.reset_index()

        return None

    def _filter_datasets(self):
        """drop duplicated ressources and create a unique name"""

        # set name

        self.data[self.name] = self.data.loc[
            self.data["aom_name"] != "France", "aom_name"
        ]
        self.data[self.name] = self.data[self.name].fillna(self.data["dataset_title"])
        self.data["_upper_name"] = comparable_string(self.data[self.name])

        # drop duplicated datagouv_id
        self.data = self.data.sort_values(["datagouv_id", "updated"], ascending=False)
        self.data = self.data.drop_duplicates(["datagouv_id", "updated"])

        # drop duplicated ressources

        self.data = self.data.sort_values(["_upper_name", "updated"], ascending=False)
        self.data = self.data.drop_duplicates(["_upper_name", "title", "dataset_title"])

        self._filter_publishers(dup_name="_upper_name")
        self._filter_today()

        # set name to name + resource title if duplicated name
        self.data.loc[
            self.data["_upper_name"].duplicated(keep=False), self.name
        ] = self.data["dataset_title"]
        self.data["_upper_name"] = comparable_string(self.data[self.name])

        self.data.loc[
            self.data[["_upper_name", "dataset_title"]].duplicated(keep=False),
            self.name,
        ] = (
            self.data[self.name] + " " + self.data["title"]
        )

        self.data = self.data.drop(
            columns=[
                "dataset_title",
                "title",
                "aom_name",
                "area_name",
                "_upper_name",
                "datagouv_id",
            ]
        )

        return None

    def _filter_publishers(self, dup_name):
        """keep smallest publisher for each name/dataset tile"""

        df = self.data

        df["publisher_name"] = df["publisher_name"].fillna(df["aom_name"])

        df["_pub"] = pd.merge(
            df[["publisher_name"]],
            df["publisher_name"].value_counts().to_frame("_c"),
            left_on="publisher_name",
            right_index=True,
        )["_c"]

        df["_upper_dataset"] = comparable_string(df["dataset_title"])
        df["_min_pub"] = df.groupby([dup_name, "_upper_dataset"])["_pub"].transform(min)

        self.data = df.loc[df._pub == df._min_pub].drop(
            columns=["publisher_name", "_pub", "_min_pub", "_upper_dataset"]
        )

    def _filter_today(self):
        """keep valid today feed or last date if duplicated"""

        df = self.data
        df["_dist"] = self._day_distance(df["start"], df["end"])
        df["_min_dist"] = df.groupby(["name", "dataset_title"])["_dist"].transform(min)

        self.data = df.loc[df._dist == df._min_dist].drop(
            columns=["_dist", "_min_dist"]
        )

        return None

    def _day_distance(self, start, end):
        """number of days from today between start and end time Series"""

        today = pd.Timestamp.today()
        df = today - start
        df = df.dt.days.abs().to_frame("_st")
        df["_end"] = end - today
        df["_end"] = df["_end"].dt.days.abs()

        df["_range"] = (pd.Timestamp.today() >= start) & (pd.Timestamp.today() <= end)
        df.loc[df._range, "_st"] = 0

        return df[["_st", "_end"]].min(axis=1)
