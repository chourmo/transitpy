"""Parse, normalize and extract information from one or multiple GTFS files"""

from transitpy.datasource.PAN import PAN_Datasource as PAN_Datasource

from transitpy.feed import Feed as Feed
from transitpy.feed import is_gtfs_path as is_gtfs_path

from transitpy.spatial import match_to_grid as match_to_grid
from transitpy.spatial import feed_geometries as feed_geometries

from transitpy.statistics import route_stats as route_stats
from transitpy.statistics import stop_stats as stop_stats
from transitpy.statistics import transfer_route_stats as transfer_route_stats
from transitpy.statistics import transfer_stop_stats as transfer_stop_stats

from transitpy.transfers import make_transfers as make_transfers
