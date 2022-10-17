"""Parse, normalize and extract information from one or multiple GTFS files"""

# Add imports here
from transitpy.feed import is_gtfs_data, Feed
from transitpy.statistics import (
    route_stats,
    stop_stats,
    transfer_route_stats,
    transfer_stop_stats,
)
from transitpy.spatial import match_to_grid
from transitpy.transfers import make_transfers
from transitpy.datasource.PAN import PAN_Datasource


# Handle versioneer
from ._version import get_versions
versions = get_versions()
__version__ = versions['version']
__git_revision__ = versions['full-revisionid']
del get_versions, versions

from . import _version
__version__ = _version.get_versions()['version']
