import os

BUCKET = os.environ.get("WOFS_BUCKET", "dep-public-data")
"""The output bucket."""

VERSION = "0.2.0"
"""The data version."""

WOFL_DATASET_ID = "wofl"
"""The dataset identifier for water observations feature layer (WOFL)."""

OUTPUT_COLLECTION_ROOT = os.environ.get(
    "OUTPUT_COLLECTION_ROOT", "https://stac.digitalearthpacific.org"
)
"""The root path of the stac collections."""
