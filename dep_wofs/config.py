import os

BUCKET = os.environ.get("WOFS_BUCKET", "dep-public-data")
VERSION = "0.2.0"
WOFL_DATASET_ID = "wofl"
OUTPUT_COLLECTION_ROOT = os.environ.get(
    "OUTPUT_COLLECTION_ROOT", "https://stac.digitalearthpacific.org"
)
