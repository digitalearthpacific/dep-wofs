import os

BUCKET = os.environ.get("WOFS_BUCKET", "dep-public-data")
OUTPUT_COLLECTION_ROOT = os.environ.get(
    "OUTPUT_COLLECTION_ROOT", "https://stac.digitalearthpacific.org"
)
