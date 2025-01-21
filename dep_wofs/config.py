import os
from typing import Literal

BUCKET = os.environ.get("WOFS_BUCKET", "dep-public-data")
STAGING_OR_PROD: Literal["staging", "prod"] = "prod"
