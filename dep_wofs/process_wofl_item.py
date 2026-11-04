"""Create the WOfL (water observation feature layer) for a single Landsat scene."""

from pathlib import Path
import traceback
import warnings

import boto3
from dep_tools.aws import object_exists, s3_dump
from dep_tools.loaders import OdcLoader
from dep_tools.namers import DailyItemPath
from dep_tools.task import AwsDsCogWriter, ItemStacTask
from dep_tools.writers import AwsStacWriter
from dep_tools.stac_utils import StacCreator
from pystac import Item

from config import BUCKET, WOFL_DATASET_ID, VERSION
from processors import DepWOfSClassifier


def process_wofl_item(item: Item, bucket=BUCKET, version=VERSION):
    """Make and write the WOfL for one Landsat scene, unless it already exists.

    Errors are not raised. Instead, the error message is written to S3 as a
    ``.error.txt`` file next to the log.

    Args:
        item: The Landsat Collection 2 Level 2 STAC item.
        bucket: The output bucket.
        version: The output version.

    Returns:
        The paths written, or None if the output already existed or processing
        failed.
    """
    itempath = DailyItemPath(
        bucket=bucket,
        sensor="ls",
        dataset_id=WOFL_DATASET_ID,
        version=version,
        time=item.get_datetime(),
    )
    tile_id = (
        # "{item.properties['landsat:wrs_path']}{item.properties['landsat:wrs_row']}"
        item.properties["landsat:wrs_path"],
        item.properties["landsat:wrs_row"],
    )
    if not object_exists(bucket=bucket, key=itempath.stac_path(tile_id)):
        fix_ard_bucket_hrefs(item)
        try:
            loader = OdcLoader(
                dtype="uint16",
                bands=["blue", "green", "red", "nir08", "swir16", "swir22", "qa_pixel"],
                chunks=dict(band=1, time=1, x=4096, y=4096),
                stac_cfg={
                    "landsat-c2l2-sr": {
                        "assets": {"*": {"nodata": 0}, "qa_pixel": {"nodata": 1}}
                    }
                },
                anchor="center",
            )
            return ItemStacTask(
                id=tile_id,
                item=item,
                loader=loader,
                processor=DepWOfSClassifier(),
                writer=AwsDsCogWriter(itempath),
                stac_creator=StacCreator(itempath),
                stac_writer=AwsStacWriter(itempath),
            ).run()

        except Exception:
            daily_log_path = Path(itempath.log_path()).with_suffix(".error.txt")
            warnings.warn(
                f"Error while processing item. Log file copied to {daily_log_path}"
            )
            boto3_client = boto3.client("s3")

            s3_dump(
                data=traceback.format_exc(),
                bucket=bucket,
                key=str(daily_log_path),
                client=boto3_client,
            )


def fix_ard_bucket_hrefs(item: Item) -> None:
    """Point asset links at the bucket where the Landsat files actually are.

    Some Tier 2 items in the USGS STAC API give S3 links in the
    ``usgs-landsat-ard`` bucket, but the files are in ``usgs-landsat`` at the
    same path. Reading the wrong bucket fails with "Access Denied". For example:
    https://landsatlook.usgs.gov/stac-server/collections/landsat-c2l2-sr/items/LC08_L2SR_076069_20201231_20210308_02_T2_SR

    Args:
        item: The Landsat Collection 2 Level 2 STAC item. Changed in place.
    """
    wrong_prefix = "s3://usgs-landsat-ard/collection02/level-2/"
    right_prefix = "s3://usgs-landsat/collection02/level-2/"
    for asset in item.assets.values():
        if asset.href.startswith(wrong_prefix):
            asset.href = asset.href.replace(wrong_prefix, right_prefix, 1)
