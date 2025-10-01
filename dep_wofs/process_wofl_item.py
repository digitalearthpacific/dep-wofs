from datetime import datetime
import json
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
import s3fs
from typer import run

from config import BUCKET, WOFL_DATASET_ID, VERSION
from processors import DepWOfSClassifier


def load_stac_item(stac_url):
    fs = s3fs.S3FileSystem(anon=False, requester_pays=True)
    with fs.open(stac_url, "r") as src:
        stac_json = json.load(src)
    return Item.from_dict(stac_json)


def process_wofl_url(item_url: str, bucket=BUCKET, version=VERSION):
    item = load_stac_item(item_url)
    process_wofl_item(item, bucket, version)


def process_wofl_item(item: Item | str, bucket=BUCKET, version=VERSION):
    if isinstance(item, str):
        item = load_stac_item(item)
    itempath = DailyItemPath(
        bucket=bucket,
        sensor="ls",
        dataset_id=WOFL_DATASET_ID,
        version=version,
        time=item.get_datetime(),
    )
    tile_id = (
        f"{item.properties['landsat:wrs_path']}{item.properties['landsat:wrs_row']}"
    )
    if not object_exists(bucket=bucket, key=itempath.stac_path(tile_id)):
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

        except Exception as e:
            raise e
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


if __name__ == "__main__":
    run(process_wofl_url)
