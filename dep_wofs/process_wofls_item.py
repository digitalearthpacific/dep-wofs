from logging import Logger, getLogger
from pathlib import Path
import traceback
import warnings

import boto3
from dep_tools.aws import object_exists, s3_dump
from dep_tools.loaders import OdcLoader
from dep_tools.namers import S3ItemPath
from dep_tools.task import AwsDsCogWriter, Task
from dep_tools.loaders import StacLoader
from dep_tools.processors import Processor
from dep_tools.writers import Writer, AwsStacWriter
from dep_tools.stac_utils import StacCreator
from pystac import Item

from config import BUCKET, WOFL_DATASET_ID, VERSION
from processors import DepWOfSClassifier


def process_wofl_item(item: Item, tile_id, version=VERSION):
    itempath = S3ItemPath(
        bucket=BUCKET,
        sensor="ls",
        dataset_id=WOFL_DATASET_ID,
        version=version,
        time=item.get_datetime(),
    )
    if not object_exists(bucket=BUCKET, key=itempath.stac_path(tile_id)):
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
                loader=loader,
                processor=DepWOfSClassifier(),
                writer=AwsDsCogWriter(itempath),
                stac_creator=StacCreator(itempath),
                stac_writer=AwsStacWriter(itempath),
            ).run(item)

        except Exception as e:
            warnings.warn("Error from one of the dailies, check the output logs")
            daily_log_path = Path(itempath.log_path()).with_suffix(".error.txt")
            boto3_client = boto3.client("s3")

            s3_dump(
                data=traceback.format_exc(),
                bucket=BUCKET,
                key=str(daily_log_path),
                client=boto3_client,
            )


def copy_stac_properties(item, ds):
    ds.attrs["stac_properties"] = {
        **ds.attrs["stac_properties"],
        **item.properties,
    }
    ds.attrs["stac_properties"]["start_datetime"] = ds.attrs["stac_properties"][
        "datetime"
    ]
    ds.attrs["stac_properties"]["end_datetime"] = ds.attrs["stac_properties"][
        "datetime"
    ]
    return ds


class ItemStacTask(Task):
    def __init__(
        self,
        id: str,
        loader: StacLoader,
        processor: Processor,
        writer: Writer,
        post_processor: Processor | None = None,
        stac_creator: StacCreator | None = None,
        stac_writer: Writer | None = None,
        logger: Logger = getLogger(),
    ):
        super().__init__(
            task_id=id, loader=loader, processor=processor, writer=writer, logger=logger
        )
        self.post_processor = post_processor
        self.stac_creator = stac_creator
        self.stac_writer = stac_writer

    def run(self, item):
        input_data = self.loader.load([item], areas=None)

        output_data = copy_stac_properties(item, self.processor.process(input_data))

        if self.post_processor is not None:
            output_data = self.post_processor.process(output_data)

        paths = self.writer.write(output_data, self.id)

        if self.stac_creator is not None and self.stac_writer is not None:
            stac_item = self.stac_creator.process(output_data, self.id)
            self.stac_writer.write(stac_item, self.id)

        return paths
