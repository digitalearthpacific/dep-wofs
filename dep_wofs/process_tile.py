from typing_extensions import Annotated

import boto3
from distributed import Client
from odc.stac import configure_s3_access
from typer import Option, run

from cloud_logger import CsvLogger, S3Handler
from dep_tools.loaders import OdcLoader
from dep_tools.namers import S3ItemPath
from dep_tools.processors import XrPostProcessor
from dep_tools.searchers import LandsatPystacSearcher
from dep_tools.task import AwsStacTask as Task
from grid import grid
from processors import WoflWofsProcessor


SR_BANDS = ["blue", "green", "red", "nir08", "swir16", "swir22"]


def bool_parser(raw: str):
    return False if raw == "False" else True


def main(
    row: Annotated[str, Option(parser=int)],
    column: Annotated[str, Option(parser=int)],
    datetime: Annotated[str, Option()],
    version: Annotated[str, Option()],
    dataset_id: str = "wofs",
) -> None:
    boto3.setup_default_session()
    configure_s3_access(cloud_defaults=True, requester_pays=True)
    cell = grid.loc[[(column, row)]]

    itempath = S3ItemPath(
        bucket="dep-public-staging",
        sensor="ls",
        dataset_id=dataset_id,
        version=version,
        time=datetime,
    )

    searcher = LandsatPystacSearcher(
        catalog="https://earth-search.aws.element84.com/v1",
        exclude_platforms=["landsat-7"],
        datetime=datetime,
    )

    stacloader = OdcLoader(
        # clip_to_area=True,
        groupby="solar_day",  # OK but we may need to revisit for clouds
        crs=cell.crs,
        dtype="uint16",
        bands=SR_BANDS + ["qa_pixel"],
        chunks=dict(band=1, time=1, x=4096, y=4096),
        fail_on_error=False,
        resolution=30,
    )

    processor = WoflWofsProcessor(send_area_to_processor=True)
    post_processor = XrPostProcessor(
        convert_to_int16=True,
        output_value_multiplier=100,
        extra_attrs=dict(dep_version=version),
    )

    logger = CsvLogger(
        name=dataset_id,
        path=f"{itempath.bucket}/{itempath.log_path()}",
        overwrite=False,
        header="time|index|status|paths|comment\n",
    )

    id = (row, column)

    try:
        paths = Task(
            itempath=itempath,
            id=id,
            area=cell,
            searcher=searcher,
            loader=stacloader,
            processor=processor,
            post_processor=post_processor,
            logger=logger,
        ).run()
    except Exception as e:
        logger.error([id, "error", e])
        raise e

    logger.info([id, "complete", paths])


if __name__ == "__main__":
    with Client():
        run(main)
