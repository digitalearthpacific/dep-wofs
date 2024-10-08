from typing_extensions import Annotated

import boto3
from distributed import Client
from typer import Option, run

from cloud_logger import CsvLogger, S3Handler
from dep_tools.loaders import OdcLoader
from dep_tools.namers import S3ItemPath
from dep_tools.processors import XrPostProcessor
from dep_tools.searchers import PystacSearcher
from dep_tools.task import AwsStacTask as Task

from grid import grid
from processors import WofsProcessor


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
    cell = grid.loc[[(column, row)]]

    itempath = S3ItemPath(
        bucket="dep-public-staging",
        sensor="ls",
        dataset_id=dataset_id,
        version=version,
        time=datetime,
    )

    searcher = PystacSearcher(
        catalog="https://stac.staging.digitalearthpacific.io",
        datetime=datetime,
    )

    stacloader = OdcLoader(
        crs=cell.crs,
        dtype="uint16",
        chunks=dict(x=4096, y=4096),
        fail_on_error=False,
        resolution=30,
    )

    processor = WofsProcessor(send_area_to_processor=True)
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
        cloud_handler=S3Handler,
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
