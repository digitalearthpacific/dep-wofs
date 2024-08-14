import json
import sys
from itertools import product
from typing import Annotated, Optional

import typer
from aiobotocore.session import AioSession
from cloud_logger import CsvLogger, filter_by_log, S3Handler
from dep_tools.namers import S3ItemPath

from grid import grid


def parse_datetime(datetime):
    years = datetime.split("-")
    if len(years) == 2:
        years = range(int(years[0]), int(years[1]) + 1)
    elif len(years) > 2:
        ValueError(f"{datetime} is not a valid value for --datetime")
    return years


def main(
    datetime: Annotated[str, typer.Option()],
    version: Annotated[str, typer.Option()],
    limit: Optional[str] = None,
    no_retry_errors: Optional[bool] = False,
    dataset_id: str = "wofs",
) -> None:

    years = parse_datetime(datetime)

    itempath = S3ItemPath(
        bucket="dep-public-staging",
        sensor="ls",
        dataset_id=dataset_id,
        version=version,
        time=datetime,
    )

    logger = CsvLogger(
        name=dataset_id,
        path=f"{itempath.bucket}/{itempath.log_path()}",
        overwrite=False,
        header="time|index|status|paths|comment\n",
        cloud_handler=S3Handler,
        session=AioSession(profile="dep-staging-admin"),
    )

    grid_subset = filter_by_log(grid, logger.parse_log(), not no_retry_errors)
    params = [
        {
            "row": region[0][0],
            "column": region[0][1],
            "datetime": region[1],
        }
        for region in product(grid_subset.index, years)
    ]

    if limit is not None:
        params = params[0 : int(limit)]

    json.dump(params, sys.stdout)


if __name__ == "__main__":
    typer.run(main)
