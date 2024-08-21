import json
import sys
from itertools import product
from typing import Annotated, Optional, Literal

import typer
from aiobotocore.session import AioSession
from cloud_logger import CsvLogger, filter_by_log, S3Handler
from dep_tools.namers import S3ItemPath

import grid as wofs_grid


def parse_datetime(datetime):
    years = datetime.split("-")
    if len(years) == 2:
        years = range(int(years[0]), int(years[1]) + 1)
    elif len(years) > 2:
        ValueError(f"{datetime} is not a valid value for --datetime")
    return years


def bool_parser(raw: str):
    return False if raw == "False" else True


def main(
    datetime: Annotated[str, typer.Option()],
    version: Annotated[str, typer.Option()],
    limit: Optional[str] = None,
    retry_errors: Annotated[str, typer.Option(parser=bool_parser)] = "True",
    grid: Optional[str] = "dep",
    setup_auth: Annotated[str, typer.Option(parser=bool_parser)] = "False",
    dataset_id: str = "wofs",
) -> None:
    if setup_auth:
        from aiobotocore.session import AioSession

        handler_kwargs = dict(session=AioSession(profile="dep-staging-admin"))
    else:
        handler_kwargs = dict()

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
        **handler_kwargs,
    )

    this_grid = wofs_grid.grid if grid == "dep" else wofs_grid.ls_grid
    first_name = dict(dep="row", ls="path")
    second_name = dict(dep="column", ls="row")
    grid_subset = filter_by_log(this_grid, logger.parse_log(), retry_errors)
    params = [
        {
            # we could take fromthe grid if we name the dep_grid multiindex
            first_name[grid]: region[0][0],
            second_name[grid]: region[0][1],
            "datetime": region[1],
        }
        for region in product(grid_subset.index, years)
    ]

    if limit is not None:
        params = params[0 : int(limit)]

    json.dump(params, sys.stdout)


if __name__ == "__main__":
    typer.run(main)
