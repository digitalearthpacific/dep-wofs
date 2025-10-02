import json
import sys
from itertools import product
from typing import Annotated, Optional

import typer
from cloud_logger import CsvLogger, filter_by_log, S3Handler
from dep_tools.namers import S3ItemPath
from dep_tools.parsers import datetime_parser, bool_parser

import grid as wofs_grid
from config import BUCKET


def main(
    datetime: Annotated[str, typer.Option(parser=datetime_parser)],
    version: Annotated[str, typer.Option()],
    limit: Optional[str] = None,
    retry_errors: Annotated[str, typer.Option(parser=bool_parser)] = "True",
    grid: Optional[str] = "dep",
    dataset_id: Optional[str] = "wofs_summary_annual",
    overwrite_existing_log: Annotated[str, typer.Option(parser=bool_parser)] = "False",
    filter_using_log: Annotated[str, typer.Option(parser=bool_parser)] = "True",
) -> None:
    this_grid = wofs_grid.grid if grid == "dep" else wofs_grid.ls_grid
    first_name = dict(dep="column", ls="path")
    second_name = dict(dep="row", ls="row")

    params = list()
    for year in datetime:
        itempath = S3ItemPath(
            bucket=BUCKET,
            sensor="ls",
            dataset_id=dataset_id,
            version=version,
            time=str(year).replace("/", "_"),
        )

        logger = CsvLogger(
            name=dataset_id,
            path=f"{itempath.bucket}/{itempath.log_path()}",
            overwrite=overwrite_existing_log,
            header="time|index|status|paths|comment\n",
            cloud_handler=S3Handler,
        )

        grid_subset = (
            filter_by_log(this_grid, logger.parse_log(), retry_errors)
            if filter_using_log
            else this_grid
        )

        these_params = [
            {
                # we could take fromthe grid if we name the dep_grid multiindex
                first_name[grid]: region[0][0],
                second_name[grid]: region[0][1],
                "datetime": region[1],
            }
            for region in product(grid_subset.index, [year])
        ]
        params += these_params

    if limit is not None:
        params = params[0 : int(limit)]

    json.dump(params, sys.stdout)


if __name__ == "__main__":
    typer.run(main)
