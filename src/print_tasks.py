import json
import sys
from itertools import product
from typing import Annotated, Optional

import typer
from azure_logger import CsvLogger, filter_by_log, get_log_path
from dep_tools.utils import get_container_client

from grid import grid


def main(
    regions: Annotated[str, typer.Option()],
    datetime: Annotated[str, typer.Option()],
    version: Annotated[str, typer.Option()],
    limit: Optional[str] = None,
    dataset_id: str = "wofs",
) -> None:
    region_codes = None if regions.upper() == "ALL" else regions.split(",")

    years = datetime.split(",")

    grid_subset = (
        grid.loc[grid.code.isin(region_codes)] if region_codes is not None else grid
    )

    prefix = f"{dataset_id}/{version}"
    logger = CsvLogger(
        name=dataset_id,
        container_client=get_container_client(),
        path=get_log_path(prefix, dataset_id, version),
        overwrite=False,
        header="time|index|status|paths|comment\n",
    )

    filter_by_log(grid_subset, logger.parse_log())
    params = [
        {
            "region-code": region[0][0],
            "region-index": region[0][1],
            "datetime": region[1],
        }
        for region in product(grid_subset.index, [years])
    ]

    if limit is not None:
        params = params[0 : int(limit)]

    json.dump(params, sys.stdout)


if __name__ == "__main__":
    typer.run(main)
