import json
from itertools import product
from typing import Annotated

import typer
from azure_logger import CsvLogger, filter_by_log, get_log_path
from dep_tools.utils import get_container_client

from grid import grid

#    region_codes: Annotated[List, typer.Option()],
#    dataset_id: str = "wofs",
#    version: str = "21Aug2023",
#    years=2023
#    limit: int = None,


def main(
    region_code: Annotated[str, typer.Option()],
    datetime: Annotated[str, typer.Option()],
    version: Annotated[str, typer.Option()],
    limit: Annotated[str, typer.Option()],
    dataset_id: str = "wofs",
) -> None:
    grid_subset = (
        grid.loc[grid.code == region_code] if region_code is not None else grid
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
        {"region_code": region[0][0], "region_id": region[0][1], "datetime": region[1]}
        for region in product(grid_subset.index, [datetime])
    ]

    if limit is not None:
        params = params[0: int(limit)]

    json.dump(params)


if __name__ == "__main__":
    typer.run(main)
