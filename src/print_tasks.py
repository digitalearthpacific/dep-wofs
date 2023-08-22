from itertools import product

import typer
from azure_logger import CsvLogger, filter_by_log, get_log_path
from dep_tools.utils import get_container_client
from grid import grid


def main(
    dataset_id: str = "wofs", version: str = "21Aug2023", years=range(2013, 2024)
) -> None:
    aoi_by_tile = grid

    prefix = f"{dataset_id}/{version}"
    logger = CsvLogger(
        name=dataset_id,
        container_client=get_container_client(),
        path=get_log_path(prefix, dataset_id, version),
        overwrite=False,
        header="time|index|status|paths|comment\n",
    )

    aoi_by_tile = filter_by_log(aoi_by_tile, logger.parse_log())
    print(list(product(aoi_by_tile.index, years)))


if __name__ == "__main__":
    typer.run(main)
