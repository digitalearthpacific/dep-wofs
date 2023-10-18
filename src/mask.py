from typing_extensions import Annotated

from geopandas import GeoDataFrame, read_file
from pystac_client import Client
import typer
from xarray import DataArray, Dataset

from azure_logger import CsvLogger, filter_by_log
from dep_tools.azure import get_container_client
from dep_tools.exceptions import EmptyCollectionError
from dep_tools.loaders import OdcLoaderMixin, StackXrLoader
from dep_tools.processors import Processor
from dep_tools.stac_utils import set_stac_properties
from dep_tools.utils import search_across_180
from dep_tools.namers import DepItemPath
from dep_tools.runner import run_by_area
from dep_tools.stac_utils import set_stac_properties
from dep_tools.writers import AzureDsWriter


class DepLoader(OdcLoaderMixin, StackXrLoader):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _get_items(self, area):
        stac_catalog = "https://stac.staging.digitalearthpacific.org"
        client = Client.open(stac_catalog)
        item_collection = search_across_180(
            area, client=client, collections=["dep_ls_wofs"], datetime=self.datetime
        )
        if len(item_collection.items) == 0:
            raise EmptyCollectionError
        return item_collection


class Clipper(Processor):
    def process(self, xr: DataArray, area: GeoDataFrame) -> Dataset:
        output = xr.rio.clip(area.geometry)
        output_nodata = -32767
        output = output.where(output.notnull(), output_nodata)

        return (
            set_stac_properties(xr, output)
            .to_dataset(name="mean", promote_attrs=True)
            .squeeze()
            .astype("int16")
        )


def main(
    datetime: Annotated[str, typer.Option()],
    version: Annotated[str, typer.Option()],
    dataset_id: str = "wofs",
):
    from grid import grid

    land = (
        read_file(
            "https://deppcpublicstorage.blob.core.windows.net/output/aoi/aoi.gpkg"
        )
        .to_crs(grid.crs)
        .unary_union.buffer(90)
    )
    areas = grid.intersection(land)
    grid.geometry = areas
    grid = grid[~grid.geometry.is_empty]

    loader = DepLoader(
        epsg=3832, datetime=datetime, dask_chunksize=dict(x=4096, y=4096)
    )
    processor = Clipper(send_area_to_processor=True)
    itempath = DepItemPath("ls", dataset_id, version, datetime)

    writer = AzureDsWriter(
        itempath=itempath,
        extra_attrs=dict(dep_version=version),
    )
    logger = CsvLogger(
        name=dataset_id,
        container_client=get_container_client(),
        path=itempath.log_path(),
        overwrite=True,
        header="time|index|status|paths|comment\n",
    )
    grid = filter_by_log(grid, logger.parse_log())

    run_by_area(
        areas=grid,
        loader=loader,
        processor=processor,
        writer=writer,
        logger=logger,
        continue_on_error=False,
    )


if __name__ == "__main__":
    main("2013", "0.0.4")
