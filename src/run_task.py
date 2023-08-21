from typing import List, Union

import geopandas as gpd
import typer
from xarray import DataArray

from azure_logger import CsvLogger, get_log_path, filter_by_log
from dep_tools.runner import run_by_area
from dep_tools.loaders import LandsatOdcLoader
from dep_tools.processors import LandsatProcessor
from dep_tools.utils import get_container_client, scale_and_offset
from dep_tools.writers import AzureXrWriter


def wofs(tm_da: DataArray) -> DataArray:
    # First, rescale to what the wofs model expects
    # (input values should be scaled, not raw int)
    l1_scale = 0.0001
    l1_rescale = 1.0 / l1_scale
    tm_da = scale_and_offset(tm_da, scale=[l1_rescale])
    # lX indicates a left path from node X
    # rX indicates a right
    # dX is just the logic for _that_ node
    tm = tm_da.to_dataset("band")
    tm["ndi52"] = normalized_ratio(tm.swir16, tm.green)
    tm["ndi43"] = normalized_ratio(tm.nir08, tm.red)
    tm["ndi72"] = normalized_ratio(tm.swir22, tm.green)

    d1 = tm.ndi52 <= -0.01
    l2 = d1 & (tm.blue <= 2083.5)
    d3 = tm.swir22 <= 323.5

    l3 = l2 & d3
    w1 = l3 & (tm.ndi43 <= 0.61)

    r3 = l2 & ~d3
    d5 = tm.blue <= 1400.5
    d6 = tm.ndi72 <= -0.23
    d7 = tm.ndi43 <= 0.22
    w2 = r3 & d5 & d6 & d7

    w3 = r3 & d5 & d6 & ~d7 & (tm.blue <= 473.0)

    w4 = r3 & d5 & ~d6 & (tm.blue <= 379.0)
    w7 = r3 & ~d5 & (tm.ndi43 <= -0.01)

    d11 = tm.ndi52 <= 0.23
    l13 = ~d1 & d11 & (tm.blue <= 334.5) & (tm.ndi43 <= 0.54)
    d14 = tm.ndi52 <= -0.12

    w5 = l13 & d14
    r14 = l13 & ~d14
    d15 = tm.red <= 364.5

    w6 = r14 & d15 & (tm.blue <= 129.5)
    w8 = r14 & ~d15 & (tm.blue <= 300.5)

    w10 = (
        ~d1
        & ~d11
        & (tm.ndi52 <= 0.32)
        & (tm.blue <= 249.5)
        & (tm.ndi43 <= 0.45)
        & (tm.red <= 364.5)
        & (tm.blue <= 129.5)
    )

    water = w1 | w2 | w3 | w4 | w5 | w6 | w7 | w8 | w10
    return water.where(tm.red.notnull())


def normalized_ratio(band1: DataArray, band2: DataArray) -> DataArray:
    return (band1 - band2) / (band1 + band2)


class WofsLandsatProcessor(LandsatProcessor):
    def process(self, xr: DataArray) -> DataArray:
        xr = super().process(xr)
        return wofs(xr).resample(time="1Y").mean().squeeze()


def main(
    area_index: List[int],
    datetime: str,
    version: str,
    dataset_id: str = "wofs",
) -> None:
    aoi_by_tile = (
        gpd.read_file(
            "https://deppcpublicstorage.blob.core.windows.net/output/aoi/aoi_split_by_landsat_pathrow.gpkg"
        )
        .set_index(["PATH", "ROW"], drop=False)
        .loc[area_index]
    )

    prefix = f"{dataset_id}/{version}"

    loader = LandsatOdcLoader(
        datetime=datetime,
        dask_chunksize=dict(band=1, time=1, x=4096, y=4096),
        odc_load_kwargs=dict(
            resampling={"qa_pixel": "nearest", "*": "cubic"},
            fail_on_error=False,
            resolution=30,
        ),
    )

    processor = WofsLandsatProcessor(dilate_mask=True)
    writer = AzureXrWriter(
        dataset_id=dataset_id,
        year=datetime,
        prefix=prefix,
        convert_to_int16=True,
        overwrite=False,
        output_value_multiplier=10000,
        extra_attrs=dict(dep_version=version),
    )
    logger = CsvLogger(
        name=dataset_id,
        container_client=get_container_client(),
        path=get_log_path(prefix, dataset_id, version),
        overwrite=False,
        header="time|index|status|paths|comment\n",
    )

    breakpoint()
    run_by_area(
        areas=aoi_by_tile,
        loader=loader,
        processor=processor,
        writer=writer,
        logger=logger,
    )


if __name__ == "__main__":
    typer.run(main)
