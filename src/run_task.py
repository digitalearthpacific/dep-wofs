from typing_extensions import Annotated

import numpy as np
import typer
from xarray import DataArray, Dataset

from azure_logger import CsvLogger, get_log_path
from dep_tools.loaders import LandsatOdcLoader
from dep_tools.namers import DepItemPath
from dep_tools.processors import LandsatProcessor
from dep_tools.runner import run_by_area_dask_local
from dep_tools.utils import get_container_client, scale_and_offset
from dep_tools.writers import AzureDsWriter

from grid import grid


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
    def process(self, xr: DataArray) -> Dataset:
        xr = super().process(xr)
        output = wofs(xr).resample(time="1Y").mean().squeeze()
        start_datetime = np.datetime_as_string(
            np.datetime64(xr.time.min().values, "Y"), unit="ms"
        )

        end_datetime = np.datetime_as_string(
            np.datetime64(xr.time.max().values, "Y")
            + np.timedelta64(1, "Y")
            - np.timedelta64(1, "ns")
        )
        # This _should_ set this attr on the output cog
        output["time"] = start_datetime
        output.attrs["stac_properties"] = dict(
            start_datetime=start_datetime, end_datetime=end_datetime
        )

        return output.to_dataset(name="mean")


def main(
    region_code: Annotated[str, typer.Option()],
    region_index: Annotated[str, typer.Option()],
    datetime: Annotated[str, typer.Option()],
    version: Annotated[str, typer.Option()],
    dataset_id: str = "wofs",
) -> None:
    cell = grid.loc[[(region_code, region_index)]]

    loader = LandsatOdcLoader(
        epsg=3832,
        datetime=datetime,
        dask_chunksize=dict(band=1, time=1, x=1024, y=1024),
        odc_load_kwargs=dict(fail_on_error=False, resolution=30),
    )

    processor = WofsLandsatProcessor(dilate_mask=True)

    itempath = DepItemPath("ls", dataset_id, version, datetime)

    writer = AzureDsWriter(
        itempath=itempath,
        convert_to_int16=True,
        overwrite=True,
        output_value_multiplier=100,
        extra_attrs=dict(dep_version=version),
    )
    logger = CsvLogger(
        name=dataset_id,
        container_client=get_container_client(),
        path=get_log_path(dataset_id, version, datetime),
        overwrite=False,
        header="time|index|status|paths|comment\n",
    )

    run_by_area_dask_local(
        areas=cell,
        loader=loader,
        processor=processor,
        writer=writer,
        logger=logger,
        continue_on_error=False,
    )


if __name__ == "__main__":
    typer.run(main)
