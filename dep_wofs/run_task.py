from typing_extensions import Annotated
from dep_tools.searchers import LandsatPystacSearcher

import typer
import planetary_computer as pc
from xarray import DataArray, Dataset

from cloud_logger import CsvLogger, S3Handler
from dep_tools.aws import write_stac_aws, write_to_s3
from dep_tools.loaders import OdcLoader, SearchLoader
from dep_tools.namers import DepItemPath
from dep_tools.processors import LandsatProcessor, XrPostProcessor
from dep_tools.task import ErrorCategoryAreaTask as Task
from dep_tools.stac_utils import (
    set_stac_properties,
    StacCreator,
)
from dep_tools.utils import scale_and_offset
from dep_tools.writers import DepWriter, DsCogWriter, StacWriter
from grid import grid


def wofs(tm: Dataset) -> DataArray:
    # First, rescale to what the wofs model expects
    # (input values should be scaled, not raw int)
    l1_scale = 0.0001
    l1_rescale = 1.0 / l1_scale
    tm = scale_and_offset(tm, scale=[l1_rescale])
    # lX indicates a left path from node X
    # rX indicates a right
    # dX is just the logic for _that_ node
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
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def process(self, xr: Dataset) -> Dataset:
        xr = super().process(xr)
        output = wofs(xr).resample(time="1Y").mean().squeeze()
        output = set_stac_properties(xr, output)
        return output.to_dataset(name="mean", promote_attrs=True)


def main(
    row: Annotated[str, typer.Option(parser=int)],
    column: Annotated[str, typer.Option(parser=int)],
    datetime: Annotated[str, typer.Option()],
    version: Annotated[str, typer.Option()],
    dataset_id: str = "wofs",
) -> None:
    cell = grid.loc[[(row, column)]]

    searcher = LandsatPystacSearcher(exclude_platforms=["landsat-7"], datetime=datetime)
    stacloader = OdcLoader(
        clip_to_area=True,
        epsg=cell.crs,
        bands=["red", "green", "blue", "nir08", "swir16", "swir22", "qa_pixel"],
        dask_chunksize=dict(band=1, time=1, x=4096, y=4096),
        fail_on_error=False,
        resolution=30,
        patch_url=pc.sign,
    )
    loader = SearchLoader(searcher, stacloader)

    processor = WofsLandsatProcessor(mask_clouds_kwargs=dict(filters=[("dilation", 2)]))

    itempath = DepItemPath("ls", dataset_id, version, datetime)

    stac_creator = StacCreator(itempath, bucket="dep-cl")
    stac_writer = StacWriter(
        itempath, stac_creator, write_stac_function=write_stac_aws, bucket="dep-cl"
    )

    writer = DepWriter(
        itempath=itempath,
        pre_processor=XrPostProcessor(
            convert_to_int16=True,
            output_value_multiplier=100,
            extra_attrs=dict(dep_version=version),
        ),
        cog_writer=DsCogWriter(itempath, write_function=write_to_s3, bucket="dep-cl"),
        stac_writer=stac_writer,
        overwrite=True,
    )

    logger = CsvLogger(
        name=dataset_id,
        path=f"dep-cl/{itempath.log_path()}",
        overwrite=False,
        header="time|index|status|paths|comment\n",
        cloud_handler=S3Handler,
    )

    Task(
        id=(row, column),
        area=cell,
        loader=loader,
        processor=processor,
        writer=writer,
        logger=logger,
    ).run()


if __name__ == "__main__":
    typer.run(main)
