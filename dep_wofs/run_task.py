from typing_extensions import Annotated

from distributed import Client
from odc.stac import configure_s3_access
import planetary_computer as pc
from typer import Option, run
from xarray import DataArray, Dataset

from cloud_logger import CsvLogger, S3Handler
from dep_tools.loaders import OdcLoader
from dep_tools.namers import S3ItemPath
from dep_tools.processors import LandsatProcessor, XrPostProcessor
from dep_tools.searchers import LandsatPystacSearcher
from dep_tools.task import AwsStacTask as Task
from dep_tools.utils import scale_and_offset
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
        return output.to_dataset(name="mean", promote_attrs=True)


def bool_parser(raw: str):
    return False if raw == "False" else True


def main(
    row: Annotated[str, Option(parser=int)],
    column: Annotated[str, Option(parser=int)],
    datetime: Annotated[str, Option()],
    version: Annotated[str, Option()],
    dataset_id: str = "wofs",
    setup_auth: Annotated[str, Option(parser=bool_parser)] = "False",
) -> None:

    if setup_auth:
        import boto3
        from aiobotocore.session import AioSession

        boto3.setup_default_session(profile_name="dep-staging-admin")
        handler_kwargs = dict(session=AioSession(profile="dep-staging-admin"))
    else:
        handler_kwargs = dict()

    configure_s3_access(cloud_defaults=True, requester_pays=True)
    cell = grid.loc[[(row, column)]]

    itempath = S3ItemPath(
        bucket="dep-public-staging",
        sensor="ls",
        dataset_id=dataset_id,
        version=version,
        time=datetime,
    )

    searcher = LandsatPystacSearcher(
        catalog="https://earth-search.aws.element84.com/v1",
        exclude_platforms=["landsat-7"],
        datetime=datetime,
    )

    stacloader = OdcLoader(
        clip_to_area=True,
        epsg=cell.crs,
        dtype="float32",
        bands=["red", "green", "blue", "nir08", "swir16", "swir22", "qa_pixel"],
        chunks=dict(band=1, time=1, x=4096, y=4096),
        fail_on_error=False,
        resolution=30,
        #        patch_url=pc.sign,
    )

    processor = WofsLandsatProcessor(mask_clouds_kwargs=dict(filters=[("dilation", 2)]))
    post_processor = XrPostProcessor(
        convert_to_int16=True,
        output_value_multiplier=100,
        extra_attrs=dict(dep_version=version),
    )

    logger = CsvLogger(
        name=dataset_id,
        path=f"{itempath.bucket}/{itempath.log_path()}",
        overwrite=False,
        header="time|index|status|paths|comment\n",
        cloud_handler=S3Handler,
        **handler_kwargs,
    )

    id = (row, column)

    try:
        paths = Task(
            itempath=itempath,
            id=id,
            area=cell,
            searcher=searcher,
            loader=stacloader,
            processor=processor,
            post_processor=post_processor,
            logger=logger,
        ).run()
    except Exception as e:
        logger.error([id, "error", e])
        raise e

    logger.info([id, "complete", paths])


if __name__ == "__main__":
    with Client():
        run(main)
