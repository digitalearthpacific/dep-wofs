from typing_extensions import Annotated

from distributed import Client
from wofs.virtualproduct import scale_usgs_collection2
from wofs.wofls import woffles_usgs_c2
from odc.stac import configure_s3_access, load
from odc.stats.plugins.wofs import StatsWofs
from typer import Option, run
from xarray import Dataset

from cloud_logger import CsvLogger, S3Handler
from dep_tools.loaders import OdcLoader
from dep_tools.namers import S3ItemPath
from dep_tools.processors import LandsatProcessor, XrPostProcessor
from dep_tools.searchers import LandsatPystacSearcher, PystacSearcher
from dep_tools.task import AwsStacTask as Task
from grid import grid


SR_BANDS = ["blue", "green", "red", "nir08", "swir16", "swir22"]


class WofsLandsatProcessor(LandsatProcessor):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def process(self, ds: Dataset) -> Dataset:
        ds = super().process(ds)

        woffles = dea_wofls(ds)
        summarizer = StatsWofs()
        prepped = woffles.groupby("time").apply(
            lambda da: summarizer.native_transform(da.to_dataset(name="water"))
        )
        return summarizer.reduce(prepped)[["frequency"]]


def dea_wofls(ds: Dataset):
    ds[SR_BANDS] = scale_usgs_collection2(ds[SR_BANDS])
    # rename for wofs functionality
    ds = ds.rename(
        {
            "blue": "nbart_blue",
            "green": "nbart_green",
            "red": "nbart_red",
            "nir08": "nbart_nir",
            "swir16": "nbart_swir_1",
            "swir22": "nbart_swir_2",
            "qa_pixel": "fmask",
        }
    )
    # computing here so it doesn't need to be reloaded for each time step
    dsm = load_dsm(ds).compute()

    return ds.groupby("time").apply(
        # There should be no nodata in the cop data, but we need to think about
        # tile margins (all land should be covered) and see the odc stac issue
        # about gaps
        lambda ds_yr: woffles_usgs_c2(ds_yr.squeeze(), dsm, ignore_dsm_no_data=True)
    )


def load_dsm(like: Dataset):
    # Use this instead of just searching to be OK across -180
    items = PystacSearcher(
        catalog="https://earth-search.aws.element84.com/v1",
        collections=["cop-dem-glo-30"],
    ).search(like.odc.geobox)

    return (
        load(
            items, like=like, chunks=dict(x=like.chunks["x"][0], y=like.chunks["y"][0])
        )
        .rename(dict(data="elevation"))  # renamed for wofs functionality
        .squeeze()
        .assign_attrs(crs=like.rio.crs)  # needed for wofs functionality
    )


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
        # clip_to_area=True,
        groupby="solar_day",  # OK but we may need to revisit for clouds
        crs=cell.crs,
        dtype="uint16",
        bands=SR_BANDS + ["qa_pixel"],
        chunks=dict(band=1, time=1, x=4096, y=4096),
        fail_on_error=False,
        resolution=30,
    )

    processor = WofsLandsatProcessor(mask_clouds=False, scale_and_offset=False)
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
