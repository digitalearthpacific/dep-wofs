from typing_extensions import Annotated

from distributed import Client
from wofs.virtualproduct import WOfSClassifier
from odc.geo.geobox import GeoBox
from odc.geo.geom import Geometry
from odc.stac import configure_s3_access, load
from odc.stats.plugins.wofs import StatsWofs
from typer import Option, run
from xarray import Dataset

from cloud_logger import CsvLogger, S3Handler
from dep_grid import gadm_union
from dep_tools.loaders import OdcLoader
from dep_tools.namers import S3ItemPath
from dep_tools.processors import Processor, XrPostProcessor
from dep_tools.searchers import LandsatPystacSearcher, PystacSearcher
from dep_tools.task import AwsStacTask as Task
from grid import grid


SR_BANDS = ["blue", "green", "red", "nir08", "swir16", "swir22"]


class WofsLandsatProcessor(Processor):
    def process(self, ds: Dataset, area=None) -> Dataset:
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
        ).assign_attrs(
            crs=ds.rio.crs
        )  # <- might need to start doing this on load
        woffles = DepWOfSClassifier(c2_scaling=True).compute(ds)

        summarizer = StatsWofs()
        prepped = woffles.groupby("time").apply(
            lambda ds: summarizer.native_transform(ds)
        )
        output = summarizer.reduce(prepped)[["frequency"]]
        if area is not None:
            land_mask = area.clip(gadm_union.to_crs(area.crs))
            geom = Geometry(land_mask.geometry.unary_union, crs=area.crs)
            output["frequency_masked"] = output.frequency.odc.mask(geom)
        return output


class DepWOfSClassifier(WOfSClassifier):
    def __init__(self, **kwargs):
        # Placeholder needed here so _load_dsm is called
        super().__init__(dsm_path="this_is_a_placeholder", **kwargs)

    def _load_dsm(self, gbox):
        # This comes in as a datacube.utils.geometry._base.GeoBox, which fails
        # instance type tests downstream in odc.stac.load (also in dep_tools.utils).
        realgeobox = GeoBox(gbox.shape, gbox.affine, gbox.crs)

        # Use this instead of just searching to be OK across -180
        items = PystacSearcher(
            catalog="https://earth-search.aws.element84.com/v1",
            collections=["cop-dem-glo-30"],
        ).search(realgeobox)

        return (
            load(items, geobox=realgeobox)
            .rename(dict(data="elevation"))  # renamed for wofs functionality
            .squeeze()
            .assign_attrs(crs=realgeobox.crs)
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

    processor = WofsLandsatProcessor(send_area_to_processor=True)
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
