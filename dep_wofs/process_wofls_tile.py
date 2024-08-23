from datetime import datetime
from typing_extensions import Annotated

from distributed import Client
from odc.stac import configure_s3_access
from pystac import ItemCollection
from typer import Option, run

from cloud_logger import CsvLogger, S3Handler
from dep_tools.loaders import OdcLoader
from dep_tools.namers import S3ItemPath
from dep_tools.processors import XrPostProcessor
from dep_tools.searchers import LandsatPystacSearcher, Searcher
from dep_tools.task import AwsStacTask

from grid import ls_grid
from dep_wofs import WoflProcessor


class MultiItemTask:
    def __init__(
        self, items: ItemCollection, itempath, searcher, post_processor, **kwargs
    ):
        self._items = items
        self._itempath = itempath
        self._searcher = searcher
        self._post_processor = post_processor
        self._kwargs = kwargs
        self._task_class = AwsStacTask

    def run(self):
        paths = []
        for item in self._items:
            self._itempath.time = item.get_datetime()
            self._searcher.item = item
            self._post_processor.properties = item.properties
            paths += self._task_class(
                self._itempath,
                searcher=self._searcher,
                post_processor=self._post_processor,
                **self._kwargs,
            ).run()
        return paths


class IS(Searcher):
    def search(self, area):
        return [self.item]


class DailyPostProcessor(XrPostProcessor):
    """An XrPostProcessor which adds whatever properties are available at
    self.properties to the stac properties. In this workflow, used to add the
    stac item info to the output."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.properties = None

    def process(self, ds):
        ds = super().process(ds)
        if isinstance(self.properties, dict):
            ds.attrs["stac_properties"] = {
                **ds.attrs["stac_properties"],
                **self.properties,
            }
            ds.attrs["stac_properties"]["start_datetime"] = ds.attrs["stac_properties"][
                "datetime"
            ]
            ds.attrs["stac_properties"]["end_datetime"] = ds.attrs["stac_properties"][
                "datetime"
            ]
        return ds


class DailyItemPath(S3ItemPath):
    def __init__(self, time: datetime | None = None, **kwargs):
        super().__init__(time=time, **kwargs)

    def _folder(self, item_id) -> str:
        return f"{self._folder_prefix}/{self._format_item_id(item_id)}/{self.time:%Y/%m/%d}"

    def basename(self, item_id) -> str:
        return f"{self.item_prefix}_{self._format_item_id(item_id, join_str='_')}_{self.time:%Y-%m-%d}"


def bool_parser(raw: str):
    return False if raw == "False" else True


def main(
    path: Annotated[str, Option(parser=int)],
    row: Annotated[str, Option(parser=int)],
    datetime: Annotated[str, Option()],
    version: Annotated[str, Option()],
    dataset_id: str = "wofl",
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
    id = (path, row)
    cell = ls_grid.loc[[id]]

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
        query={
            "landsat:wrs_row": dict(eq=str(row).zfill(3)),
            "landsat:wrs_path": dict(eq=str(path).zfill(3)),
        },
        datetime=datetime,
    )

    logger = CsvLogger(
        name=dataset_id,
        path=f"{itempath.bucket}/{itempath.log_path()}",
        overwrite=False,
        header="time|index|status|paths|comment\n",
        cloud_handler=S3Handler,
        **handler_kwargs,
    )

    try:
        items = searcher.search(cell)
    except Exception as e:
        logger.error([id, "error", e])
        raise e

    SR_BANDS = ["blue", "green", "red", "nir08", "swir16", "swir22"]
    stacloader = OdcLoader(
        dtype="uint16",
        bands=SR_BANDS + ["qa_pixel"],
        chunks=dict(band=1, time=1, x=4096, y=4096),
        fail_on_error=False,
    )

    processor = WoflProcessor()
    post_processor = DailyPostProcessor(
        convert_to_int16=True,
        output_value_multiplier=1,
        output_nodata=1,
        extra_attrs=dict(dep_version=version),
    )

    daily_itempath = DailyItemPath(
        bucket="dep-public-staging",
        sensor="ls",
        dataset_id=dataset_id,
        version=version,
        time=None,
    )
    item_searcher = IS()

    try:
        paths = MultiItemTask(
            id=id,
            items=items,
            itempath=daily_itempath,
            area=cell,
            searcher=item_searcher,
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
