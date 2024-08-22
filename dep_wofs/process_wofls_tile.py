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
from dep_tools.searchers import LandsatPystacSearcher
from dep_tools.task import AwsStacTask

from grid import ls_grid
from dep_wofs import WoflProcessor


class MultiItemTask:
    def __init__(self, items: ItemCollection, itempath, searcher, **kwargs):
        self._items = items
        self._itempath = itempath
        self._searcher = IS()
        self._kwargs = kwargs
        self._task_class = AwsStacTask

    def run(self):
        paths = []
        for item in self._items:
            # This could be faster if we just send the item to the loader
            # but I think we'd need to hack the searcher
            # self._searcher._kwargs["datetime"] = self._itempath.time
            self._itempath.time = item.get_datetime()
            self._searcher.item = item
            paths += self._task_class(
                self._itempath, searcher=self._searcher, **self._kwargs
            ).run()
        return paths


class IS:
    def search(self, area):
        return [self.item]


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
    cell = ls_grid.loc[[(path, row)]]

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
    items = searcher.search(cell)

    SR_BANDS = ["blue", "green", "red", "nir08", "swir16", "swir22"]
    stacloader = OdcLoader(
        groupby="solar_day",
        dtype="uint16",
        bands=SR_BANDS + ["qa_pixel"],
        chunks=dict(band=1, time=1, x=4096, y=4096),
        fail_on_error=False,
        resolution=30,
    )

    processor = WoflProcessor()
    post_processor = XrPostProcessor(
        convert_to_int16=True,
        output_value_multiplier=1,
        # not positive what this should be. Official value is 1 but output is
        # bit based
        output_nodata=-999,
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

    daily_itempath = DailyItemPath(
        bucket="dep-public-staging",
        sensor="ls",
        dataset_id=dataset_id,
        version=version,
        time=None,
    )

    id = (path, row)
    try:
        paths = MultiItemTask(
            id=id,
            items=items,
            itempath=daily_itempath,
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
