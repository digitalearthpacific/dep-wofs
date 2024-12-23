from datetime import datetime
from pathlib import Path
from typing_extensions import Annotated
import warnings

import boto3
from distributed import Client
from odc.stac import configure_s3_access
import odc.stac
from pystac import ItemCollection, read_dict
import pystac_client
from typer import Option, run

from cloud_logger import CsvLogger
from dep_tools.aws import object_exists, s3_dump
from dep_tools.exceptions import EmptyCollectionError
from dep_tools.loaders import StacLoader
from dep_tools.namers import S3ItemPath
from dep_tools.processors import XrPostProcessor
from dep_tools.searchers import LandsatPystacSearcher, Searcher
from dep_tools.stac_utils import StacCreator
from dep_tools.task import AwsStacTask

from grid import ls_grid
from processors import WoflProcessor

BUCKET = "dep-public-staging"


def use_alternate_s3_href(modifiable: pystac_client.Modifiable) -> None:
    if isinstance(modifiable, dict):
        if modifiable["type"] == "FeatureCollection":
            new_features = list()
            for item_dict in modifiable["features"]:
                use_alternate_s3_href(item_dict)
                new_features.append(item_dict)
            modifiable["features"] = new_features
        else:
            stac_object = read_dict(modifiable)
            use_alternate_s3_href(stac_object)
            modifiable.update(stac_object.to_dict())
    else:
        for _, asset in modifiable.assets.items():
            asset_dict = asset.to_dict()
            if "alternate" in asset_dict.keys():
                asset.href = asset.to_dict()["alternate"]["s3"]["href"]


class MultiItemTask:
    def __init__(
        self,
        tile_id,
        items: ItemCollection,
        itempath,
        searcher,
        post_processor,
        **kwargs,
    ):
        self._tile_id = tile_id
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
            if not object_exists(
                bucket=BUCKET, key=self._itempath.stac_path(self._tile_id)
            ):
                try:
                    paths += self._task_class(
                        self._itempath,
                        id=self._tile_id,
                        searcher=self._searcher,
                        post_processor=self._post_processor,
                        **self._kwargs,
                    ).run()
                except Exception as e:
                    warnings.warn(
                        "Error from one of the dailies, check the output logs"
                    )
                    daily_log_path = Path(self._itempath.log_path()).with_suffix(
                        ".error.txt"
                    )
                    boto3_client = boto3.client("s3")
                    s3_dump(
                        data=e,
                        bucket=BUCKET,
                        key=str(daily_log_path),
                        client=boto3_client,
                    )

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


class PassThroughOdcLoader(StacLoader):
    """Just loads the items"""

    def __init__(self, **kwargs):
        self._kwargs = kwargs

    def load(self, items, _):
        return odc.stac.load(
            items,
            anchor="center",
            **self._kwargs,
        )


def main(
    path: Annotated[str, Option(parser=int)],
    row: Annotated[str, Option(parser=int)],
    datetime: Annotated[str, Option()],
    version: Annotated[str, Option()],
    dataset_id: str = "wofl",
) -> None:
    configure_s3_access(cloud_defaults=True, requester_pays=True)

    id = (path, row)
    cell = ls_grid.loc[[id]]

    itempath = S3ItemPath(
        bucket=BUCKET,
        sensor="ls",
        dataset_id=dataset_id,
        version=version,
        time=datetime,
    )

    client = pystac_client.Client.open(
        "https://landsatlook.usgs.gov/stac-server",
        modifier=use_alternate_s3_href,
    )

    searcher = LandsatPystacSearcher(
        client=client,
        query={
            "landsat:wrs_row": dict(eq=str(row).zfill(3)),
            "landsat:wrs_path": dict(eq=str(path).zfill(3)),
        },
        datetime=datetime,
        collections=["landsat-c2l2-sr"],
    )

    logger = CsvLogger(
        name=dataset_id,
        path=f"{itempath.bucket}/{itempath.log_path()}",
        overwrite=False,
        header="time|index|status|paths|comment\n",
    )

    try:
        items = searcher.search(cell)
    except EmptyCollectionError as e:
        logger.error([id, "no items found", e])
        warnings.warn("No stac items found, exiting")
        # Don't reraise, it just means there's no data
        return None

    SR_BANDS = ["blue", "green", "red", "nir08", "swir16", "swir22"]
    stacloader = PassThroughOdcLoader(
        dtype="uint16",
        bands=SR_BANDS + ["qa_pixel"],
        chunks=dict(band=1, time=1, x=4096, y=4096),
        stac_cfg={
            "landsat-c2l2-sr": {
                "assets": {"*": {"nodata": 0}, "qa_pixel": {"nodata": 1}}
            }
        },
    )

    processor = WoflProcessor()
    post_processor = DailyPostProcessor(
        convert_to_int16=False,
        output_nodata=1,
        extra_attrs=dict(dep_version=version),
    )

    daily_itempath = DailyItemPath(
        bucket=BUCKET,
        sensor="ls",
        dataset_id=dataset_id,
        version=version,
        time=None,
    )
    item_searcher = IS()

    try:
        paths = MultiItemTask(
            tile_id=id,
            items=items,
            itempath=daily_itempath,
            area=cell,
            searcher=item_searcher,
            loader=stacloader,
            processor=processor,
            post_processor=post_processor,
            logger=logger,
            stac_creator=StacCreator(daily_itempath, with_raster=True, with_eo=True),
        ).run()
    except Exception as e:
        # Quoting string here to escape newlines
        logger.error([id, "error", [], f'"{e}"'])
        raise e

    logger.info([id, "complete", paths])


if __name__ == "__main__":
    with Client():
        run(main)
