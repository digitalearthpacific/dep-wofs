from typing import Callable

from odc.geo.geobox import GeoBox
from odc.geo.geom import Geometry
from odc.stac import load
from odc.stats.plugins.wofs import StatsWofs
from wofs.virtualproduct import WOfSClassifier
from xarray import Dataset

from dep_grid import gadm_union
from dep_tools.processors import Processor
from dep_tools.searchers import PystacSearcher


def wofls(ls_c2_ds: Dataset) -> Dataset:
    return WoflProcessor().process(ls_c2_ds)


def wofs(wofls: Dataset, mask=None) -> Dataset:
    return WofsProcessor().process(wofls, mask)


class WoflProcessor(Processor):
    def __init__(self):
        self.classifier = DepWOfSClassifier()

    def process(self, ls_c2_ds):
        return self.classifier.compute(ls_c2_ds)


class WofsProcessor(Processor):
    def process(self, wofls: Dataset, area=None) -> Dataset:
        summarizer = StatsWofs()
        prepped = wofls.groupby("time").apply(
            lambda ds: summarizer.native_transform(ds)
        )
        output = summarizer.reduce(prepped)
        if area is not None:
            land_mask = area.clip(gadm_union.to_crs(area.crs))
            geom = Geometry(land_mask.geometry.unary_union, crs=area.crs)
            output["frequency_masked"] = output.frequency.odc.mask(geom)
        return output


# Eventually we will create wofs from wofl not from scratch, but we need
# the stac catalog, etc. set up
class WoflWofsProcessor(Processor):
    def process(self, ls_c2_ds, area):
        return wofs(wofls(ls_c2_ds), area)


class DepWOfSClassifier(WOfSClassifier):
    def __init__(self, **kwargs):
        # Placeholder needed here so _load_dsm is called
        super().__init__(c2_scaling=True, dsm_path="this_is_a_placeholder", **kwargs)
        self._dsm = None

    def compute(self, data) -> Dataset:
        data = data.rename(
            {
                "blue": "nbart_blue",
                "green": "nbart_green",
                "red": "nbart_red",
                "nir08": "nbart_nir",
                "swir16": "nbart_swir_1",
                "swir22": "nbart_swir_2",
                "qa_pixel": "fmask",
            }
        ).assign_attrs(crs=data.rio.crs)
        return super().compute(data)

    def _load_dsm(self, gbox):
        # cache dsm for multiple dates in the same aoi. after testing, should
        # test if gbox is the same too.
        if self._dsm is None:
            # This comes in as a datacube.utils.geometry._base.GeoBox, which fails
            # instance type tests downstream in odc.stac.load (also in dep_tools.utils).
            realgeobox = GeoBox(gbox.shape, gbox.affine, gbox.crs)

            # Use this instead of just searching to be OK across -180
            items = PystacSearcher(
                catalog="https://earth-search.aws.element84.com/v1",
                collections=["cop-dem-glo-30"],
            ).search(realgeobox)

            self._dsm = (
                load(items, geobox=realgeobox)
                .rename(dict(data="elevation"))  # renamed for wofs functionality
                .squeeze()
                .assign_attrs(crs=realgeobox.crs)
                .compute()
            )
        return self._dsm
