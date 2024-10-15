from odc.geo.geobox import GeoBox
from odc.geo.geom import Geometry, unary_intersection, multipolygon
from odc.stac import load
from odc.stats.plugins.wofs import StatsWofs
from wofs.virtualproduct import WOfSClassifier
from xarray import Dataset

from dep_tools.processors import Processor
from dep_tools.searchers import PystacSearcher
from dep_wofs.grid import GADM


def wofl(ls_c2_ds: Dataset) -> Dataset:
    return WoflProcessor().process(ls_c2_ds)


def wofs(wofls: Dataset, mask=None) -> Dataset:
    return WofsProcessor().process(wofls, mask)


class WoflProcessor(Processor):
    """A tiny wrapper around `DepWOfSClassifier.compute` which allows conformance
    to the DEP scaling abstractions in dep-tools.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Init this separately to accommodate dsm caching
        self.classifier = DepWOfSClassifier()

    def process(self, ls_c2_ds):
        output = self.classifier.compute(ls_c2_ds)
        output.water.attrs["nodata"] = 1
        return output


class WofsProcessor(Processor):
    """A wrapper around `odc.stats.plugins.wofs.StatsWofs`. Accepts a time
    series of WOfS feature layers indexed by a "time" coordinate. In addition
    to "count_wet", "count_clear" and "frequency" variables, optionally creates
    a masked version of frequency if a mask is provided. Useful for additional
    area filtering. In the DEP workflow, it is used to mask out ocean waters
    which are poorly classified by the WOfS algorithm.
    """

    def process(self, wofls: Dataset, area=None) -> Dataset:
        summarizer = StatsWofs()
        prepped = wofls.groupby("time").apply(
            lambda ds: summarizer.native_transform(ds)
        )
        output = summarizer.reduce(prepped)
        if area is not None:

            geom = unary_intersection(
                [
                    area.boundingbox.polygon,
                    Geometry(GADM.to_crs(area.crs).geometry.unary_union, crs=area.crs),
                ]
            )
            # land_mask = area.clip(GADM.to_crs(area.crs))
            # geom = Geometry(land_mask.geometry.unary_union, crs=area.crs)
            output["frequency_masked"] = output.frequency.odc.mask(geom)
        return output


# Eventually we will create wofs from wofl not from scratch, but we need
# the stac catalog, etc. set up
class WoflWofsProcessor(Processor):
    def process(self, ls_c2_ds, area):
        return wofs(wofl(ls_c2_ds), area)


class DepWOfSClassifier(WOfSClassifier):
    """A wrapper around wofs.virtualproduct.WOfSClassifier. Allows the use of
    input data with band names "blue", "green", "red", "nir08", "swir16",
    "swir22" and "qa_pixel", rather than "nbart_blue", "nbart_green",
    "nbart_red", "nbart_nir", "nbart_swir_1", "nbart_swir_2", and "fmask".
    Also uses the Copernicus 30-meter dem loaded from the stac catalog rather
    than a datacube loaded DEM.
    """

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
        # This comes in as a datacube.utils.geometry._base.GeoBox, which fails
        # instance type tests downstream in odc.stac.load (also in dep_tools.utils).
        realgeobox = GeoBox(gbox.shape, gbox.affine, gbox.crs)
        # cache dsm for multiple dates in the same aoi.
        if self._dsm is None or (self._realgeobox and (self._realgeobox != realgeobox)):
            print("calculating dsm")
            self._realgeobox = realgeobox

            # Use this instead of just searching to be OK across -180
            items = PystacSearcher(
                catalog="https://earth-search.aws.element84.com/v1",
                collections=["cop-dem-glo-30"],
            ).search(self._realgeobox)

            self._dsm = (
                load(items, geobox=self._realgeobox)
                .rename(dict(data="elevation"))  # renamed for wofs functionality
                .squeeze()
                .assign_attrs(crs=self._realgeobox.crs)
                .persist()
            )
        return self._dsm
