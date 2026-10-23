"""Processors that make WOfLs from Landsat data and summarise them into WOfS."""

from odc.geo.geobox import GeoBox
from odc.geo.geom import Geometry, unary_intersection
from odc.stac import load
from odc.stats.plugins.wofs import StatsWofs, StatsWofsFullHistory
from wofs.virtualproduct import WOfSClassifier
from xarray import DataArray, Dataset

from dep_tools.grids import gadm
from dep_tools.processors import Processor
from dep_tools.searchers import PystacSearcher


def wofl(ls_c2_ds: Dataset) -> Dataset:
    """Produce WOFL data for a single landsat collection-2 level-2 scene.

    Args:
        ls_c2_ds:  An :class:`xarray.Dataset` containing landsat data, with
        variables "blue", "green", "red", "nir08", "swir16", "swir22" and
        "qa_pixel" corresponding to the associated landsat bands.

    Returns:
        WOFL.

    """
    return DepWOfSClassifier().compute(ls_c2_ds)


def wofs(wofls: Dataset, mask_ocean: bool = True) -> Dataset:
    """Produce WOfS summary data from WOFL data.

    Args:
        wofls: A time series of WOFL data, indexed by a "time" coordinate.
        mask_ocean: Whether to add a `frequency_masked` band, where pixels
            outside GADM land are set to NaN. Needs `wofls` to have a CRS.

    Returns: A Dataset with "count_wet", "count_clear" and "frequency"
        variables, plus "frequency_masked" if `mask_ocean` is True.
    """
    return WofsProcessor(mask_ocean=mask_ocean).process(wofls)


class WofsFullHistoryProcessor(Processor):
    """Summarise annual WOfS summaries into an all-time summary.

    A wrapper around `odc.stats.plugins.wofs.StatsWofsFullHistory`. Like
    :class:`WofsProcessor`, it can add a ``frequency_masked`` band with ocean
    masked out.

    Args:
        mask_ocean: Whether to add a ``frequency_masked`` band, where pixels
            outside GADM land are set to NaN.
    """

    def __init__(self, mask_ocean: bool = True):
        super().__init__()
        self.mask_ocean = mask_ocean

    def process(self, wofs_annuals: Dataset) -> Dataset:
        """Summarise annual WOfS summaries.

        Args:
            wofs_annuals: Annual summaries with ``count_clear`` and ``count_wet``
                variables, indexed by a "time" coordinate.

        Returns:
            A Dataset with ``count_wet``, ``count_clear`` and ``frequency`` variables,
            plus ``frequency_masked`` if ``mask_ocean`` is True.
        """
        output = StatsWofsFullHistory().reduce(wofs_annuals)
        if self.mask_ocean:
            output["frequency_masked"] = mask_to_land(output.frequency)
        return output


class WofsProcessor(Processor):
    """A wrapper around `odc.stats.plugins.wofs.StatsWofs`. Accepts a time
    series of WOfS feature layers indexed by a "time" coordinate. In addition
    to "count_wet", "count_clear" and "frequency" variables, optionally creates
    a version of frequency with ocean masked out. In the DEP workflow, this is
    used because ocean waters are poorly classified by the WOfS algorithm.

    Args:
        mask_ocean: Whether to add a ``frequency_masked`` band, where pixels
            outside GADM land are set to NaN.
    """

    def __init__(self, mask_ocean: bool = True):
        super().__init__()
        self.mask_ocean = mask_ocean

    def process(self, wofls: Dataset) -> Dataset:
        """Summarise a time series of WOfLs.

        Args:
            wofls: WOfLs indexed by a "time" coordinate.

        Returns:
            A Dataset with ``count_wet``, ``count_clear`` and ``frequency`` variables,
            plus ``frequency_masked`` if ``mask_ocean`` is True.
        """
        summarizer = StatsWofs()
        prepped = wofls.groupby("time").apply(
            lambda ds: summarizer.native_transform(ds)
        )
        output = summarizer.reduce(prepped)
        if self.mask_ocean:
            output["frequency_masked"] = mask_to_land(output.frequency)
        return output


class DepWOfSClassifier(WOfSClassifier, Processor):
    """A wrapper around wofs.virtualproduct.WOfSClassifier. Allows the use of
    input data with band names "blue", "green", "red", "nir08", "swir16",
    "swir22" and "qa_pixel", rather than "nbart_blue", "nbart_green",
    "nbart_red", "nbart_nir", "nbart_swir_1", "nbart_swir_2", and "fmask".
    Also uses the Copernicus 30-meter dem loaded from the stac catalog rather
    than a datacube loaded DEM.
    """

    def __init__(self, **kwargs):
        """Set up the classifier for Landsat Collection 2 data.

        Args:
            **kwargs: Passed on to `wofs.virtualproduct.WOfSClassifier`.
        """
        # Placeholder needed here so _load_dsm is called
        super().__init__(c2_scaling=True, dsm_path="this_is_a_placeholder", **kwargs)
        self._dsm = None

    def compute(self, data) -> Dataset:
        """Rename bands to the names WOfSClassifier expects, then classify.

        Args:
            data: A Landsat scene with bands named as in :class:`DepWOfSClassifier`.

        Returns:
            A Dataset with a ``water`` variable, where 1 means no data.
        """
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
        output = super().compute(data)
        output.water.attrs["nodata"] = 1
        return output

    def process(self, input_data):
        """Same as :meth:`compute`, for use as a dep_tools Processor."""
        return self.compute(input_data)

    def _load_dsm(self, gbox):
        """Load the Copernicus 30 m DEM for the given area.

        The DEM is kept and reused while the area stays the same, since one area can
        have many scenes.

        Args:
            gbox: The area, as a datacube GeoBox.

        Returns:
            A Dataset with an ``elevation`` variable.
        """
        # This comes in as a datacube.utils.geometry._base.GeoBox, which fails
        # instance type tests downstream in odc.stac.load (also in dep_tools.utils).
        realgeobox = GeoBox(gbox.shape, gbox.affine, gbox.crs)
        # cache dsm for multiple dates in the same aoi.
        if self._dsm is None or (self._realgeobox and (self._realgeobox != realgeobox)):
            self._realgeobox = realgeobox

            # Use PystacSearcher instead of just searching to be OK across -180
            items = PystacSearcher(
                # Note that this server can get overloaded and refuse connections
                # when running with many parallel pods (say 100+)
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


def mask_to_land(data: DataArray) -> DataArray:
    """Set pixels outside GADM land to NaN. Pixels touching the coastline are kept.

    Args:
        data: Data with a CRS, so that its GeoBox is known.

    Returns:
        The masked data.
    """
    geobox = data.odc.geobox
    land = unary_intersection(
        [
            geobox.boundingbox.polygon,
            Geometry(gadm().to_crs(geobox.crs).geometry.unary_union, crs=geobox.crs),
        ]
    )
    return data.odc.mask(land)
