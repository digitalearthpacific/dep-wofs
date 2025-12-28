from os.path import getmtime
from pathlib import Path

import geopandas as gpd
import pandas as pd

from sample_wofls_images import OUTPUT_DIR

VALIDATION_POINT_DIR = OUTPUT_DIR / "labeled"


def load_scene_data(blinded_path) -> pd.DataFrame:
    path_with_names = Path(str(blinded_path).replace("_blinded", ""))
    wofl_classes = gpd.read_file(path_with_names).to_crs(3832)

    lookup = pd.DataFrame(
        dict(label=["w", "l", "c", "u"], reference=["wet", "dry", "cloud", "unknown"])
    )

    validation_classes = gpd.read_file(blinded_path).to_crs(3832)
    validation_classes = pd.merge(validation_classes, lookup).drop(["label"], axis=1)

    combined_data = (
        pd.merge(wofl_classes, validation_classes, on="geometry")
        #        .drop(["geometry"], axis=1)
        .assign(
            modified_time=getmtime(blinded_path),
            id=path_with_names.stem,
            wofl=lambda r: r.name.str[:3],
        )
    )

    # These are all (I hand checked) on small islands or in Papua and are actually
    # land (GADM is wrong)
    combined_data.loc[
        (combined_data.name == "dry_ocean") & (combined_data.reference == "dry"),
        "reference",
    ] = "wet"

    return combined_data


def load_validation_data():
    return pd.concat(
        [
            load_scene_data(path)
            for path in Path(VALIDATION_POINT_DIR).glob("*blinded.gpkg")
        ]
    )
