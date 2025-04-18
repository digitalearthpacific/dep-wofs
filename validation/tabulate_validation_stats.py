from pathlib import Path

import geopandas as gpd
import pandas as pd

VALIDATION_POINT_DIR = "."


def load_scene_data(blinded_path) -> pd.DataFrame:
    path_with_names = str(blinded_path).replace("_blinded", "")
    wofl_classes = gpd.read_file(path_with_names)

    lookup = pd.DataFrame(
        dict(label=["w", "l", "c", "u"], user=["wet", "dry", "cloud", "unknown"])
    )

    validation_classes = gpd.read_file(blinded_path)
    validation_classes = pd.merge(validation_classes, lookup).drop(["label"], axis=1)

    return pd.merge(wofl_classes, validation_classes, on="geometry").drop(
        ["geometry"], axis=1
    )


def main():
    validation_data = pd.concat(
        [
            load_scene_data(path)
            for path in Path(VALIDATION_POINT_DIR).glob("*blinded.gpkg")
        ]
    )
    breakpoint()


if __name__ == "__main__":
    main()
