"""Load the hand-labelled validation points, calculate WOfL accuracy, and
write a markdown report (filled in from accuracy_report_template.md) with a
plot to accuracy_report/."""

from datetime import date
from os.path import getmtime
from pathlib import Path

import geopandas as gpd
import pandas as pd
import plotnine as pn

from create_random_samples import OUTPUT_DIR


def main():
    output_dir = Path(__file__).parent / "accuracy_report"
    output_dir.mkdir(exist_ok=True)

    all_points = load_validation_data()
    # Unknown points can't be assessed; cloud points count as WOfL errors
    points = all_points.loc[all_points.reference != "unknown"].assign(
        correct=lambda df: df.wofl == df.reference
    )
    inland_points = points.loc[points.name.str.endswith("_land")]

    export_points(points, Path(__file__).parent / "validation_data.gpkg")
    plot_accuracy_by_scene(points).save(
        output_dir / "accuracy_by_scene.png", width=8, height=4, dpi=150
    )
    (output_dir / "accuracy_report.md").write_text(
        make_report(all_points, points, inland_points)
    )


def load_validation_data() -> pd.DataFrame:
    return pd.concat(
        [
            load_scene_data(path)
            for path in (OUTPUT_DIR / "labeled").glob("*blinded.gpkg")
        ]
    )


def load_scene_data(blinded_path) -> pd.DataFrame:
    """Join the hand labels for one scene to the sampled WOfL classes."""
    path_with_names = Path(str(blinded_path).replace("_blinded", ""))
    wofl_classes = gpd.read_file(path_with_names).to_crs(3832)

    lookup = pd.DataFrame(
        dict(label=["w", "l", "c", "u"], reference=["wet", "dry", "cloud", "unknown"])
    )

    validation_classes = gpd.read_file(blinded_path).to_crs(3832)
    validation_classes = pd.merge(validation_classes, lookup).drop(["label"], axis=1)

    combined_data = pd.merge(wofl_classes, validation_classes, on="geometry").assign(
        modified_time=getmtime(blinded_path),
        id=path_with_names.stem,
        wofl=lambda r: r.name.str[:3],
    )

    # Hand checked: these are land outside the Pacific GADM data (Torres Strait
    # islands, Indonesian Papua) or small islands it misses (e.g. Kiritimati),
    # so they belong in the dry_land stratum
    combined_data.loc[
        (combined_data.name == "dry_ocean") & (combined_data.reference == "dry"),
        "name",
    ] = "dry_land"

    return combined_data


def export_points(points: pd.DataFrame, path: Path) -> None:
    """Write the assessed points, so the reference data is kept in git (the
    labelled scene files are not)."""
    gpd.GeoDataFrame(
        points.drop(columns="correct").assign(
            combined=lambda df: df.wofl + "_" + df.reference
        ),
        crs=3832,
    ).to_file(path, layer="validation_data")


def make_report(
    all_points: pd.DataFrame, points: pd.DataFrame, inland_points: pd.DataFrame
) -> str:
    class_accuracy = pd.concat(
        {
            "User's": percent_correct(points, "wofl"),
            "Producer's": percent_correct(points, "reference"),
            "Inland user's": percent_correct(inland_points, "wofl"),
            "Inland producer's": percent_correct(inland_points, "reference"),
        },
        axis=1,
    ).loc[["dry", "wet"]]

    stratum_accuracy = pd.concat(
        [
            pd.crosstab(points.name, points.reference, normalize="index")
            .mul(100)
            .round(1),
            percent_correct(points, "name").rename("user's accuracy"),
        ],
        axis=1,
    )

    template = Path(__file__).parent / "accuracy_report_template.md"
    return template.read_text().format(
        date=date.today().isoformat(),
        scene_count=all_points.id.nunique(),
        point_count=len(all_points),
        unknown_count=len(all_points) - len(points),
        labelled_table=pd.crosstab(
            all_points.name, all_points.reference, margins=True
        ).to_markdown(),
        overall_accuracy=percent(points.correct.mean()),
        inland_accuracy=percent(inland_points.correct.mean()),
        cloud_percent=percent((points.reference == "cloud").mean()),
        confusion_table=pd.crosstab(points.wofl, points.reference).to_markdown(),
        inland_confusion_table=pd.crosstab(
            inland_points.wofl, inland_points.reference
        ).to_markdown(),
        class_accuracy_table=class_accuracy.to_markdown(),
        stratum_table=stratum_accuracy.to_markdown(),
    )


def percent_correct(points: pd.DataFrame, by: str) -> pd.Series:
    """Percent of points where WOfL matches the reference, for each value of
    `by`. By WOfL class this is user's accuracy; by reference class it is
    producer's accuracy."""
    return points.groupby(by).correct.mean().mul(100).round(1)


def percent(fraction: float) -> float:
    return round(100 * fraction, 1)


def plot_accuracy_by_scene(points: pd.DataFrame) -> pn.ggplot:
    # Scenes are numbered in the order they were labelled
    by_scene = (
        points.assign(order=points.modified_time.factorize(sort=True)[0] + 1)
        .groupby("order")
        .correct.agg(["mean", "sum", "count"])
        .reset_index()
        .assign(
            cumulative_accuracy=lambda df: df["sum"].cumsum() / df["count"].cumsum()
        )
    )
    return (
        pn.ggplot(by_scene, pn.aes(x="order"))
        + pn.geom_point(pn.aes(y="mean"))
        + pn.geom_line(pn.aes(y="cumulative_accuracy"))
        + pn.xlab("Number of scenes labelled")
        + pn.ylab("Overall accuracy")
        + pn.theme_bw()
    )


if __name__ == "__main__":
    main()
