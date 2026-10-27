"""Count DEP Landsat WOfL items per year and platform, compare them to the
upstream USGS Landsat scenes they are made from, and write a markdown report
(filled in from landsat_item_tally_template.md) with plots to
landsat_item_tally/."""

from datetime import date
from pathlib import Path

import pandas as pd
import plotnine as pn
from pystac_client import Client

from dep_tools.landsat_utils import landsat_grid

PLATFORMS = ["LANDSAT_4", "LANDSAT_5", "LANDSAT_7", "LANDSAT_8", "LANDSAT_9"]


def main():
    output_dir = Path(__file__).parent / "landsat_item_tally"
    output_dir.mkdir(exist_ok=True)
    years = range(1984, date.today().year + 1)

    # Count both catalogs
    wofl = count_items_by_year_and_platform(
        "https://stac.digitalearthpacific.org", "dep_ls_wofl", years
    )
    # Same scenes processing searches for: every DEP Landsat path/row
    usgs = count_items_by_year_and_platform(
        "https://landsatlook.usgs.gov/stac-server",
        "landsat-c2l2-sr",
        years,
        pathrow_filter(landsat_grid().index.tolist()),
    )
    counts = (
        pd.merge(
            wofl,
            usgs,
            on=["year", "platform"],
            how="outer",
            suffixes=("_wofl", "_usgs"),
        )
        .fillna(0)
        .astype({"number_of_items_wofl": int, "number_of_items_usgs": int})
        .rename(
            columns={
                "number_of_items_wofl": "wofl_items",
                "number_of_items_usgs": "usgs_items",
            }
        )
        .assign(Landsat=lambda df: df.platform.str[-1:])
        .sort_values(["year", "platform"])
    )
    counts.to_csv(output_dir / "landsat_items_by_year_platform.csv", index=False)

    # Plots
    plot_catalog_comparison(counts).save(
        output_dir / "landsat_items_per_year.png", width=8, height=4, dpi=150
    )
    plot_wofl_by_platform(counts).save(
        output_dir / "landsat_items_per_year_by_platform.png",
        width=8,
        height=4,
        dpi=150,
    )

    (output_dir / "landsat_item_tally.md").write_text(make_report(counts))


def count_items_by_year_and_platform(
    catalog: str, collection: str, years, extra_filter: dict | None = None
) -> pd.DataFrame:
    client = Client.open(catalog)
    records = []
    for year in years:
        for platform in PLATFORMS:
            platform_filter = {"op": "=", "args": [{"property": "platform"}, platform]}
            search_filter = (
                {"op": "and", "args": [platform_filter, extra_filter]}
                if extra_filter
                else platform_filter
            )
            # matched() reads the total from the first page, so items are
            # never downloaded
            number_of_items = client.search(
                collections=[collection],
                datetime=str(year),
                filter=search_filter,
                filter_lang="cql2-json",
                limit=1,
            ).matched()
            records.append(
                {"year": year, "platform": platform, "number_of_items": number_of_items}
            )
    return pd.DataFrame.from_records(records).loc[lambda df: df.number_of_items > 0]


def pathrow_filter(pathrows: list[tuple[int, int]]) -> dict:
    """CQL2 filter matching any of the given WRS-2 path/rows."""
    return {
        "op": "or",
        "args": [
            {
                "op": "and",
                "args": [
                    {
                        "op": "=",
                        "args": [{"property": "landsat:wrs_path"}, f"{path:03}"],
                    },
                    {"op": "=", "args": [{"property": "landsat:wrs_row"}, f"{row:03}"]},
                ],
            }
            for path, row in pathrows
        ],
    }


def plot_catalog_comparison(counts: pd.DataFrame) -> pn.ggplot:
    by_catalog = (
        counts.groupby("year")[["usgs_items", "wofl_items"]]
        .sum()
        .reset_index()
        .melt(id_vars="year", var_name="catalog", value_name="number_of_items")
        .replace({"catalog": {"usgs_items": "USGS Landsat", "wofl_items": "DEP WOfL"}})
    )
    return (
        pn.ggplot(by_catalog, pn.aes(x="year", y="number_of_items", fill="catalog"))
        + pn.geom_bar(stat="identity", position="dodge")
        + pn.xlab("Year")
        + pn.ylab("Count of Images")
        + pn.labs(fill="Catalog")
        + pn.scale_y_continuous(expand=(0, 500))
        + pn.scale_x_continuous(expand=(0, 0.5))
        + pn.theme_bw()
    )


def plot_wofl_by_platform(counts: pd.DataFrame) -> pn.ggplot:
    return (
        pn.ggplot(counts, pn.aes(x="year", y="wofl_items", fill="Landsat"))
        + pn.geom_bar(stat="identity")
        + pn.xlab("Year")
        + pn.ylab("Count of WOfL Images")
        + pn.scale_y_continuous(expand=(0, 500))
        + pn.scale_x_continuous(expand=(0, 0.5))
        + pn.theme_bw()
    )


def make_report(counts: pd.DataFrame) -> str:
    comparison = counts.groupby("year")[["usgs_items", "wofl_items"]].sum()
    comparison.loc["Total"] = comparison.sum()
    comparison = comparison.assign(
        missing=lambda df: df.usgs_items - df.wofl_items,
        percent_processed=lambda df: (100 * df.wofl_items / df.usgs_items).round(1),
    ).rename(
        columns={
            "usgs_items": "USGS scenes",
            "wofl_items": "WOfL items",
            "missing": "Difference",
            "percent_processed": "% processed",
        }
    )

    by_platform = counts.pivot_table(
        index="year",
        columns="Landsat",
        values="wofl_items",
        aggfunc="sum",
        fill_value=0,
        margins=True,
        margins_name="Total",
    ).rename(
        columns=lambda landsat: f"Landsat {landsat}" if landsat != "Total" else landsat
    )

    template = Path(__file__).parent / "landsat_item_tally_template.md"
    return template.read_text().format(
        date=date.today().isoformat(),
        comparison_table=comparison.to_markdown(),
        platform_table=by_platform.to_markdown(),
    )


if __name__ == "__main__":
    main()
