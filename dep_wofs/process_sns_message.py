from pystac import Item
from typer import typer

from dep_wofs.process_wofls_item import process_wofl_item


def process_sns_message(msg: str):
    message = json.loads(msg)
    if should_be_processed(message["id"]):
        stac_url = f"{message['s3_location']}/{message['landsat_product_id']}_stac.json"
        stac_item = Item.from_file(stac_url)
        process_wofl_item(stac_item)


def should_be_processed(id: str) -> bool:
    info = parse_landsat_id(id)
    levels = ["L1TP"]
    tiers = ["T1", "T2"]
    return (
        info["pathrow"] in ls_grid.index
        and info["processing_level" in levels]
        and info["collection_number"] == "02"
        and info["tier"] in tiers
    )


def parse_landsat_id(id: str) -> dict:
    # LXSS_LLLL_PPPRRR_YYYYMMDD_yyyymmdd_CC_TX
    #    0    1      2        3        4  5  6
    # Where:
    #
    # L = Landsat [0]
    # X = Sensor (“C”=OLI/TIRS combined, “O”=OLI-only, “T”=TIRS-only,
    #             “E”=ETM+, “T”=“TM, “M”=MSS) [1]
    # SS = Satellite (”07”=Landsat 7, “08”=Landsat 8) [2:4]
    # LLL = Processing correction level (L1TP/L1GT/L1GS) [5:9]
    # PPP = WRS path
    # RRR = WRS row
    # YYYYMMDD = Acquisition year, month, day
    # yyyymmdd - Processing year, month, day
    # CC = Collection number (01, 02, …)
    # TX = Collection category (“RT”=Real-Time, “T1”=Tier 1, “T2”=Tier 2)
    pieces = id.split("_")
    return dict(
        sensor=pieces[0][1],
        satellite=pieces[0][2:4],
        processing_level=pieces[1],
        pathrow=pieces[2],
        acq_date=datetime.strptime(pieces[3], "%Y%m%d"),
        processing_time=datetime.strptime(pieces[3], "%Y%m%d"),
        collection=pieces[4],
        tier=pieces[5],
    )


if __name__ == "__main__":
    typer.run(process_sns_message)
