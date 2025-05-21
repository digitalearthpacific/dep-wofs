from pystac import read_dict
import pystac_client


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


def parse_datetime(datetime):
    years = datetime.split("_")
    if len(years) == 2:
        years = range(int(years[0]), int(years[1]) + 1)
    elif len(years) > 2:
        ValueError(f"{datetime} is not a valid value for --datetime")
    return years


def bool_parser(raw: str):
    return False if raw == "False" else True
