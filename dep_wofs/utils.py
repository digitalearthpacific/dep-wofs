def parse_datetime(datetime):
    years = datetime.split("_")
    if len(years) == 2:
        years = range(int(years[0]), int(years[1]) + 1)
    elif len(years) > 2:
        ValueError(f"{datetime} is not a valid value for --datetime")
    return years


def bool_parser(raw: str):
    return False if raw == "False" else True
