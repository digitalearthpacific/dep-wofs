from stac_geoparquet.arrow import parse_stac_ndjson_to_parquet
from s3fs import S3FileSystem


def convert_stac_to_geoparquet():
    fs = S3FileSystem(anon=True)
    stac_json_paths = [
        f for f in fs.ls("s3://dep-public-data/dep_ls_wofl") if f.endswith("json")
    ]
    breakpoint()


if __name__ == "__main__":
    convert_stac_to_geoparquet()
