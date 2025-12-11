import datetime as dt

import dagster as dg
import requests

from ....resources.chargy_config import ChargyConfig
from ....resources.s3_resources import S3Coordinate, S3Functions


@dg.asset(
    name="kml",
    key_prefix=["source", "chargy"],
    group_name="ingestion",
    kinds={"file", "s3", "source"},
    description="Raw XML file from Chargy open data, whose content changes every 5 minutes. Storing the KML file on S3 under a timestamped key.",
    owners=["xavier@reveillon.family"],
    tags={"source": "chargy", "type": "xml", "storageType": "s3", "level": "landing"},
    freshness_policy=dg.FreshnessPolicy.time_window(
        warn_window=dt.timedelta(minutes=10),
        fail_window=dt.timedelta(hours=2),
    ),
)
def kml(
    context: dg.AssetExecutionContext,
    s3_functions: S3Functions,
    chargy_config: ChargyConfig,
):
    """
    Gets KML file from Chargy opendata and uploads it to S3 under a timestamped key.
    """
    # Use an explicit UTC timestamp for filename and logs
    start_time = dt.datetime.now(dt.timezone.utc)
    context.log.debug("Starting kml_file execution.")

    context.log.debug(f"Fetching KML from URL: {chargy_config.chargy_url}")
    xml_response = requests.get(chargy_config.chargy_url)

    if xml_response.status_code != 200:
        raise dg.ValueError(
            f"Failed to fetch KML from URL {chargy_config.chargy_url}. The server returned the status {xml_response.status_code}"
        )

    content = xml_response.content

    context.log.debug(f"Fetched KML content with size {len(content)} bytes")

    filename: str = start_time.isoformat(sep="T", timespec="seconds") + ".kml"

    kmlkey: str = (
        chargy_config.kml_key_refix + start_time.strftime("%Y-%m-%d") + "/" + filename
    )

    context.log.debug(
        f"Uploading KML to S3 bucket={chargy_config.chargy_bucket} key={kmlkey}"
    )

    s3_coord = S3Coordinate(bucket=chargy_config.chargy_bucket, key=kmlkey)
    s3_functions.put_object_to_s3(
        s3_coord=s3_coord,
        body=content,
    )

    file_size = len(content)
    context.log.debug(f"Uploaded KML; file_size_b={file_size}")

    context.log.debug("Ended kml_file execution.")

    return dg.MaterializeResult(
        metadata={
            "source_url": dg.MetadataValue.url(chargy_config.chargy_url),
            "file_size_b": dg.MetadataValue.int(file_size),
            "dagster/uri": f"s3://{s3_coord['bucket']}/{s3_coord['key']}",
        },
    )


defs = dg.Definitions(assets=[kml])
