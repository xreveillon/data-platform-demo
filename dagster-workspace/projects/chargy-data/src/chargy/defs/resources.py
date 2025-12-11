import os

import dagster as dg
from dagster_aws.s3 import S3Resource
from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.io_manager.base import DbIOManagerImplementation
from dagster_iceberg.io_manager.polars import PolarsIcebergIOManager

from ..resources.chargy_config import ChargyConfig
from ..resources.iceberg_config import (
    IcebergConfigResource,
)
from ..resources.postgres_iomanagers import PostgresPolarsIOManager
from ..resources.s3_iomanagers import S3PolarsParquetIOManager, S3TargzIOManager
from ..resources.s3_resources import S3Functions


def create_environment_variables_for_pyiceberg():
    os.environ["PYICEBERG_CATALOG__NESSIE__URI"] = os.environ[
        "CHARGY_ICEBERG_CATALOG_URI"
    ]
    os.environ["PYICEBERG_CATALOG__NESSIE__S3__ENDPOINT"] = os.environ[
        "CHARGY_S3_ENDPOINT_URL"
    ]
    os.environ["PYICEBERG_CATALOG__NESSIE__S3__ACCESS_KEY_ID"] = os.environ[
        "CHARGY_S3_ACCESS_KEY_ID"
    ]
    os.environ["PYICEBERG_CATALOG__NESSIE__S3__SECRET_ACCESS_KEY"] = os.environ[
        "CHARGY_S3_SECRET_KEY"
    ]
    os.environ["PYICEBERG_CATALOG__NESSIE__WAREHOUSE"] = os.environ[
        "CHARGY_ICEBERG_WAREHOUSE"
    ]


@dg.definitions
def resources() -> dg.Definitions:
    chargy_config = ChargyConfig(
        chargy_url=dg.EnvVar("CHARGY_KML_FILE_URL"),
        chargy_bucket=dg.EnvVar("CHARGY_S3_BUCKET"),
        kml_key_refix="source/kml/",
        kml_daily_archive_key_prefix="source/archives/kml",
        bronze_prefix="bronze/chargy",
        postgresql_start_date=dg.EnvVar("CHARGY_POSTGRES_START_DATE"),
        postgresql_end_date=dg.EnvVar("CHARGY_POSTGRES_END_DATE"),
    )
    s3Resource = S3Resource(
        endpoint_url=dg.EnvVar("CHARGY_S3_ENDPOINT_URL"),
        aws_access_key_id=dg.EnvVar("CHARGY_S3_ACCESS_KEY_ID"),
        aws_secret_access_key=dg.EnvVar("CHARGY_S3_SECRET_KEY"),
    )
    s3functions = S3Functions(s3=s3Resource)

    create_environment_variables_for_pyiceberg()

    return dg.Definitions(
        resources={
            "chargy_config": chargy_config,
            "s3": s3Resource,
            "s3_functions": s3functions,
            "s3_targz_iomanager": S3TargzIOManager(
                s3_functions=s3functions,
                bucket=chargy_config.chargy_bucket,
                key_globalprefix="",
            ),
            "s3_polarsparquet_iomanager": S3PolarsParquetIOManager(
                endpoint_url=dg.EnvVar("CHARGY_S3_ENDPOINT_URL"),
                access_key_id=dg.EnvVar("CHARGY_S3_ACCESS_KEY_ID"),
                secret_access_key=dg.EnvVar("CHARGY_S3_SECRET_KEY"),
                bucket=dg.EnvVar("CHARGY_S3_BUCKET"),
            ),
            "postgrespolars_iomanager": PostgresPolarsIOManager(
                pg_host=dg.EnvVar("CHARGY_POSTGRES_SOURCE_HOST"),
                pg_database=dg.EnvVar("CHARGY_POSTGRES_SOURCE_DATABASE"),
                pg_port=dg.EnvVar.int("CHARGY_POSTGRES_SOURCE_PORT"),
                pg_username=dg.EnvVar("CHARGY_POSTGRES_SOURCE_USERNAME"),
                pg_password=dg.EnvVar("CHARGY_POSTGRES_SOURCE_PASSWORD"),
            ),
            "iceberg_config": IcebergConfigResource(
                uri=dg.EnvVar("CHARGY_ICEBERG_CATALOG_URI"),
                s3_endpoint=dg.EnvVar("CHARGY_S3_ENDPOINT_URL"),
                s3_access_key_id=dg.EnvVar("CHARGY_S3_ACCESS_KEY_ID"),
                s3_secret_access_key=dg.EnvVar("CHARGY_S3_SECRET_KEY"),
                s3_bucket=dg.EnvVar("CHARGY_ICEBERG_DATA_BUCKET"),
                iceberg_warehouse=dg.EnvVar("CHARGY_ICEBERG_WAREHOUSE"),
            ),
            "polarsiceberg_iomanager": PolarsIcebergIOManager(
                name="nessie",
                config=IcebergCatalogConfig(properties={}),
                db_io_manager=DbIOManagerImplementation.custom,
            ),
        },
    )
