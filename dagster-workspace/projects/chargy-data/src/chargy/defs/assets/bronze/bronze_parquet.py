import datetime as dt
from collections.abc import Sequence
from pathlib import Path

import dagster as dg

from ...automation.partitions import kml_partition
from .utils.kml_to_polars import parse_path_to_polar


def create_bronze_shared_spec(
    key: dg.AssetKey | str | Sequence[str], description: str, **kwargs
):
    params = {
        "key": key,
        "description": description,
        "kinds": {"parquet", "s3", "bronze"},
        "owners": ["xavier@reveillon.family"],
        "tags": {
            "source": "chargy",
            "type": "parquet",
            "storageType": "s3",
            "level": "bronze",
        },
        "freshness_policy": dg.FreshnessPolicy.time_window(
            warn_window=dt.timedelta(days=2),
            fail_window=dt.timedelta(days=5),
        ),
        "group_name": "bronze",
        "automation_condition": dg.AutomationCondition.eager().without(
            dg.AutomationCondition.in_latest_time_window()
        ),
        **kwargs,
    }
    return dg.AssetSpec(**params)


@dg.multi_asset(
    partitions_def=kml_partition,
    ins={
        "kml_archive": dg.AssetIn(
            key=["source", "archives", "kml"],
            partition_mapping=dg.IdentityPartitionMapping(),
        )
    },
    outs={
        "station": dg.AssetOut.from_spec(
            spec=create_bronze_shared_spec(
                key=["bronze", "chargy", "station"],
                description="Stores all stations extracted in a day to a parquet file. Daily partitioned. Stored on S3",
                metadata={
                    "partition_by": "date",
                    "dagster/column_schema": dg.TableSchema(
                        columns=[
                            dg.TableColumn(
                                name="timestamp",
                                type="datetime",
                                description="Timestamp of the reading",
                                constraints=dg.TableColumnConstraints(
                                    nullable=False,
                                    unique=False,
                                ),
                            ),
                            dg.TableColumn(
                                name="name",
                                type="string",
                                description="Name of the station",
                                constraints=dg.TableColumnConstraints(
                                    nullable=False,
                                    unique=False,
                                ),
                            ),
                            dg.TableColumn(
                                name="visibility",
                                type="boolean",
                                description="Indicates if the station should be shown on a map",
                                constraints=dg.TableColumnConstraints(
                                    nullable=True,
                                    unique=False,
                                ),
                            ),
                            dg.TableColumn(
                                name="address",
                                type="string",
                                description="Address of the station",
                                constraints=dg.TableColumnConstraints(
                                    nullable=True,
                                    unique=False,
                                ),
                            ),
                            dg.TableColumn(
                                name="description",
                                type="string",
                                description="Description of the station",
                                constraints=dg.TableColumnConstraints(
                                    nullable=True,
                                    unique=False,
                                ),
                            ),
                            dg.TableColumn(
                                name="status",
                                type="string",
                                description="Status of the station. #AVAILABLE if at least one charging point is available. #UNAVAILABLE if not.",
                                constraints=dg.TableColumnConstraints(
                                    nullable=False,
                                    unique=False,
                                ),
                            ),
                            dg.TableColumn(
                                name="cpnum",
                                type="integer",
                                description="Number of charging points in the station",
                                constraints=dg.TableColumnConstraints(
                                    nullable=True,
                                    unique=False,
                                ),
                            ),
                            dg.TableColumn(
                                name="coordinates",
                                type="string",
                                description='GPS coordinates of the station, in format "longitude,latitude"',
                                constraints=dg.TableColumnConstraints(
                                    nullable=False,
                                    unique=False,
                                ),
                            ),
                        ]
                    ),
                },
            ),
            io_manager_key="s3_polarsparquet_iomanager",
        ),
        "charging_device": dg.AssetOut.from_spec(
            spec=create_bronze_shared_spec(
                key=["bronze", "chargy", "charging_device"],
                description="Stores all charging devices extracted in a day to a parquet file. Daily partitioned. Stored on S3",
                metadata={"partition_by": "date"},
            ),
            io_manager_key="s3_polarsparquet_iomanager",
        ),
        "charging_point": dg.AssetOut.from_spec(
            spec=create_bronze_shared_spec(
                key=["bronze", "chargy", "charging_point"],
                description="Stores all charging points extracted in a day to a parquet file. Daily partitioned. Stored on S3",
                metadata={"partition_by": "date"},
            ),
            io_manager_key="s3_polarsparquet_iomanager",
        ),
    },
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=15),
)
def bronze_chargy(context: dg.AssetExecutionContext, kml_archive: Path):
    station_df, charging_device_df, charging_point_df = parse_path_to_polar(
        str(kml_archive), True, partition_column_name="date"
    )

    yield dg.Output(
        value=station_df,
        output_name="station",
    )

    yield dg.Output(
        value=charging_device_df,
        output_name="charging_device",
    )

    yield dg.Output(
        value=charging_point_df,
        output_name="charging_point",
    )
