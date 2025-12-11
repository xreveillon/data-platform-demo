import itertools

import dagster as dg

from ...automation.partitions import postgres_partition

metadata_for_station_status_with_metadata = {
    "dagster/table_name": "public.station_status",
    "partition_by": "status_date",
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
    "query": """
SELECT  sts.status_date             AS "timestamp",
        stt.name,
        stt.visibility,
        stt.address,
        NULL::TEXT                  AS description,
        sts.status,
        stt.number_of_connectors    AS cpnum,
        TO_CHAR(sts.longitude, 'FM999.999999999')
            || ','
            || TO_CHAR(sts.latitude, 'FM999.999999999') AS coordinates
FROM    station_status sts
        LEFT OUTER JOIN station stt ON
                sts.latitude = stt.latitude
                AND sts.longitude = stt.longitude
                AND sts.status_date >= stt.version_start_date
                AND sts.status_date < stt.version_end_date
        """,
}


metadata_for_charging_point_status_with_metadata = {
    "dagster/table_name": "public.connector_status",
    "partition_by": "status_date",
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
                name="coordinates",
                type="string",
                description="GPS coordinates of the station",
                constraints=dg.TableColumnConstraints(
                    nullable=True,
                    unique=False,
                ),
            ),
            dg.TableColumn(
                name="station_name",
                type="string",
                description="Name of the station",
                constraints=dg.TableColumnConstraints(
                    nullable=True,
                    unique=False,
                ),
            ),
            dg.TableColumn(
                name="device_id",
                type="int",
                description="Identifier of the charging device",
                constraints=dg.TableColumnConstraints(
                    nullable=True,
                    unique=False,
                ),
            ),
            dg.TableColumn(
                name="id",
                type="int",
                description="Identifier of the charging point",
                constraints=dg.TableColumnConstraints(
                    nullable=False,
                    unique=False,
                ),
            ),
            dg.TableColumn(
                name="name",
                type="string",
                description="Name of the charging point",
                constraints=dg.TableColumnConstraints(
                    nullable=True,
                    unique=False,
                ),
            ),
            dg.TableColumn(
                name="maxchspeed",
                type="float",
                description="Electric power of the charging point, in kW",
                constraints=dg.TableColumnConstraints(
                    nullable=True,
                    unique=False,
                ),
            ),
            dg.TableColumn(
                name="connector",
                type="int",
                description="Number of the charging point on parent charging device",
                constraints=dg.TableColumnConstraints(
                    nullable=True,
                    unique=False,
                ),
            ),
            dg.TableColumn(
                name="description",
                type="string",
                description="Description of the charging point",
                constraints=dg.TableColumnConstraints(
                    nullable=True,
                    unique=False,
                ),
            ),
        ]
    ),
    "query": """
SELECT  consta.status_date          AS "timestamp",
        TO_CHAR(dev.longitude, 'FM999.999999999')
            || ','
            || TO_CHAR(dev.latitude, 'FM999.999999999') AS coordinates,
        sta."name"                  AS station_name,
        dev.id                      AS device_id,
        consta.connector_id         AS id,
        con."name"                  AS "name",
        con.maxchspeed,
        con.connector,
        consta.status               AS description
FROM    connector_status consta
        LEFT OUTER JOIN connector con ON
            consta.connector_id = con.id
            AND consta.status_date >= con.version_start_date
            AND consta.status_date < con.version_end_date
        LEFT OUTER JOIN charging_device dev ON
            con.charging_device_id = dev.id
            AND consta.status_date >= dev.version_start_date
            AND consta.status_date < dev.version_end_date
        LEFT OUTER JOIN station sta ON
            dev.latitude = sta.latitude
            AND dev.longitude = sta.longitude
            AND consta.status_date >= sta.version_start_date
            AND consta.status_date < sta.version_end_date
        """,
}

metadata_for_station_status = {
    "dagster/table_name": "public.station_status",
    "partition_by": "status_date",
    "dagster/column_schema": dg.TableSchema(
        columns=[
            dg.TableColumn(
                name="latitude",
                type="float",
                description="Latitude of the station, as part of the GPS coordinate",
            ),
            dg.TableColumn(
                name="longitude",
                type="float",
                description="longitude of the station, as part of the GPS coordinate",
            ),
            dg.TableColumn(
                name="status_date",
                type="datetime",
                description="Time of the reading",
            ),
            dg.TableColumn(
                name="status",
                type="string",
                description="Status of the station. #AVAILABLE when at least one of the charging point is available. Else #UNAVAILABLE",
            ),
        ]
    ),
}

pg_station_status = dg.AssetSpec(
    key=["externalsource", "chargy", "station_status"],
    description="List of the readings on the status of stations",
    kinds={"postgres", "table", "source"},
    tags={
        "source": "chargy",
        "type": "table",
        "storageType": "postgres",
        "level": "bronze",
    },
    group_name="bronze",
    partitions_def=postgres_partition,
    metadata=metadata_for_station_status,
).with_io_manager_key("postgrespolars_iomanager")

pg_station_status_with_metadata = dg.AssetSpec(
    key=["externalsource", "chargy", "station_status_with_metadata"],
    description="List of the readings on the status of stations",
    kinds={"postgres", "table", "source"},
    tags={
        "source": "chargy",
        "type": "table",
        "storageType": "postgres",
        "level": "bronze",
    },
    group_name="bronze",
    partitions_def=postgres_partition,
    metadata=metadata_for_station_status_with_metadata,
).with_io_manager_key("postgrespolars_iomanager")

pg_charging_point_status_with_metadata = dg.AssetSpec(
    key=["externalsource", "chargy", "charging_point_status_with_metadata"],
    description="List of the readings on the status of charging points",
    kinds={"postgres", "table", "source"},
    tags={
        "source": "chargy",
        "type": "table",
        "storageType": "postgres",
        "level": "bronze",
    },
    group_name="bronze",
    partitions_def=postgres_partition,
    metadata=metadata_for_charging_point_status_with_metadata,
).with_io_manager_key("postgrespolars_iomanager")


@dg.op(
    name="op_record_historical_postgres",
    description="Records postgres assets as materialized up until now. Should not be run in a partitioned job, but instead manually launched once",
    tags={"target": "postgres", "kind": "historical", "constraint": "once-only"},
)
def record_all_historical_partitions_for_postgres(context: dg.OpExecutionContext):
    postgres_assets = {
        dg.AssetKey(
            ["externalsource", "chargy", "station_status"]
        ): metadata_for_station_status_with_metadata,
        dg.AssetKey(
            ["externalsource", "chargy", "station_status_with_metadata"]
        ): metadata_for_station_status,
        dg.AssetKey(
            ["externalsource", "chargy", "charging_point_status_with_metadata"]
        ): metadata_for_charging_point_status_with_metadata,
    }

    count: int = 0

    partition_keys = postgres_partition.get_partition_keys()

    for (asset_key, metadata), partition_key in itertools.product(
        postgres_assets.items(), partition_keys
    ):
        context.instance.report_runless_asset_event(
            dg.AssetMaterialization(
                asset_key=asset_key,
                partition=partition_key,
                metadata=metadata,
            )
        )
        count += 1

    context.log.info(f"Successfully recorded {count} historical partitions")
    return


@dg.job(
    name="job_record_historical_postgres",
    description="One-time job to mark historical data partitions as materialized. To be run manually.",
)
def record_historical_materializations_job():
    record_all_historical_partitions_for_postgres()
