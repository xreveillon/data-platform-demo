import calendar as cal
import datetime as dt

import dagster as dg
import polars as pl

from ....cursors.cursor_data import CursorData, cursordata_from_json, cursordata_to_json
from ...automation.partitions import monthly_partition

ASSET_KEY_HISTORICAL_POSTGRES = dg.AssetKey(
    ["externalsource", "chargy", "charging_point_status_with_metadata"]
)
ASSET_KEY_PARQUET = dg.AssetKey(["bronze", "chargy", "charging_point"])

UPSTREAM_ASSET_KEYS = [ASSET_KEY_HISTORICAL_POSTGRES, ASSET_KEY_PARQUET]


@dg.asset(
    partitions_def=monthly_partition,
    name="charging_point_with_status",
    key_prefix=["silver", "chargy"],
    kinds={"iceberg", "silver"},
    owners=["xavier@reveillon.family"],
    tags={
        "source": "chargy",
        "type": "iceberg",
        "catalog": "nessie",
        "warehouse": "nessie_warehouse",
    },
    freshness_policy=dg.FreshnessPolicy.time_window(
        warn_window=dt.timedelta(days=2),
        fail_window=dt.timedelta(days=5),
    ),
    ins={
        "historical_postgres": dg.AssetIn(
            key=ASSET_KEY_HISTORICAL_POSTGRES,
            partition_mapping=dg.TimeWindowPartitionMapping(
                allow_nonexistent_upstream_partitions=True
            ),
        ),
        "parquet": dg.AssetIn(
            key=ASSET_KEY_PARQUET,
            dagster_type=pl.LazyFrame,
            partition_mapping=dg.TimeWindowPartitionMapping(
                allow_nonexistent_upstream_partitions=True
            ),
        ),
    },
    group_name="silver",
    description="All charging points readings, with description of the charging point and its status",
    io_manager_key="polarsiceberg_iomanager",
    metadata={
        "dagster/table_name": "chargy.charging_point_with_status",
        "partition_expr": "date",
    },
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    # automation_condition=dg.AutomationCondition.eager().without(
    #     dg.AutomationCondition.in_latest_time_window()
    # ),    # Creates backfill runs, and dagster-iceberg is not compatible with it (no partition_key in context)
)
def charging_point_with_status(
    context: dg.AssetExecutionContext,
    historical_postgres: pl.LazyFrame | None,
    parquet: pl.LazyFrame | None,
):
    if historical_postgres is not None:
        historical_postgres = historical_postgres.with_columns(
            pl.col("timestamp").dt.date().alias("date"),
            pl.col("id").cast(pl.Utf8).alias("id"),
            pl.col("device_id").cast(pl.Utf8).alias("device_id"),
            pl.col("connector").cast(pl.Int64).alias("connector"),
        )
        if parquet is not None:
            parquet = parquet.with_columns(
                pl.col("date").str.to_date("%Y-%m-%d").alias("date"),
                pl.col("maxchspeed").cast(pl.Decimal(38, 10)).alias("maxchspeed"),
            )
            res = pl.concat([historical_postgres, parquet], how="vertical")
            return res.collect()
        else:
            return historical_postgres.collect()
    if parquet is not None:
        return parquet.with_columns(
            pl.col("date").str.to_date("%Y-%m-%d").alias("date"),
            pl.col("maxchspeed").cast(pl.Decimal(38, 10)).alias("maxchspeed"),
        ).collect()
    return None


silver_charging_point_selection = dg.AssetSelection.assets(
    dg.AssetKey(["silver", "chargy", "charging_point_with_status"])
)

materialize_silver_charging_point_job = dg.define_asset_job(
    name="materialize_monthly_charging_point_silver",
    selection=silver_charging_point_selection,
)


@dg.sensor(
    name="trigger_silver_charging_point_sensor",
    job=materialize_silver_charging_point_job,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def trigger_silver_charging_point_sensor(context: dg.SensorEvaluationContext):
    asset_records: list[tuple[dg.EventLogRecord, dg.AssetKey]] = []

    if context.cursor:
        csr_data: CursorData = cursordata_from_json(context.cursor)
    else:
        csr_data = {"last_storage_ids": {}, "month_data": {}}

    for asset_key in UPSTREAM_ASSET_KEYS:
        asset_key_events = context.instance.get_event_records(
            event_records_filter=dg.EventRecordsFilter(
                asset_key=asset_key,
                event_type=dg.DagsterEventType.ASSET_MATERIALIZATION,
                after_cursor=csr_data["last_storage_ids"].get(asset_key, None),
            ),
            limit=62,
            ascending=True,
        )
        asset_records.extend([tuple([event, asset_key]) for event in asset_key_events])  # pyright: ignore[reportArgumentType]
        context.log.info(
            f"Discovered {len(asset_key_events)} new events for asset '{asset_key.to_user_string()}'"
        )

    if len(asset_records) == 0:
        context.log.info("No new event found")
        return

    asset_records.sort(key=lambda x: x[0].storage_id)

    last_partition_key: str = monthly_partition.get_last_partition_key()  # pyright: ignore[reportAssignmentType]

    month_dict = csr_data["month_data"]

    for tup in asset_records:
        if (
            len(tup) == 2
            and tup[0]
            and tup[0].partition_key
            and tup[0].asset_materialization
        ):
            dat = to_date(tup[0].partition_key)
            mth = to_month_key(tup[0].partition_key)
            month_dict[mth] = month_dict.get(mth, set())
            month_dict[mth].add(dat)
        csr_data["last_storage_ids"][tup[1]] = tup[0].storage_id

    mth_to_remove: set[str] = set()
    for mth, dat_set in month_dict.items():
        if len(dat_set) == nb_days_in_month(mth) or mth == last_partition_key:
            context.log.debug(f"Yielding RunRequest for partition {mth}")
            yield dg.RunRequest(partition_key=mth)
            mth_to_remove.add(mth)

    for mth in mth_to_remove:
        del month_dict[mth]

    context.update_cursor(cursordata_to_json(csr_data))


def to_month_key(s: str) -> str:
    if len(s) == 7:
        return s
    if len(s) == 10:
        d = dt.datetime.strptime(s, "%Y-%m-%d")
        return f"{d.year}-{d.month:02}"

    raise ValueError(
        f"Input was expected to be a string of 10 characters representing a date. Input: {s}"
    )


def nb_days_in_month(mth_key: str) -> int:
    mth_dat = dt.datetime.strptime(mth_key, "%Y-%m")
    return cal.monthrange(mth_dat.year, mth_dat.month)[1]


def to_date(s: str) -> dt.date:
    if len(s):
        return dt.datetime.strptime(s, "%Y-%m-%d").date()
    return dt.datetime.strptime(s, "%Y-%m").date()
