import datetime as dt
import os

import dagster as dg

_postgres_start_date: dt.date = dt.datetime.strptime(
    os.environ["CHARGY_POSTGRES_START_DATE"], "%Y-%m-%d"
)
_postgres_end_date: dt.date = dt.datetime.strptime(
    os.environ["CHARGY_POSTGRES_END_DATE"], "%Y-%m-%d"
)
_dagster_kml_start_date: dt.date = _postgres_end_date + dt.timedelta(days=1)

postgres_partition = dg.DailyPartitionsDefinition(
    start_date=_postgres_start_date, end_date=_postgres_end_date, timezone="UTC"
)

kml_partition = dg.DailyPartitionsDefinition(
    start_date=_dagster_kml_start_date, timezone="UTC"
)

daily_partition = dg.DailyPartitionsDefinition(
    start_date=_postgres_start_date,
    timezone="UTC",
)

monthly_partition = dg.MonthlyPartitionsDefinition(
    start_date=_postgres_start_date, timezone="UTC", end_offset=1, fmt="%Y-%m"
)

__all__ = [
    "daily_partition",
    "monthly_partition",
    "postgres_partition",
    "monthly_partition",
]
