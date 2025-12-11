"""
chargy.defs.automation.schedules

This module defines the 5-minute partitioned schedule
that targets the `kml_file` asset and imports the shared `partition5min`
TimeWindowPartitionsDefinition from the sibling `partition` module.
"""

import dagster as dg

from ..assets.source.kml import kml

schedule5min = dg.ScheduleDefinition(
    cron_schedule="*/5 * * * *",
    name="schedule5min",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
    target=[kml],
    description="Runs every 5 minutes",
)


__all__ = ["schedule5min"]
