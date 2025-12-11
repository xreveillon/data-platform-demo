import dagster as dg

my_custom_automation_condition_sensor = dg.AutomationConditionSensorDefinition(
    name="default_automation_condition_sensor",
    target=dg.AssetSelection.all(),
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30,
)
