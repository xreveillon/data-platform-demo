import dagster as dg
from pyiceberg.catalog import load_catalog

from ...resources.iceberg_config import IcebergConfigResource


@dg.op(description="Creates required namespaces/schemas in the iceberg catalog")
def create_needed_namespaces_op(
    context: dg.OpExecutionContext, iceberg_config: IcebergConfigResource
) -> None:
    properties = {
        "type": iceberg_config.type,
        "uri": iceberg_config.uri,
        "warehouse": iceberg_config.iceberg_warehouse,
    }
    if iceberg_config.s3_endpoint:
        properties["s3.endpoint"] = iceberg_config.s3_endpoint
    if iceberg_config.s3_access_key_id:
        properties["s3.access-key-id"] = iceberg_config.s3_access_key_id
    if iceberg_config.s3_secret_access_key:
        properties["s3.secret-access-key"] = iceberg_config.s3_secret_access_key

    catalog = load_catalog("nessie-catalog", **properties)

    namespaces_to_create = {"chargy"}

    for ns in namespaces_to_create:
        try:
            catalog.create_namespace_if_not_exists(ns)
        except Exception as e:
            context.log.error(f"Failed to create namespace {ns}: {e}")


@dg.job(description="Creates required namespaces/schemas in the iceberg catalog")
def iceberg_create_needed_namespaces() -> None:
    create_needed_namespaces_op()
