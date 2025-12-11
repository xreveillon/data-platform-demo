from typing import Optional

import dagster as dg
from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.io_manager.base import DbIOManagerImplementation
from dagster_iceberg.io_manager.polars import PolarsIcebergIOManager
from pydantic import Field


class IcebergConfigResource(dg.ConfigurableResource):
    type: str = Field(
        default="rest", description="Type (in sense of PyIceberg) of Iceberg catalog"
    )
    uri: str = Field(description="URI of the Iceberg catalog")
    iceberg_warehouse: str = Field(description="Warehouse inside the catalog")
    s3_endpoint: Optional[str] = Field(
        default=None,
        description="In case of on-premise object store, URL of the object store",
    )
    s3_access_key_id: Optional[str] = Field(
        default=None, description="Access Key for the object store"
    )
    s3_secret_access_key: Optional[str] = Field(
        default=None, description="Secret key for the object store"
    )
    s3_bucket: Optional[str] = Field(
        default=None, description="Bukcet where the iceberg data will be stored"
    )


class ConfigurableIcebergIOManager(PolarsIcebergIOManager):
    iceberg_config: dg.ResourceDependency[IcebergConfigResource]

    def __init__(self, **data):
        # Extract the iceberg_config before calling super().__init__
        iceberg_config_resource = data.pop("iceberg_config")

        # Build the catalog config from your resource
        catalog_config = IcebergCatalogConfig(
            properties={
                "type": iceberg_config_resource.type,
                "uri": iceberg_config_resource.uri,
                "warehouse": iceberg_config_resource.iceberg_warehouse,
                "s3.endpoint": iceberg_config_resource.s3_endpoint,
                "s3.access-key-id": iceberg_config_resource.s3_access_key_id,
                "s3.secret-access-key": iceberg_config_resource.s3_secret_access_key,
            }
        )

        # Initialize the parent with the catalog config
        super().__init__(
            name="nessie",
            config=catalog_config,
            namespace=data.get("namespace", "default"),
            db_io_manager=DbIOManagerImplementation.custom,
            **data,
        )
