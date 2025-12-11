import datetime as dt
from typing import Any, Callable, overload
from urllib.parse import quote

import dagster as dg
import polars as pl
from pydantic import Field


class PostgresPolarsIOManager(dg.ConfigurableIOManager):
    """IO manager to load data from a PostgreSQL database, based on the metadata of the asset.
    The IO manager stores the configuration for the connection, and expects the asset metadata to know what to query.
    metadata fields:
        dagster/table_name: table where to get the data from. May include the schema (schema.table). Mandatory.
        dagster/column_schema: list of columns to query. Optional. If not provided, will collect all columns (SELECT *)
        partition_by: column on which the partition of the asset is based. The IO manager will combine this with the partition(s) provided by dagster
        query: a query can be provided, and in that case dagster/table_name is not mandatory. In case of partitioned asset, partition_by is still mandatory.
    """

    pg_host: str | None = Field(
        default=None, description="DNS or IP of the postgres server"
    )
    pg_port: int | None = Field(
        default=5432, description="Port to connect to. Defaults to 5432"
    )
    pg_database: str | None = Field(
        default="postgres", description="Postgres database. Defaults to postgres"
    )
    pg_username: str | None = Field(default=None, description="User to connect with")
    pg_password: str | None = Field(default=None, description="Password of the user")
    pg_default_schema: str | None = Field(
        default=None,
        description="Default schema to use in the absence of the schema in the metadata 'dagster/table_name'. Defaults to none, meaning using the one defined for the user in the database",
    )
    pg_connection_uri: str | None = Field(
        default=None,
        description="Connection uri for postgres, in format accepted by polars.",
    )

    def load_input(self, context: dg.InputContext) -> pl.LazyFrame | None:
        if not context.metadata:
            context.log.warning(
                "No metadata directly in context. Will look for it in context.upstream_output.metadata."
            )
            if not context.upstream_output or not context.upstream_output.metadata:
                raise ValueError("No asset metadata was provided")
            else:
                metadata = context.upstream_output.metadata
        else:
            metadata = context.metadata

        if (
            context.has_partition_key or context.has_asset_partitions
        ) and "partition_by" not in metadata:
            raise ValueError(
                '"partition_by" is mandatory in asset metadata when partitioning is enabled'
            )
        if "dagster/table_name" not in metadata and "query" not in metadata:
            raise ValueError(
                'At least one of "dagster/table_name" and "query" must be in the metadata of the asset'
            )

        table_name = str(metadata.get("dagster/table_name"))
        schema: str | None = self.pg_default_schema
        if "." in table_name:
            schema = ""
        columns: dg.TableSchema | None = metadata.get("dagster/column_schema", None)

        partition_by: str | None = metadata.get("partition_by", None)
        customized_args: dict[str, Any] = {}
        if context.has_partition_key or context.has_asset_partitions:
            customized_args["partition_by"] = partition_by
            partition_keys = context.asset_partition_keys
            time_window: dg.TimeWindow | None = None

            try:
                time_window = context.asset_partitions_time_window
                context.log.debug(
                    f"The partitions of the asset {context.asset_key.to_user_string()} are of type TimeWindow"
                )
                customized_args["partition_time_start"] = time_window.start
                customized_args["partition_time_end"] = time_window.end
            except KeyError:
                context.log.debug(
                    f"The partitions of the asset {context.asset_key.to_user_string()} are not of type TimeWindow"
                )
                if len(partition_keys) > 1:
                    raise ValueError(
                        f"This IO manager does not support multi-partitions runs that are not based on time-windows. Asset-key: {context.asset_key.to_user_string()}"
                    )
                customized_args["partition_key"] = partition_keys[0]

        query = metadata.get("query", None)

        uri = self._build_connection_uri()

        if query is not None:
            where_clause = _build_where_clause(columns=columns, **customized_args)
            query = f"{query}\n{where_clause}"
        else:
            query = _build_query(
                table=table_name,
                schema_name=schema,
                columns=columns,
                **customized_args,
            )
        context.log.debug(f"Query launched against PostgreSQL: {query}")
        df = pl.read_database_uri(uri=uri, query=query)

        if df.is_empty():
            return
        return df.lazy()

    def _build_connection_uri(self, hide_password: bool = False) -> str:
        if hide_password:
            return f"postgresql://{quote(str(self.pg_username))}:*****@{self.pg_host}:{self.pg_port}/{self.pg_database}"
        return f"postgresql://{quote(str(self.pg_username))}:{quote(str(self.pg_password))}@{self.pg_host}:{self.pg_port}/{self.pg_database}"

    def handle_output(self, context: dg.OutputContext, obj):
        raise ValueError(
            "Function for persisting data is not implemented in PostgresPolarsIOManager yet."
        )


# For querying over ONE partition
@overload
def _build_query(
    table: str,
    partition_by: str,
    partition_key: str,
    *,
    columns: dg.TableSchema | None = None,
    schema_name: str | None = None,
) -> str: ...


# For querying over a time-window partition
@overload
def _build_query(
    table: str,
    partition_by: str,
    *,
    partition_time_start: dt.datetime,
    partition_time_end: dt.datetime,
    columns: dg.TableSchema | None = None,
    schema_name: str | None = None,
) -> str: ...


# For querying without partition
@overload
def _build_query(
    table: str,
    *,
    columns: dg.TableSchema | None = None,
    schema_name: str | None = None,
) -> str: ...


def _build_query(
    table: str,
    partition_by: str | None = None,
    partition_key: str | None = None,
    *,
    partition_time_start: dt.datetime | None = None,
    partition_time_end: dt.datetime | None = None,
    columns: dg.TableSchema | None = None,
    schema_name: str | None = None,
) -> str:
    select_clause: str = "SELECT *\n"
    if columns is not None and columns.columns and len(columns.columns) > 0:
        column_list = ", ".join(col.name for col in columns.columns)
        select_clause = f"SELECT {column_list}\n"

    if schema_name is not None and len(schema_name) > 0:
        schema_dot = schema_name + "."
    else:
        schema_dot = ""
    from_clause = f"FROM {schema_dot}{table}\n"

    if partition_by is not None and len(partition_by) > 0:
        if (
            partition_key is None
            and partition_time_start is None
            and partition_time_end is None
        ):
            raise ValueError(
                "Partition_key is provided, either partition_key or both partition_time_start and partition_time_end must be provided"
            )
        elif partition_key is None and (
            partition_time_start is None or partition_time_end is None
        ):
            raise ValueError(
                "Both of partition_time_start and partition_time_end must be provided"
            )

    where_clause = ""
    if partition_by is not None and len(partition_by) > 0:
        if partition_time_start is not None:
            where_clause = f"WHERE {partition_by} >= '{partition_time_start.isoformat()}' AND {partition_by} < '{partition_time_end.isoformat()}'"  # pyright: ignore[reportOptionalMemberAccess]
        elif partition_key is not None:
            column_type = "string"
            if columns is not None and columns.columns:
                column_type = _look_for_column_type(partition_by, columns)

            where_clause = f"WHERE {partition_by} = {_sql_quote_for_type(partition_key, column_type)}\n"

        else:
            raise ValueError("This error should not even be reached!")

    return f"{select_clause}{from_clause}{where_clause}"


def _build_where_clause(
    partition_by: str,
    partition_key: str | None = None,
    partition_time_start: dt.datetime | None = None,
    partition_time_end: dt.datetime | None = None,
    columns: dg.TableSchema | None = None,
) -> str:
    where_clause = ""
    if partition_by is not None and len(partition_by) > 0:
        if (
            partition_key is None
            and partition_time_start is None
            and partition_time_end is None
        ):
            raise ValueError(
                "partition_key, partition_time_start, partition_time_end cannot all be empty when partition_by is provided"
            )
        if partition_time_start is not None and partition_time_end is not None:
            where_clause = f"WHERE {partition_by} >= '{partition_time_start.isoformat()}' AND {partition_by} < '{partition_time_end.isoformat()}'"
        elif partition_key is not None:
            column_type = "string"
            if columns is not None and columns.columns:
                column_type = _look_for_column_type(partition_by, columns)

            where_clause = f"WHERE {partition_by} = {_sql_quote_for_type(partition_key, column_type)}\n"
    return where_clause


def _look_for_column_type(column_name: str, column_list: dg.TableSchema):
    for col in column_list.columns:
        if col.name == column_name:
            return str(col.type)


def _sql_quote_for_type(value: Any, column_type: str | None = None) -> str:
    def single_quote(s: str, /) -> str:
        return f"'{s}'"

    if column_type is None:
        return single_quote(str(value))

    def nothing(s: str, /) -> str:
        return s

    def datetime_with_tz(s: str | dt.datetime, /) -> str:
        s_str = s if isinstance(s, str) else s.isoformat()
        return f"{single_quote(s_str)}::TIMESTAMP WITH TIME ZONE"

    def convert_to_date(s: str | dt.date | dt.datetime) -> str:
        if isinstance(s, dt.datetime):
            s_str = str(s.date())
        else:
            s_str = str(s)
        return f"{single_quote(s_str)}::DATE"

    sqlencode: dict[str, Callable[[Any], str]] = {
        "string": single_quote,
        "numeric": nothing,
        "number": nothing,
        "date": convert_to_date,
        "datetime": single_quote,
        "datetimetz": datetime_with_tz,
    }

    return sqlencode.get(column_type, single_quote)(value)
