import logging
import pathlib
import tarfile
from collections.abc import Sequence
from tempfile import TemporaryDirectory, TemporaryFile, gettempdir
from typing import Optional, TypedDict

import dagster as dg
import polars as pl

from .s3_resources import S3Coordinate, S3Functions

log = logging.getLogger(__name__)

ARCHIVE_EXTENSION = ".tar.gz"
ARCHIVE_TYPE_READ = "r:gz"
ARCHIVE_TYPE_WRITE = "w:gz"


class S3TargzIOManager(dg.ConfigurableIOManager):
    """This IOManager takes an asset, being a simple path of a file or folder, and transforms it into
    a targzip archive pushed to S3.
    When persisting an asset, the asset being formatted as a pathlib.Path, this IOManager creates
    a targzip archive of the Path, and pushes it to S3 under the key
      s3://bucket/key_globalprefix/asset_key1/asset_key2/...asset_keyN[/partition_key]/{path.name}.tar.gz
    When asked to provide an asset, the IOManager will download the targzip archive(s),
    untar it(them) to a persistent directory, and give the location of that directory to
    downstream assets.


    Args:
        s3_resource (S3Functions): real resource containing the connection to the S3 storage.
        bucket (str): simply the name of the bucket to store the assets in.
        key_globalprefix(str): Key prefix to store the asset under in the bucket.
    """

    s3_functions: dg.ResourceDependency[S3Functions]
    """Resource containing the connection and client to the S3 storage."""

    bucket: str
    """Name of the bucket"""

    key_globalprefix: str
    """Key prefix under which to store the asset (tar-gzipped)"""

    tempfolder_root: str = gettempdir()
    """Folder under which to store temp folders and files, used only when providing data
    to downstream assets. Usually tempfile.gettempdir(), but keep in mind that
    this folder is in memory for some linux OSes.
    """

    def load_input(self, context: dg.InputContext) -> pathlib.Path | None:
        context.log.debug(f"Loading input for asset {context.asset_key}")
        partition_keys: Sequence[str | None] = []

        if context.has_partition_key or context.has_asset_partitions:
            context.log.debug(f"Asset {context.asset_key} has partition keys")
            partition_keys = context.asset_partition_keys

        else:
            context.log.debug(f"Asset {context.asset_key} does not have partition keys")
            partition_keys = [None]

        with TemporaryDirectory(dir=self.tempfolder_root, delete=False) as tempdir:
            tmp_path = pathlib.Path(tempdir)
            for partition_key in partition_keys:
                context.log.debug(f"Processing partition key {str(partition_key)}")
                if partition_key is not None:
                    tmp_part_path = tmp_path / partition_key
                else:
                    tmp_part_path = tmp_path
                tmp_part_path.mkdir(parents=True, exist_ok=True)

                context.log.debug(
                    f"Downloading and untarring S3 objects for partition key {str(partition_key)}"
                )
                self._download_untar_s3_objects(
                    self._get_s3_path_prefix(context.asset_key, partition_key),
                    tmp_part_path,
                )
                context.log.debug(
                    f"Finished processing partition key {str(partition_key)}"
                )

        return tmp_path

    def _get_s3_path_prefix(
        self, asset_key: dg.AssetKey, partition_key: str | None = None
    ):
        """Gives the key prefix under which to store the asset on S3 storage.
        The prefix ends with "/".

        Args:
            asset_key (AssetKey): dagster identifier of the asset. When it's a composed key,
                the members will be joined with "/"
            partition_key (optional, str): when it is provided, the partition key is added to
                to the S3 key prefix.
        """
        asset_name = "/".join(asset_key.path)
        if partition_key is not None and len(partition_key) > 0:
            path_partition_key = partition_key + "/"
        else:
            path_partition_key = ""
        key = f"{self.key_globalprefix}{asset_name}/{path_partition_key}"
        log.debug(f"Generated S3 key prefix: {key}")
        return key

    def handle_output(self, context: dg.OutputContext, obj: pathlib.Path | None):
        if obj is None:
            return

        if not obj.exists():
            raise FileNotFoundError(f"File or folder {obj} does not exist")

        if (not obj.is_file()) and not obj.is_dir():
            raise TypeError(f"Object {obj} is neither a file nor a directory")

        partition_keys: Sequence[str | None] = [None]

        if context.has_partition_key or context.has_asset_partitions:
            partition_keys = context.asset_partition_keys

        total_compressed_size_b: int = 0
        total_uncompressed_size_b: int = 0
        nb_files: int = 0
        uri: str = ""
        s3_coord: S3Coordinate | None = None

        for partition_key in partition_keys:
            context.log.debug(f"Working on partition {str(partition_key)}")
            s3_dest_key_prefix = self._get_s3_path_prefix(
                context.asset_key,
                partition_key,
            )

            if partition_key is None:
                src_path = obj
            else:
                src_path = obj / partition_key

            if not src_path.exists():
                context.log.warning(
                    f"No file or folder was found for partition key {str(partition_key)}"
                )
                continue

            s3_coord = S3Coordinate(
                bucket=self.bucket,
                key=f"{s3_dest_key_prefix}{src_path.name}{ARCHIVE_EXTENSION}",
            )
            uri = f"s3://{s3_coord['bucket']}/{s3_coord['key']}"

            stat = _compute_stat_folder(src_path)

            nb_files += stat["file_count"]
            total_uncompressed_size_b += stat.get("total_size_b", 0)

            if src_path.exists():
                with TemporaryFile() as tmpfile:
                    context.log.info(
                        f"Creating tar archive for file or folder {src_path}"
                    )
                    tmpfile.seek(0)
                    _create_targz_file(src_path, tmpfile)
                    context.log.info("Tar archive created successfully.")

                    context.log.info(f"Uploading tar archive to {uri}")
                    tmpfile.seek(0)
                    self.s3_functions.put_object_to_s3(s3_coord, tmpfile)
                    context.log.info("Tar archive uploaded successfully")

                    # Go to end of temp file
                    tmpfile.seek(0, 2)
                    total_compressed_size_b += tmpfile.tell()

        metadata = {
            "dagster/uri": dg.MetadataValue.text(uri),
            "file_count": dg.MetadataValue.int(nb_files),
            "total_uncompressed_b": dg.MetadataValue.int(total_uncompressed_size_b),
        }

        if s3_coord is not None:
            metadata["s3_key"] = dg.MetadataValue.text(s3_coord["key"])

        context.add_output_metadata(metadata)

    def _download_untar_s3_objects(self, key_prefix: str, dest_folder: pathlib.Path):
        s3_prefix = S3Coordinate(bucket=self.bucket, key=key_prefix)
        self.s3_functions.download_files_from_s3folder(
            s3_folder=s3_prefix,
            local_folder=dest_folder,
            file_extension=ARCHIVE_EXTENSION,
        )
        for archive in dest_folder.glob(f"*{ARCHIVE_EXTENSION}"):
            with tarfile.open(name=archive, mode="r:gz") as tar:
                tar.extractall(path=dest_folder, filter=tarfile.data_filter)
                tar.close()
            archive.unlink()


def _create_targz_file(source_path: pathlib.Path, tmpfile):
    """Creates a targzip archive of the file-object given.
    If it's a folder, the content of the archive will be /folder/contentOfFolder.
    If it's a file, the content of the archive will be /file.
    Removes any metadata (uid, gid, chmod, ...) from elements in the archive.

    Args:
        source_path (pathlib.Path): path of the element to create an archive from
        tmpfile: file-object location where to create the archive.
    """
    with tarfile.open(fileobj=tmpfile, mode=ARCHIVE_TYPE_WRITE) as tar_file:
        log.debug(f"Creating tar file and adding file or folder {source_path} in it")
        tar_file.add(name=source_path, arcname=source_path.name, recursive=True)
        tar_file.close()
        log.debug("Created tar file")
    return


class _StatFolderSchema(TypedDict):
    file_count: int
    total_size_b: int


def _compute_stat_folder(folder_path: pathlib.Path) -> _StatFolderSchema:
    if (not folder_path.exists()) or (not folder_path.is_dir()):
        raise ValueError(f"Invalid folder path: {folder_path}")
    file_count: int = 0
    total_size_b: int = 0
    for f in folder_path.iterdir():
        if f.is_file():
            file_count += 1
            total_size_b += f.stat().st_size
        if f.is_dir():
            tmp_stat = _compute_stat_folder(f)
            file_count += tmp_stat["file_count"]
            total_size_b += tmp_stat["total_size_b"]
    return _StatFolderSchema(file_count=file_count, total_size_b=total_size_b)


class S3PolarsParquetIOManager(dg.ConfigurableIOManager):
    endpoint_url: str
    bucket: str
    access_key_id: str
    secret_access_key: str
    default_partition_by_columnname: Optional[str] = "partition-col"

    def _get_s3_key_prefix(
        self, asset_key: dg.AssetKey, partition_key: str | None = None
    ):
        """Gives the key under which to store the asset on S3 storage under a parquet file format

        Args:
            asset_key (AssetKey): dagster identifier of the asset. When it's a composed key,
                the members will be joined with "/"
            partition_key (optional, str): when it is provided, the partition key is added to
                to the S3 key.
        """
        asset_name = "/".join(asset_key.path)
        if partition_key is not None and len(partition_key) > 0:
            path_partition_key = f"/{asset_key.path[-1]}_{partition_key}"
        else:
            path_partition_key = ""
        key = f"{asset_name}{path_partition_key}"
        log.debug(f"Generated S3 key: {key}")
        return key

    def handle_output(
        self, context: dg.OutputContext, obj: pl.DataFrame | pl.LazyFrame | None
    ):
        """With partitioned assets, the dataset will be materialized to S3,
        under the path s3://bucket/assetKeyMembersSlashJoined/partitionByFromMetadata=partitionKey/part-000.parquet
        When not partitioned, the asset goes to s3://bucket/assetKeyMembersSlashJoined.parquet
        """
        if obj is None:
            return
        lf: pl.LazyFrame
        if isinstance(obj, pl.DataFrame):
            lf = obj.lazy()
        else:
            lf = obj

        asset_key = context.asset_key
        partition_by_colname: str | None = None
        partition_keys: Sequence[str] | None = None

        if context.metadata:
            context.log.debug(f"Metadata: {context.metadata}")
            partition_by_colname = context.metadata.get(
                "partition_by", self.default_partition_by_columnname
            )
        else:
            context.log.debug("No metadata found")
            partition_by_colname = self.default_partition_by_columnname

        context.log.debug(f"Partition by column name: {partition_by_colname}")

        if context.has_partition_key or context.has_asset_partitions:
            partition_keys = context.asset_partition_keys

        if partition_by_colname and partition_keys:
            if partition_by_colname not in lf.collect_schema().names():
                if len(partition_keys) > 1:
                    raise ValueError(
                        f"This is a multi-partition runs, and the partition column'{partition_by_colname}' is missing from the data."
                    )
                context.log.debug("Adding partition key to columns of the dataframe")
                lf = lf.with_columns(
                    pl.lit(partition_keys[0]).alias(partition_by_colname)
                )

        write_options = {
            "compression": "zstd",
            "compression_level": 22,
            "storage_options": {
                "access_key_id": self.access_key_id,
                "secret_access_key": self.secret_access_key,
                "region": "us-east-1",
                "bucket": self.bucket,
                "endpoint_url": self.endpoint_url,
                "virtual_hosted_style_request": "false",
            },
        }

        s3_file_prefix = f"s3://{self.bucket}/{self._get_s3_key_prefix(asset_key)}"
        if partition_keys:
            write_options["partition_by"] = partition_by_colname
            write_options["file"] = s3_file_prefix + "/"
        else:
            write_options["file"] = f"{s3_file_prefix}.parquet"

        context.log.debug(f"Final write_options: {write_options}")

        df = lf.collect()

        nb_rows = df.height
        estimated_size_b = df.estimated_size(unit="b")

        context.add_output_metadata(
            metadata={
                "dagster/row_count": nb_rows,
                "estimated_size_b": estimated_size_b,
            }
        )

        df.write_parquet(**write_options)

    def load_input(self, context: dg.InputContext):
        asset_key = context.asset_key
        partition_by = self.default_partition_by_columnname
        if context.upstream_output and context.upstream_output.metadata:
            context.log.debug(
                "Metadata is present for upstream asset. Will look for partition_by column in it"
            )
            partition_by = context.upstream_output.metadata.get(
                "partition_by", self.default_partition_by_columnname
            )
            context.log.debug(f"Resulting partition_by column: {partition_by}")

        base_key = self._get_s3_key_prefix(asset_key)

        # Determine read path based on partitioning
        if partition_by:
            # For hive-partitioned data: point to root directory
            read_path = f"s3://{self.bucket}/{base_key}/"
        else:
            # For non-partitioned data: point to the specific parquet file
            read_path = f"s3://{self.bucket}/{base_key}.parquet"

        # Scan the parquet data
        lazy_df = pl.scan_parquet(
            read_path,
            storage_options={
                "access_key_id": self.access_key_id,
                "secret_access_key": self.secret_access_key,
                "region": "us-east-1",
                "endpoint_url": self.endpoint_url,
            },
            hive_partitioning=True if partition_by else False,
        )

        # Apply partition filtering if needed
        if context.has_asset_partitions and partition_by:
            partition_keys = context.asset_partition_keys
            lazy_df = lazy_df.filter(pl.col(partition_by).is_in(partition_keys))

        # Return based on type annotation
        if context.dagster_type.typing_type == pl.LazyFrame:
            return lazy_df
        else:
            return lazy_df.collect()


__all__ = ["S3TargzIOManager", "S3PolarsParquetIOManager"]
