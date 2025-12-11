import datetime as dt
import pathlib as pl
from io import BytesIO
from logging import getLogger
from os import utime
from tempfile import NamedTemporaryFile
from typing import Any, BinaryIO, NotRequired, TypedDict, Union

import dagster as dg
from dagster_aws.s3 import S3Resource

log = getLogger(__name__)


class S3Coordinate(TypedDict):
    """Class to be exchanged between S3 functions, to know exactly where to look for in S3 storage"""

    bucket: str
    """Name of the bucket (is attribute 'Bucket' in boto3)"""

    key: str
    """Key (or key prefix) in S3 (is attribute 'Key' in boto3)"""

    mtime: NotRequired[dt.datetime]
    """Last modification time (is attribute 'LastModified' in boto3)"""

    etag: NotRequired[str]
    """Hash of the object on S3. Calculated only by the S3 storage. (is attribute 'ETag' in boto3)"""

    size: NotRequired[int]
    """Size in bytes of the object as returned by S3 (is attribute 'Size' in boto3)"""


class S3Functions(dg.ConfigurableResource):
    """Overlay of S3Resource to simplify uploads and downloads of files or objects"""

    s3: dg.ResourceDependency[S3Resource]
    """S3Resource used as connection to the S3 storage"""

    def _get_client(self) -> Any:
        return self.s3.get_client()

    def _list_s3_folder_content(
        self, prefix: S3Coordinate
    ) -> tuple[list[S3Coordinate], list[str]]:
        """Lists only one level of "files" and "folders" below the given prefix"""
        client = self._get_client()

        prefix = prefix.copy()

        if not prefix["key"].endswith("/"):
            prefix["key"] += "/"
        log.debug(f"Listing S3 folder content for prefix: {prefix['key']}")
        res_file_coords: list[S3Coordinate] = []
        res_folder_keys: list[str] = []

        paginator = client.get_paginator("list_objects_v2")

        for page in paginator.paginate(
            Bucket=prefix["bucket"],
            Prefix=prefix["key"],
            Delimiter="/",
        ):
            log.debug(f"Contents of page: {page}")
            if page is not None and "Contents" in page:
                log.debug(f"Found {len(page['Contents'])} files")
                res_file_coords.extend(
                    S3Coordinate(
                        key=obj["Key"],
                        mtime=obj["LastModified"],
                        bucket=prefix["bucket"],
                        etag=obj["ETag"],
                    )
                    for obj in page["Contents"]
                )

            if page is not None and "CommonPrefixes" in page:
                log.debug(f"Found {len(page['CommonPrefixes'])} folders")
                res_folder_keys.extend(
                    str(prefix["Prefix"]) for prefix in page["CommonPrefixes"]
                )

        return res_file_coords, res_folder_keys

    def download_files_from_s3folder(
        self,
        s3_folder: S3Coordinate,
        local_folder: pl.Path,
        file_extension: str | None = None,
    ):
        """Downloads files from the S3 storage below the given prefix, stores them in defined local_folder.

        Args
            s3_folder (S3Coordinate): bucket and key_prefix where to look for files
            local_folder (pathlib.Path): local folder where to store downloaded files
            file_extension (optional, str): filter the downloads to get only files with given extension (eg: "xml")
        """
        file_list = self._list_s3_folder_content(s3_folder)[0]
        log.debug(
            f"Downloading {len(file_list)} files from S3 folder {s3_folder['key']}"
        )
        local_folder.mkdir(parents=True, exist_ok=True)
        client = self._get_client()
        nb_files: int = 0
        for file_coord in file_list:
            if file_extension is None or file_coord["key"].endswith(file_extension):
                file_name = pl.Path(file_coord["key"]).name
                local_path = f"{local_folder}/{file_name}"
                log.debug(
                    f"Downloading file {file_coord['key']} from S3 to {local_path}"
                )
                client.download_file(
                    file_coord["bucket"], file_coord["key"], local_path
                )
                log.debug("Downloaded file")
                mtime = file_coord.get("mtime")
                if mtime is not None:
                    utime(local_path, (mtime.timestamp(), mtime.timestamp()))
                nb_files += 1
        return nb_files

    def upload_files_to_s3folder(
        self,
        s3_folder: S3Coordinate,
        local_folder: pl.Path,
        file_extension: str | None = None,
    ):
        client = self._get_client()
        if not local_folder.exists():
            raise FileNotFoundError(f"Local folder {local_folder} does not exist")
        pattern = f"*.{file_extension}" if file_extension else "*"
        for file_path in local_folder.glob(pattern):
            if file_path.is_file():
                file_key = f"{s3_folder['key']}/{file_path.relative_to(local_folder)}"
                client.upload_file(file_path, s3_folder["bucket"], file_key)

    def upload_file_to_s3(self, s3_key: S3Coordinate, local_path: pl.Path):
        client = self._get_client()
        if not local_path.exists():
            raise FileNotFoundError(f"Local folder {local_path} does not exist")
        if local_path.is_file():
            client.upload_file(local_path, s3_key["bucket"], s3_key["key"])

    def put_object_to_s3(self, s3_coord: S3Coordinate, body: Union[bytes, BinaryIO]):
        client = self._get_client()
        client.put_object(Body=body, Bucket=s3_coord["bucket"], Key=s3_coord["key"])

    def get_object_from_s3(
        self, s3_coord: S3Coordinate, max_memory: int | None = None
    ) -> Union[pl.Path, BinaryIO]:
        client = self._get_client()
        response = client.get_object(Bucket=s3_coord["bucket"], Key=s3_coord["key"])
        if "size" in s3_coord:
            file_size = s3_coord["size"]
        else:
            file_size = int(response["ContentLength"])

        streaming_body = response["Body"]

        if max_memory is not None and file_size > max_memory:
            with NamedTemporaryFile(delete=False) as temp_file:
                for chunk in streaming_body.iter_chunks(chunk_size=1024 * 1024):
                    temp_file.write(chunk)
                temp_file.flush()
                return pl.Path(temp_file.name)
        else:
            return BytesIO(streaming_body.read())
