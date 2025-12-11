import datetime as dt
from pathlib import Path
from tempfile import TemporaryDirectory

import dagster as dg

from ....resources.chargy_config import ChargyConfig
from ....resources.s3_resources import S3Coordinate, S3Functions
from ...automation.partitions import kml_partition
from .kml import kml


@dg.asset(
    partitions_def=kml_partition,
    name="kml",
    key_prefix=["source", "archives"],
    kinds={"file", "s3", "tar"},
    owners=["xavier@reveillon.family"],
    tags={
        "source": "chargy",
        "type": "tar",
        "storageType": "s3",
        "level": "landing",
    },
    freshness_policy=dg.FreshnessPolicy.time_window(
        warn_window=dt.timedelta(days=2),
        fail_window=dt.timedelta(days=5),
    ),
    deps=[kml],
    group_name="ingestion",
    description="All xml files of a day are stored in a targzip archive. Daily partitioned. Stored on S3",
    io_manager_key="s3_targz_iomanager",
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=15),
)
def archive_kml(
    context: dg.AssetExecutionContext,
    s3_functions: S3Functions,
    chargy_config: ChargyConfig,
) -> Path | None:
    partition_keys = context.partition_keys

    is_data: bool = False

    kml_start_date = chargy_config.get_kml_start_date()

    with TemporaryDirectory(delete=False) as tempdir:
        res_path = Path(tempdir)
        for key in partition_keys:
            if dt.datetime.strptime(key, "%Y-%m-%d").date() < kml_start_date:
                context.log.info(
                    f"Partition {key} is too early (before {kml_start_date.strftime('%Y-%m-%d')}, there would not be any data."
                )
                continue
            kmlkey_prefix: str = chargy_config.kml_key_refix + key + "/"
            s3_coord = S3Coordinate(
                bucket=chargy_config.chargy_bucket, key=kmlkey_prefix
            )
            context.log.info("Downloading files from s3")
            part_path = Path(tempdir) / key
            part_path.mkdir(parents=True, exist_ok=True)
            nb_files = s3_functions.download_files_from_s3folder(
                s3_folder=s3_coord, local_folder=part_path
            )
            if nb_files == 0:
                part_path.rmdir()
            elif nb_files >= 1:
                is_data = True

    if not is_data:
        context.log.info("Returning empty dataset.")
        return

    return res_path


tarred_kml_job = dg.define_asset_job(
    name="tarred_kml_job",
    selection=dg.AssetSelection.assets(
        ["source", "archives", "kml"]
    ).required_multi_asset_neighbors(),
)

tarred_kml_schedule = dg.build_schedule_from_partitioned_job(
    job=tarred_kml_job,
    name="tarred_kml_schedule",
    hour_of_day=2,
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
