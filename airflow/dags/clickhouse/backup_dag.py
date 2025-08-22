import logging
from datetime import datetime
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import boto3
from dags_utils.alerting import send_alert_to_mattermost
from dags_utils.datawarehouse_connection import get_dwh_client

from airflow.decorators import dag, task
from airflow.models import Variable


# ------------------------------------------------------------------
def get_s3_bucket_and_prefix(s3_url: str):
    """
    Return (bucket_name, base_path) from an S3 URL like:
        https://s3.amazonaws.com/my-bucket/backup-s3
    """
    parsed = urlparse(s3_url)
    # parsed.netloc contains the bucket domain; the path is /my-bucket/backup-s3
    # We need to strip any leading '/' and keep the rest.
    base_path = parsed.path.lstrip("/")  # -> "my-bucket/backup-s3"
    # The actual S3 bucket name is the first component of that path.
    bucket_name, *rest = base_path.split("/", 1)
    return bucket_name, base_path


# ──────────────────────────────────────────────────────────────────────────────
#  Helper: build an S3 client (boto3)
# ──────────────────────────────────────────────────────────────────────────────
def get_s3_client():
    AWS_ACCESS_KEY_ID = Variable.get("TRACKDECHETS_CH_BACKUP_BUCKET_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = Variable.get(
        "TRACKDECHETS_CH_BACKUP_BUCKET_SECRET_ACCESS_KEY"
    )
    return boto3.client(
        "s3",
        endpoint_url="https://s3.fr-par.scw.cloud",
        region_name="fr-par",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


# ──────────────────────────────────────────────────────────────────────────────
#  DAG definition
# ──────────────────────────────────────────────────────────────────────────────
@dag(
    schedule_interval="0 7 * * *",
    catchup=False,
    start_date=datetime(2022, 8, 31),
    tags=["clickhouse", "backup"],
    on_failure_callback=send_alert_to_mattermost,
)
def backup_clickhouse_incremental():
    """
    Daily incremental backup of a ClickHouse database to S3.

    * First run → `initial/` folder (full backup).
    * Subsequent runs → `YYYYMMDD-HHMMSS/` folder (incremental),
      referencing the `initial/` folder as the base.
    """

    logger = logging.getLogger(__name__)

    @task
    def find_latest_backup_folder() -> dict[str, bool | str | None]:
        """
        Return a tuple describing where the next backup should go.

        Returns
        -------
        is_initial : bool
            True when no initial backup exists yet.
        target_prefix : str
            S3 key prefix that will receive the new backup.
        base_prefix : str | None
            Prefix of the base (initial) backup to reference, or None for a full backup.
        """
        S3_BUCKET_URL = Variable.get("TRACKDECHETS_CH_BACKUP_BUCKET_URL")
        s3 = get_s3_client()

        # Parse bucket name & path from URL
        bucket, base_path = get_s3_bucket_and_prefix(S3_BUCKET_URL)

        # Check if the *exact* prefix `initial/` exists.
        # We use a single ListObjectsV2 call with a delimiter so we only get
        # the immediate child prefixes and can stop early when we find it.
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=bucket, Prefix=f"{base_path}/", Delimiter="/"
        ):
            if "CommonPrefixes" not in page:
                continue
            for p in page["CommonPrefixes"]:
                # Trim the base path and trailing slash.
                prefix = p["Prefix"][len(base_path) :].rstrip("/")
                if prefix == "initial":
                    # initial backup already exists → incremental next.
                    target_prefix = f"{base_path}/{datetime.now(tz=ZoneInfo('UTC')).strftime('%Y%m%d-%H%M%S')}"
                    return {
                        "is_initial": False,
                        "target_prefix": target_prefix,
                        "base_prefix": f"{base_path}/initial",
                    }

        # No `initial/` found – this is the very first run.
        target_prefix = f"{base_path}/initial"
        return {"is_initial": True, "target_prefix": target_prefix, "base_prefix": None}

    @task
    def perform_backup(is_initial: bool, target_prefix: str, base_prefix: str | None):
        """
        Execute the ClickHouse BACKUP command.
        """
        S3_BUCKET_URL = Variable.get("TRACKDECHETS_CH_BACKUP_BUCKET_URL")
        AWS_ACCESS_KEY_ID = Variable.get("TRACKDECHETS_CH_BACKUP_BUCKET_ACCESS_KEY_ID")
        AWS_SECRET_ACCESS_KEY = Variable.get(
            "TRACKDECHETS_CH_BACKUP_BUCKET_SECRET_ACCESS_KEY"
        )

        client = get_dwh_client(send_receive_timeout=60 * 60 * 4)
        logger.info(f"Target S3 prefix: {target_prefix}")

        if is_initial:
            backup_sql = f"""
                BACKUP ALL EXCEPT DATABASE pg_trackdechets_gerico,
                               pg_trackdechets_production
                TO S3('{S3_BUCKET_URL}',
                      '{AWS_ACCESS_KEY_ID}',
                      '{AWS_SECRET_ACCESS_KEY}')
            """
            logger.info("Running initial full backup.")
        else:
            # Incremental – reference the base backup folder.
            backup_sql = f"""
                BACKUP ALL EXCEPT DATABASE pg_trackdechets_gerico,
                               pg_trackdechets_production
                TO S3('{target_prefix}',
                      '{AWS_ACCESS_KEY_ID}',
                      '{AWS_SECRET_ACCESS_KEY}')
                SETTINGS base_backup = S3('{base_prefix}',
                                          '{AWS_ACCESS_KEY_ID}',
                                          '{AWS_SECRET_ACCESS_KEY}')
            """
            logger.info("Running incremental backup.")

        # Run the command
        client.command(backup_sql)
        logger.info("Backup finished successfully.")

    # ------------------------------------------------------------------
    @task
    def check_aws_credentials():
        """Quick sanity‑check that we can list the bucket."""
        s3 = get_s3_client()
        bucket, _ = get_s3_bucket_and_prefix(S3_BUCKET_URL)
        try:
            s3.head_bucket(Bucket=bucket)
        except Exception as exc:
            logger.error("Cannot access S3 bucket: %s", exc)
            raise

    # ------------------------------------------------------------------
    #  DAG flow
    # ------------------------------------------------------------------
    check_aws_credentials_task = check_aws_credentials()
    find_latest_backup_folder_task = find_latest_backup_folder()

    check_aws_credentials_task >> find_latest_backup_folder_task

    is_initial = find_latest_backup_folder_task["is_initial"]
    target_prefix = find_latest_backup_folder_task["target_prefix"]
    base_prefix = find_latest_backup_folder_task["base_prefix"]
    perform_backup(is_initial, target_prefix, base_prefix)


# ──────────────────────────────────────────────────────────────────────────────
backup_clickhouse_incremental_dag = backup_clickhouse_incremental()

if __name__ == "__main__":
    backup_clickhouse_incremental_dag.test()
