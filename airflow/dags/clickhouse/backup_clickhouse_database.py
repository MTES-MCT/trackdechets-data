import logging
from datetime import datetime

from dags_utils.datawarehouse_connection import get_dwh_client

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def get_s3_backup_folder_name() -> str:
    airflow_env = Variable.get("AIRFLOW_ENV", "dev")

    return "clickhouse" if airflow_env == "prod" else "clickhouse_dev"


def get_s3_credentials() -> tuple[str, str]:
    access_key = Variable.get("TRACKDECHETS_CH_BACKUP_BUCKET_ACCESS_KEY_ID")
    access_secret = Variable.get("TRACKDECHETS_CH_BACKUP_BUCKET_SECRET_ACCESS_KEY")

    return access_key, access_secret


@dag(
    schedule="0 7 * * *",
    catchup=False,
    start_date=datetime(2022, 8, 31),
    tags=["clickhouse", "backup"],
)
def backup_clickhouse_database():
    @task.branch
    def determine_backup_type():
        """Determine whether to create initial or incremental backup"""

        try:
            # Check if initial backup already exists in S3
            s3_hook = S3Hook(aws_conn_id="s3_connection")
            bucket_name = "data-backups"
            initial_backup_path = f"{get_s3_backup_folder_name()}/initial"

            # Try to list keys in the initial backup directory
            keys = s3_hook.list_keys(bucket_name, prefix=initial_backup_path)

            # If we get any keys, initial backup exists

            if keys and len(keys) > 0:
                return "create_incremental_backup"
            else:
                return "create_initial_backup"

        except Exception as e:
            logging.info(f"Error checking for initial backup: {e}")
            # If we can't check, default to creating initial backup
            return "create_initial_backup"

    @task
    def create_initial_backup():
        """Create initial backup to S3"""
        client = get_dwh_client(send_receive_timeout=60 * 60 * 4)

        # Get credentials from Airflow variables
        access_key, access_secret = get_s3_credentials()

        # S3 bucket details
        s3_bucket_url = f"https://data-backups.s3.fr-par.scw.cloud/{get_s3_backup_folder_name()}/initial"

        try:
            logging.info("Creating initial backup...")
            # Execute backup command
            query = f"""
            BACKUP ALL EXCEPT DATABASES pg_trackdechets_production, pg_trackdechets_vigiedechets 
            TO S3('{s3_bucket_url}', '{access_key}', '{access_secret}')
            """

            client.command(query)
            logging.info("Initial backup completed successfully")
            return "Initial backup created successfully"

        except Exception as e:
            logging.error(f"Error creating initial backup: {e}")
            raise

    @task
    def create_incremental_backup():
        """Create incremental backup to S3"""
        client = get_dwh_client(send_receive_timeout=60 * 60 * 4)

        # Get credentials from Airflow variables
        access_key, access_secret = get_s3_credentials()

        # S3 bucket details with timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        s3_bucket_url = f"https://data-backups.s3.fr-par.scw.cloud/{get_s3_backup_folder_name()}/{timestamp}"

        # Base backup location (initial backup)
        base_backup_url = f"https://data-backups.s3.fr-par.scw.cloud/{get_s3_backup_folder_name()}/initial"

        try:
            logging.info("Creating incremental backup...")
            # Execute incremental backup command
            query = f"""
            BACKUP ALL EXCEPT DATABASES pg_trackdechets_production, pg_trackdechets_vigiedechets 
            TO S3('{s3_bucket_url}', '{access_key}', '{access_secret}')
            SETTINGS base_backup = S3('{base_backup_url}', '{access_key}', '{access_secret}')
            """

            client.command(query)
            logging.info("Incremental backup completed successfully")
            return "Incremental backup created successfully"

        except Exception as e:
            logging.error(f"Error creating incremental backup: {e}")
            raise

    # Create the branch task
    branch_task = determine_backup_type()

    # Define the tasks that will be executed based on the branch
    initial_backup_task = create_initial_backup()
    incremental_backup_task = create_incremental_backup()

    # Set up the branching logic
    branch_task >> [initial_backup_task, incremental_backup_task]


backup_clickhouse_database_dag = backup_clickhouse_database()

if __name__ == "__main__":
    backup_clickhouse_database_dag.test()
