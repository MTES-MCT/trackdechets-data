"""
Airflow DAG to run retrieve_companies job on Scalingo.

This DAG executes the retrieve_companies management command on the
Vigie Dechets app using Scalingo one-off containers.
It captures logs and handles exceptions properly.
"""

import logging
import re
import subprocess
import time

from airflow.decorators import dag, task
from airflow.models import Variable
from pendulum import datetime
from dataclasses import asdict
from dags_utils.alerting import send_alert_to_mattermost
from dags_utils.scalingo import (
    CommandConfig,
    ContainerInfo,
    ScalingoCommandError,
    login_to_scalingo,
    check_container_status,
    fetch_scalingo_logs,
)

logging.basicConfig()
logger = logging.getLogger()


@dag(
    dag_id="retrieve_companies_scalingo",
    schedule_interval="11 1 * * *",
    catchup=False,
    start_date=datetime(2024, 1, 1),
    tags=["vigiedechets", "jobs", "maps"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": True,
        "email_on_retry": False,
        "on_failure_callback": send_alert_to_mattermost,
    },
)
def retrieve_companies_scalingo():
    """
    DAG to run retrieve_companies job on Scalingo.

    This DAG executes the retrieve_companies management command on the
    trackedechets-preparation-inspection app using Scalingo one-off containers.
    It captures logs and handles exceptions properly.
    """

    @task
    def start_retrieve_companies_container() -> dict:
        """
        Starts a Scalingo one-off container in detached mode.

        This function performs the following steps:
        - Logs into Scalingo using the API token
        - Starts a configured one-off Scalingo container command in detached mode
        - Returns the container ID for monitoring

        Returns:
            ContainerInfo: ContainerInfo object containing app_name, region, and container_id.

        Raises:
            LoginError: If Scalingo login fails.
            ScalingoCommandError: If the Scalingo command fails.
            ValueError: If Scalingo API token is not set or empty.
        """
        command_config = CommandConfig(
            app_name=Variable.get("SCALINGO_VIGIEDECHETS_APP_NAME"),
            container_size="2XL",
            command="python /app/src/manage.py retrieve_companies",
        )

        scalingo_token = Variable.get("SCALINGO_API_TOKEN")
        if not scalingo_token or not scalingo_token.strip():
            raise ValueError("SCALINGO_API_TOKEN is empty or not set")

        scalingo_cmd = [
            "scalingo",
            "--region",
            command_config.region,
            "--app",
            command_config.app_name,
            "run",
            "--detached",
            "--size",
            command_config.container_size,
            command_config.command,
        ]

        logger.info(
            f"Starting Scalingo one-off container for app: {command_config.app_name}"
        )
        logger.info(f"Command: {' '.join(scalingo_cmd)}")
        logger.info(f"Container size: {command_config.container_size}")

        try:
            login_to_scalingo(scalingo_token)

            process = subprocess.run(
                scalingo_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                check=False,
            )

            output = process.stdout if process.stdout else ""
            logger.info(output)

            if process.returncode != 0:
                error_output = output[-500:] if len(output) > 500 else output
                raise ScalingoCommandError(
                    return_code=process.returncode,
                    command=scalingo_cmd,
                    error_output=error_output,
                )

            container_id_match = re.search(r"--filter (one-off-[\w-]+)", output)
            if container_id_match:
                container_id = container_id_match.group(1)
            else:
                raise Exception(
                    "Could not parse container ID from output (fallback failed)"
                )

            logger.info(f"Found container IDs: {container_id_match}")
            return asdict(
                ContainerInfo(
                    app_name=command_config.app_name,
                    region=command_config.region,
                    container_id=container_id,
                )
            )

        except subprocess.CalledProcessError as e:
            error_output = e.stderr if e.stderr else str(e)
            raise ScalingoCommandError(
                return_code=e.returncode,
                command=scalingo_cmd,
                error_output=error_output,
            ) from e
        except FileNotFoundError as e:
            error_msg = (
                "Scalingo CLI not found. Make sure it's installed and in PATH. "
                "Install with: curl -O https://cli-dl.scalingo.com/install && bash install"
            )
            raise ScalingoCommandError(
                return_code=1,
                command=scalingo_cmd,
                error_output=error_msg,
            ) from e
        except Exception as e:
            raise ScalingoCommandError(
                return_code=1,
                command=scalingo_cmd,
                error_output=str(e),
            ) from e

    @task
    def monitor_retrieve_companies_container(container_info: dict):
        """
        Monitors a Scalingo one-off container until completion.

        This function:
        - Fetches logs using `scalingo logs`
        - Waits until the container finishes
        - Raises an error if the container fails, succeeds if it completes successfully

        Args:
            container_info: Dictionary containing app_name, region, and container_id.

        Raises:
            ScalingoCommandError: If the container fails or monitoring fails.
            ValueError: If Scalingo API token is not set or empty.
        """

        container_info_obj = ContainerInfo(**container_info)
        max_wait_time = 2 * 60 * 60  # 2 hours max wait time
        check_interval = 10  # Check every 10 seconds
        start_time = time.time()

        scalingo_token = Variable.get("SCALINGO_API_TOKEN")
        if not scalingo_token or not scalingo_token.strip():
            raise ValueError("SCALINGO_API_TOKEN is empty or not set")

        try:
            login_to_scalingo(scalingo_token)

            if not container_info_obj.container_id:
                raise Exception("Container ID is not set")
            logger.info(f"Monitoring container: {container_info_obj.container_id}")
            while time.time() - start_time < max_wait_time:
                if not check_container_status(container_info_obj):
                    logger.info(
                        f"Container {container_info_obj.container_id} has finished"
                    )
                    break
                time.sleep(check_interval)

            # Check if we timed out
            if time.time() - start_time >= max_wait_time:
                raise ScalingoCommandError(
                    return_code=1,
                    command=["scalingo", "run", "--detached"],
                    error_output=f"Container {container_info_obj.container_id} did not finish within {max_wait_time} seconds",
                )

            # Fetch final logs to check exit status
            logger.info("Fetching final logs to check exit status...")
            container_logs = fetch_scalingo_logs(container_info_obj)

            success_indicators = (
                r"Successfully imported ([1-9][0-9]{0,6}|10000000) companies"
            )

            has_success = re.search(success_indicators, container_logs.lower())
            if not has_success:
                raise ScalingoCommandError(
                    return_code=1,
                    command=["scalingo", "run", "--detached"],
                    error_output=f"Container {container_info_obj.container_id} failed with no success indicators found in logs\n-----\n{container_logs}\n-----",
                )

            logger.info(
                f"SUCCESS: Container {container_info_obj.container_id} completed successfully"
            )

            # Parse and display statistics from summary line
            summary_pattern = (
                r"Summary\s*—\s*created:\s*(\d+),\s*skipped duplicates:\s*(\d+),"
                r"\s*skipped existing:\s*(\d+),\s*failed:\s*(\d+)"
            )
            summary_match = re.search(summary_pattern, container_logs, re.IGNORECASE)
            if summary_match:
                created = summary_match.group(1)
                skipped_duplicates = summary_match.group(2)
                skipped_existing = summary_match.group(3)
                failed = summary_match.group(4)
                logger.info("=" * 60)
                logger.info("RETRIEVE COMPANIES STATISTICS:")
                logger.info(f"  Created: {created}")
                logger.info(f"  Skipped duplicates: {skipped_duplicates}")
                logger.info(f"  Skipped existing: {skipped_existing}")
                logger.info(f"  Failed: {failed}")
                logger.info("=" * 60)
            else:
                logger.warning(
                    "Could not find summary statistics in logs. "
                    "Expected format: 'Summary — created: X, skipped duplicates: Y, "
                    "skipped existing: Z, failed: W'"
                )

        except subprocess.CalledProcessError as e:
            error_output = e.stderr if e.stderr else str(e)
            raise ScalingoCommandError(
                return_code=e.returncode,
                command=e.cmd,
                error_output=error_output,
            ) from e
        except FileNotFoundError as e:
            error_msg = (
                "Scalingo CLI not found. Make sure it's installed and in PATH. "
                "Install with: curl -O https://cli-dl.scalingo.com/install && bash install"
            )
            raise ScalingoCommandError(
                return_code=1,
                command=["scalingo"],
                error_output=error_msg,
            ) from e

    container_info = start_retrieve_companies_container()
    monitor_retrieve_companies_container(container_info)


retrieve_companies_dag = retrieve_companies_scalingo()

if __name__ == "__main__":
    retrieve_companies_dag.test()
