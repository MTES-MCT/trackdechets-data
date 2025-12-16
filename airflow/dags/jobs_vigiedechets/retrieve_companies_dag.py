"""
Airflow DAG to run retrieve_companies job on Scalingo.

This DAG executes the retrieve_companies management command on the
trackedechets-preparation-inspection app using Scalingo one-off containers.
It captures logs and handles exceptions properly.
"""

import logging
import re
import subprocess
import time
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from pendulum import datetime
from dataclasses import dataclass, asdict
from dags_utils.alerting import send_alert_to_mattermost

logging.basicConfig()
logger = logging.getLogger()


class ScalingoCommandError(Exception):
    """
    Exception raised when the Scalingo command fails.
    """

    def __init__(self, return_code, command, error_output):
        self.return_code = return_code
        self.command = command
        self.error_output = error_output

    def __str__(self):
        return f"Scalingo command failed with return code {self.return_code}.\nError output: {self.error_output}"


class LoginError(ScalingoCommandError):
    """
    Exception raised when login to Scalingo fails.
    """

    def __str__(self):
        return f"Scalingo login failed with return code {self.return_code}.\n Error output: {self.error_output}"


@dataclass
class CommandConfig:
    app_name: str
    container_size: str
    command: str
    region: str = "osc-secnum-fr1"


@dataclass
class ContainerInfo:
    app_name: str
    region: str
    container_id: str


@dag(
    dag_id="retrieve_companies_scalingo",
    schedule_interval="11 1 * * *",  # Daily at 1:11 AM (matches cron schedule)
    catchup=False,
    start_date=datetime(2024, 1, 1),
    tags=["vigiedechets", "jobs", "maps"],
    on_failure_callback=send_alert_to_mattermost,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
    },
)
def retrieve_companies_scalingo():
    """
    DAG to run retrieve_companies job on Scalingo.

    This DAG executes the retrieve_companies management command on the
    trackedechets-preparation-inspection app using Scalingo one-off containers.
    It captures logs and handles exceptions properly.
    """

    def _login_to_scalingo(scalingo_token: str):
        """
        Helper function to login to Scalingo.

        Args:
            scalingo_token: The Scalingo API token.

        Raises:
            LoginError: If Scalingo login fails.
        """
        login_cmd = [
            "scalingo",
            "login",
            "--api-token",
            scalingo_token,
        ]

        logger.info("Logging in to Scalingo...")
        try:
            login_process = subprocess.run(
                login_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                check=True,
            )

            if login_process.stdout:
                logger.info(login_process.stdout)
            if login_process.stderr:
                logger.info(login_process.stderr)

            logger.info("Successfully logged in to Scalingo")
        except subprocess.CalledProcessError as e:
            error_output = e.stderr if e.stderr else (e.stdout if e.stdout else str(e))
            raise LoginError(
                return_code=e.returncode,
                command=login_cmd,
                error_output=error_output,
            ) from e

    @task
    def start_scalingo_container() -> dict:
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
            _login_to_scalingo(scalingo_token)

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

            # Extract the container id (e.g., "one-off-9328") from the output string if typical parsing fails
            # Look for container_id=value pattern in the text for fallback extraction
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

    def _fetch_scalingo_logs(container_info: ContainerInfo) -> str:
        """
        Fetches logs from a Scalingo one-off container.

        Args:
            container_info: ContainerInfo object containing app_name, region, and container_id.

        Returns:
            str: Logs from the container.

        Raises:
            ScalingoCommandError: If fetching logs fails.
        """
        # Fetch and log new log entries
        logs_cmd = [
            "scalingo",
            "--region",
            container_info.region,
            "--app",
            container_info.app_name,
            "logs",
            "--filter",
            f"{container_info.container_id}",
            "--lines",
            "1000",
        ]

        try:
            logs_process = subprocess.run(
                logs_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                check=True,
            )

            logs_output = logs_process.stdout
            return logs_output
        except subprocess.CalledProcessError as e:
            error_output = e.stderr if e.stderr else str(e)
            raise ScalingoCommandError(
                return_code=e.returncode,
                command=logs_cmd,
                error_output=f"Failed to fetch logs for container {container_info.container_id}: {error_output}",
            ) from e
        except FileNotFoundError as e:
            error_msg = (
                "Scalingo CLI not found. Make sure it's installed and in PATH. "
                "Install with: curl -O https://cli-dl.scalingo.com/install && bash install"
            )
            raise ScalingoCommandError(
                return_code=1,
                command=logs_cmd,
                error_output=error_msg,
            ) from e

    def _check_container_status(container_info: ContainerInfo) -> bool:
        """
        Checks if a Scalingo one-off container is still running.

        Args:
            container_info: ContainerInfo object containing app_name, region, and container_id.

        Returns:
            bool: True if container is running, False otherwise.

        Raises:
            ScalingoCommandError: If checking container status fails.
        """
        ps_cmd = [
            "scalingo",
            "--region",
            container_info.region,
            "--app",
            container_info.app_name,
            "ps",
        ]

        try:
            ps_process = subprocess.run(
                ps_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                check=True,
            )

            ps_output = ps_process.stdout
            logger.debug(f"Container status check: {ps_output}")
            return container_info.container_id in ps_output
        except subprocess.CalledProcessError as e:
            error_output = e.stderr if e.stderr else str(e)
            raise ScalingoCommandError(
                return_code=e.returncode,
                command=ps_cmd,
                error_output=f"Failed to check status for container {container_info.container_id}: {error_output}",
            ) from e
        except FileNotFoundError as e:
            error_msg = (
                "Scalingo CLI not found. Make sure it's installed and in PATH. "
                "Install with: curl -O https://cli-dl.scalingo.com/install && bash install"
            )
            raise ScalingoCommandError(
                return_code=1,
                command=ps_cmd,
                error_output=error_msg,
            ) from e

    @task
    def monitor_scalingo_container(container_info: dict):
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
        max_wait_time = 3600  # 1 hour max wait time
        check_interval = 10  # Check every 10 seconds
        start_time = time.time()

        scalingo_token = Variable.get("SCALINGO_API_TOKEN")
        if not scalingo_token or not scalingo_token.strip():
            raise ValueError("SCALINGO_API_TOKEN is empty or not set")

        try:
            _login_to_scalingo(scalingo_token)

            if not container_info_obj.container_id:
                raise Exception("Container ID is not set")
            logger.info(f"Monitoring container: {container_info_obj.container_id}")
            while time.time() - start_time < max_wait_time:
                if not _check_container_status(container_info_obj):
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
            container_logs = _fetch_scalingo_logs(container_info_obj)

            success_indicators = (
                r"Successfully imported ([1-9][0-9]{0,6}|10000000) companies"
            )

            has_success = re.search(success_indicators, container_logs.lower())
            if not has_success:
                raise ScalingoCommandError(
                    return_code=1,
                    command=["scalingo", "run", "--detached"],
                    error_output=f"Container {container_info_obj.container_id} failed with no success indicators found in logs",
                )

            logger.info(
                f"SUCCESS: Container {container_info_obj.container_id} completed successfully"
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

    container_info = start_scalingo_container()
    monitor_scalingo_container(container_info)


retrieve_companies_dag = retrieve_companies_scalingo()

if __name__ == "__main__":
    retrieve_companies_dag.test()
