import logging
import subprocess
from dataclasses import dataclass

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


def login_to_scalingo(scalingo_token: str):
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


def fetch_scalingo_logs(container_info: ContainerInfo) -> str:
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


def check_container_status(container_info: ContainerInfo) -> bool:
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
