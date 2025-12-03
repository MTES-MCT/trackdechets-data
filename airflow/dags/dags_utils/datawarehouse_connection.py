from airflow.models import Connection
from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client


def get_dwh_client(
    database: str = "default", send_receive_timeout: int = 300
) -> Client:
    """
    This utility function returns a clickhouse_connect
    client that allows to interact with the Trackdéchets datawarehouse.

    Parameters
    ----------
    database: str
        Name of the database to connect to. Default is "default" database.
    send_receive_timeout: int
        Send/receive timeout for the HTTP connection in seconds.

    Returns
    -------
    Client
        clickhouse_connect client connected to the Trackdechets' datawarehouse.
    """
    DWH_CON = Connection.get_connection_from_secrets("td_datawarehouse").to_dict()
    client = get_client(
        host=DWH_CON.get("host"),
        port=DWH_CON.get("extra").get("http_port"),
        username=DWH_CON.get("login"),
        password=DWH_CON.get("password"),
        database=database,
        send_receive_timeout=send_receive_timeout,
    )

    return client
