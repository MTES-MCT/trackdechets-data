import argparse
import logging
from pathlib import Path
import shutil
import tempfile
from typing import Literal

import clickhouse_connect
import polars as pl
from clickhouse_connect.driver.tools import insert_file

from schemas.tables_ddl import (
    GISTRID_INSTALLATIONS_TABLE_DDL,
    GISTRID_NOTIFIANTS_TABLE_DDL,
    GISTRID_NOTIFICATIONS_TABLE_DDL,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s-%(name)s-%(asctime)s-%(message)s"
)
logger = logging.getLogger(__name__)


configs = {
    "notifications": {
        "DDL": GISTRID_NOTIFICATIONS_TABLE_DDL,
        "table_name": "raw_zone_gistrid.notifications",
        "data_conversion_expression": {
            "numéro de notification": pl.String,
            "numéro GISTRID du notifiant": pl.String,
            "numéro SIRET de l'installation de traitement": pl.String,
            "numéro GISTRID de l'installation de traitement": pl.String,
        },
    },
    "notifiants": {
        "DDL": GISTRID_NOTIFIANTS_TABLE_DDL,
        "table_name": "raw_zone_gistrid.notifiants",
        "data_conversion_expression": {
            "SIRET": pl.String,
            "Date du statut": pl.String,
            "Nombre de notifications réservées": pl.String,
            "Numéro GISTRID": pl.String,
            "N° d'enregistrement": pl.String,
        },
    },
    "installations": {
        "DDL": GISTRID_INSTALLATIONS_TABLE_DDL,
        "table_name": "raw_zone_gistrid.installations",
        "data_conversion_expression": {
            "SIRET": pl.String,
            "Date du statut": pl.String,
            "Nombre de notifications réservées": pl.String,
            "Numéro GISTRID": pl.String,
            "N° d'enregistrement": pl.String,
        },
    },
}


def load_gistrid_excel_file_to_dwh(
    file_type: Literal["notifications", "notifiants", "installations"],
    filepath: str,
    dwh_host: str,
    dwh_http_port: int,
    dwh_username: str,
    dwh_password: str,
    year: int | None,
    full_refresh: bool = False,
):
    """
    Load GISTRID data from an Excel file into a ClickHouse database warehouse (DWH).

    Parameters
    ----------
    filepath : str
        The file path to the GISTRID Excel file.
    dwh_host : str
        The hostname of the DWH server.
    dwh_http_port : str
        The HTTP port number of the DWH server.
    dwh_username : str
        The username for authenticating with the DWH.
    dwh_password : str
        The password for authenticating with the DWH.
    year : int
        The year associated with the GISTRID data.

    Returns
    -------
    None

    Notes
    -----
    This function performs the following steps:
    1. Establishes a connection to the ClickHouse database using the provided credentials and host information.
    2. Creates a table in the DWH if it does not already exist.
    3. Reads the GISTRID data from an Excel file into a Polars DataFrame.
    4. Adds a new column 'annee' with the specified year value to the DataFrame.
    5. Converts the DataFrame to CSV format and stores it in a temporary directory.
    6. Inserts the CSV data into the DWH table.
    7. Removes the temporary directory and its contents after the insertion is complete.

    """

    if file_type not in ["notifications", "notifiants", "installations"]:
        raise ValueError(
            "file_type should be one of ['notifications', 'notifiants', 'installations']"
        )

    config = configs[file_type]
    table_name = config["table_name"]

    client = clickhouse_connect.get_client(
        host=dwh_host,
        port=dwh_http_port,
        username=dwh_username,
        password=dwh_password,
    )

    logger.info(
        "Creating database raw_zone_gistrid if not exists",
    )

    client.command("CREATE DATABASE IF NOT EXISTS raw_zone_gistrid")

    if full_refresh:
        logger.info(
            f"Full refresh, removing table {table_name} if not exists",
        )
        client.command(f"DROP TABLE {table_name};")

    logger.info(
        f"Creating table {table_name} if not exists",
    )

    client.command(config["DDL"])

    logger.info(
        "Converting excel file to CSV",
    )
    tmp_dir = Path(tempfile.mkdtemp(prefix="gistrid_el"))

    if file_type == "notifications":
        gistrid_df = pl.read_excel(
            filepath,
            schema_overrides=config["data_conversion_expression"],
        )
        gistrid_df = gistrid_df.with_columns(pl.lit(int(year)).alias("annee"))
    else:
        gistrid_df = pl.read_csv(
            filepath,
            separator=";",
            schema_overrides=config["data_conversion_expression"],
        )

    gistrid_df.write_csv(tmp_dir / f"{file_type}_gistrid_df.csv", null_value="NULL")

    logger.info(
        "Starting data insertion into DWH.",
    )

    insert_file(
        client=client,
        table=config["table_name"],
        file_path=str(tmp_dir / f"{file_type}_gistrid_df.csv"),
        fmt="CSVWithNames",
        settings={"format_csv_null_representation": "NULL"},
    )

    logger.info(
        "Finished data insertion into DWH.",
    )

    logger.info(
        "Removing artifacts.",
    )
    shutil.rmtree(tmp_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="GUNLoader", description="Load GUN data to DWH"
    )

    parser.add_argument(
        "--dwh_host",
        default="localhost",
        help="Host to connect to the database. Default to localhost.",
    )
    parser.add_argument(
        "--dwh_http_port",
        default=8123,
        help="HTTP port to connect to the database. Default to 8123.",
    )
    parser.add_argument(
        "--dwh_username",
        default="default",
        help="Username to connect to the database. Default to default.",
    )
    parser.add_argument(
        "--dwh_password",
        default="",
        help="Password to connect to the database.",
    )
    parser.add_argument(
        "--year",
        default=None,
        help="Year of the notifications dataset, if applicable. Default to None.",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        default=False,
        help="If true, drop the existing table before insertion.",
    )
    parser.add_argument(
        "file_type", help="Type of the dataset. For example 'installations'."
    )
    parser.add_argument("filepath", help="Path to the Excel or CSV GISTRID dataset.")

    args = parser.parse_args()

    load_gistrid_excel_file_to_dwh(
        file_type=args.file_type,
        filepath=args.filepath,
        dwh_host=args.dwh_host,
        dwh_http_port=args.dwh_http_port,
        dwh_password=args.dwh_password,
        dwh_username=args.dwh_username,
        year=args.year,
        full_refresh=args.full_refresh,
    )
