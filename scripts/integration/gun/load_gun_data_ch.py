import argparse
import logging
from pathlib import Path
import shutil
import tempfile

import clickhouse_connect
import polars as pl
from clickhouse_connect.driver.tools import insert_file

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s-%(name)s-%(asctime)s-%(message)s"
)
logger = logging.getLogger(__name__)


GUN_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS raw_zone_icpe.installations_rubriques_2024 (
	"Raison sociale/nom" Nullable(String),
	"SIRET" Nullable(String),
	"Code AIOT" String,
	"X" Nullable(Int256),
	"Y" Nullable(Int256),
	"Etat du site (code)" Nullable(String),
	"Etat du site (libellé)" Nullable(String),
	"Numéro rubrique" Nullable(String),
	"Régime" Nullable(String),
	"Quantité projet" Nullable(Float32),
	"Quantité totale" Nullable(Float32),
	"Capacité Projet" Nullable(Float32),
	"Capacité Totale" Nullable(Float32),
	"Unité" Nullable(String),
	"Etat technique de la rubrique" Nullable(String),
	"Etat administratif de la rubrique" Nullable(String),
    "Adresse partie 1" Nullable(String),
    "Adresse partie 2" Nullable(String),
    "Code postal" Nullable(String),
    "Commune" Nullable(String),
    "Coordonnée X" Nullable(Int64),
    "Coordonnée Y" Nullable(Int64),
    "Date de l'archivage de l'AIOT" Nullable(Date32),
	_inserted_at DateTime64(9, 'Europe/Paris') DEFAULT now('Europe/Paris')
)
ENGINE = MergeTree()
ORDER BY ()
"""


def load_gun_csv_file_to_dwh(
    filepath: str,
    dwh_host: str,
    dwh_http_port: str,
    dwh_username: str,
    dwh_password: str,
):
    client = clickhouse_connect.get_client(
        host=dwh_host,
        port=dwh_http_port,
        username=dwh_username,
        password=dwh_password,
        database="raw_zone_icpe",
    )

    logger.info(
        "Creating table raw_zone_icpe.installations_rubriques_2024 if not exists",
    )

    client.command(GUN_TABLE_DDL)

    tmp_dir = Path(tempfile.mkdtemp(prefix="gun_el"))

    gun_df = pl.read_csv(
        filepath,
        null_values="NULL",
        schema_overrides={
            "Code AIOT": pl.String,
            "SIRET": pl.String,
            "Code postal": pl.String,
        },
    )

    if "Capacité projet" in gun_df.columns:
        logging.info("Renaming 'Capacité projet' to 'Capacité Projet'")
        gun_df = gun_df.rename(
            {
                "Capacité projet": "Capacité Projet",
            }
        )
    if "Capacité totale" in gun_df.columns:
        logging.info("Renaming 'Capacité totale' to 'Capacité Totale'")
        gun_df = gun_df.rename({"Capacité totale": "Capacité Totale"})

    gun_df.write_csv(tmp_dir / "gun.csv", null_value="NULL")

    logger.info(
        "Starting data insertion into DWH.",
    )

    insert_file(
        client=client,
        table="raw_zone_icpe.installations_rubriques_2024",
        file_path=str(tmp_dir / "gun.csv"),
        fmt="CSVWithNames",
        settings={"format_csv_null_representation": "NULL"},
    )

    logger.info(
        "Finished data insertion into DWH.",
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
        default="8123",
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
        "filepath", help="Path to the CSV GUN dataset. Default to empty."
    )

    args = parser.parse_args()

    load_gun_csv_file_to_dwh(
        filepath=args.filepath,
        dwh_host=args.dwh_host,
        dwh_http_port=args.dwh_http_port,
        dwh_password=args.dwh_password,
        dwh_username=args.dwh_username,
    )
