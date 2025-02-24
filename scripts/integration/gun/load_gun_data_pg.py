import argparse
import logging
from datetime import datetime, timezone

import polars as pl
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s-%(name)s-%(asctime)s-%(message)s"
)
logger = logging.getLogger(__name__)


def load_gun_csv_file_to_dwh(con_url: str, filepath: str):
    engine = create_engine(con_url)

    gun_df = pl.read_csv(
        filepath,
        null_values="NULL",
        schema_overrides={"Code AIOT": pl.String, "SIRET": pl.String},
    )

    logger.info(
        "Loaded dataset with %s lines and %s columns into memory.",
        gun_df.shape[0],
        gun_df.shape[1],
    )

    logger.info(
        "Starting data insertion into DWH.",
    )

    new_icpe_data_df = gun_df.with_columns(
        pl.lit(datetime.now(timezone.utc)).alias("inserted_at")
    )

    if "Capacité projet" in new_icpe_data_df.columns:
        logging.info("Renaming 'Capacité projet' to 'Capacité Projet'")
        new_icpe_data_df = new_icpe_data_df.rename(
            {
                "Capacité projet": "Capacité Projet",
            }
        )
    if "Capacité totale" in new_icpe_data_df.columns:
        logging.info("Renaming 'Capacité totale' to 'Capacité Totale'")
        new_icpe_data_df = new_icpe_data_df.rename(
            {"Capacité totale": "Capacité Totale"}
        )

    new_icpe_data_df.write_database(
        "raw_zone_icpe.installations_rubriques_2024",
        connection=engine,
        if_table_exists="append",
    )

    logger.info(
        "Finished data insertion into DWH.",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="GUNLoader", description="Load GUN data to DWH"
    )

    parser.add_argument("con_url", help="URL to connect to the database.")
    parser.add_argument("filepath", help="Path to the CSV GUN dataset.")

    args = parser.parse_args()

    load_gun_csv_file_to_dwh(args.con_url, args.filepath)
