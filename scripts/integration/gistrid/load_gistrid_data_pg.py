import argparse
import logging
from datetime import datetime, timezone

import polars as pl
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s-%(name)s-%(asctime)s-%(message)s"
)
logger = logging.getLogger(__name__)


def load_gistrid_excel_file_to_dwh(con_url: str, filepath: str, year: int):
    engine = create_engine(con_url)

    gistrid_df = pl.read_excel(
        filepath,
        schema_overrides={
            "numéro de notification": pl.String,
            "numéro GISTRID du notifiant": pl.String,
            "numéro SIRET de l'installation de traitement": pl.String,
            "numéro GISTRID de l'installation de traitement": pl.String,
        },
    )

    gistrid_df = gistrid_df.with_columns(pl.lit(int(year)).alias("annee"))

    logger.info(
        "Loaded dataset with %s lines and %s columns into memory.",
        gistrid_df.shape[0],
        gistrid_df.shape[1],
    )

    if "pays de transit" not in gistrid_df.columns:
        logger.info(
            "Column 'pays de transit' not found, adding it to avoid schema problems'",
        )
        gistrid_df.with_columns(pl.lit(None).alias("pays de transit"))

    logger.info(
        "Starting data insertion into DWH.",
    )

    gistrid_df = gistrid_df.with_columns(
        pl.lit(datetime.now(timezone.utc)).alias("inserted_at")
    )

    gistrid_df.write_database(
        "raw_zone_gistrid.notifications",
        connection=engine,
        if_table_exists="append",
    )

    logger.info(
        "Finished data insertion into DWH.",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="GISTRIDLoader", description="Load GUN data to DWH"
    )

    parser.add_argument("con_url", help="URL to connect to the database.")
    parser.add_argument("filepath", help="Path to the Excel GISTRID dataset.")
    parser.add_argument("year", help="Year of dataset. Used to deduplicate.")

    args = parser.parse_args()

    load_gistrid_excel_file_to_dwh(args.con_url, args.filepath, args.year)
