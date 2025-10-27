import logging
import shutil
import tempfile
import time
from io import StringIO
from pathlib import Path

import httpx
import pandas as pd
import tqdm
from clickhouse_connect.driver.tools import insert_file
from dags_utils.alerting import send_alert_to_mattermost
from dags_utils.datawarehouse_connection import get_dwh_client
from pendulum import datetime

from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from geocoding.schema import COMPANIES_GEOCODED_DDL

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger()


@dag(
    schedule_interval="0 2 * * *",
    catchup=False,
    start_date=datetime(2024, 10, 19),
    on_failure_callback=send_alert_to_mattermost,
)
def companies_geocoding():
    @task
    def create_tmp_dir() -> str:
        """
        Generate a temporatory directory for artifacts.
        """
        output_path = tempfile.mkdtemp(prefix="companies_geocoding")
        return output_path

    @task
    def extract_companies_to_geocode(tmp_dir: str):
        client = get_dwh_client("raw_zone_referentials")
        logger.info("Retrieving companies to geolocalize.")

        companies_df = client.query_df(
            query="""
            select
                stock_etablissement.siret,
                coalesce(company.address, stock_etablissement.adresse) as adresse,
                stock_etablissement.code_commune_etablissement as code_commune_insee
            from
                trusted_zone_trackdechets.company
                left join trusted_zone_insee.stock_etablissement on company.siret = stock_etablissement.siret
            where
                (company.latitude is null
                    or company.longitude is null)
                and (coalesce(company.address, stock_etablissement.adresse) is not null)
            """,
        )

        logger.info("%s to geolocalize.", len(companies_df))

        companies_df.to_parquet(Path(tmp_dir) / "companies_df.parquet", index=False)

    @task
    def geocode_with_ban(tmp_dir: str):
        tmp_dir = Path(tmp_dir)

        companies_df = pd.read_parquet(tmp_dir / "companies_df.parquet")

        batch_size = 5_000
        results = []
        for i in tqdm.trange(0, len(companies_df), batch_size):
            companies_chunck_df = companies_df.iloc[i : (i + batch_size)]
            companies_chunck_df.to_csv(
                tmp_dir / f"companies_data_chunck_{i}.csv", index=False
            )
            with httpx.Client(timeout=6000) as client:
                files = {"data": open(tmp_dir / f"companies_data_chunck_{i}.csv", "rb")}

                logger.info("Chunck %s - Requesting BAN.", i)
                start_time = time.time()
                res = client.post(
                    url="https://api-adresse.data.gouv.fr/search/csv/",
                    data={"citycode": "code_commune_insee", "columns": "adresse"},
                    files=files,
                )
                total_time = time.time() - start_time

                logger.info(
                    "Chunck %s - BAN responded after : %s seconds", i, total_time
                )

            if res.status_code == 200:
                results.append(res.text)
            else:
                raise Exception(
                    "Problem requesting the ban", res.status_code, res.text, res.headers
                )

            time.sleep(5)

        geocoded_df = pd.concat([pd.read_csv(StringIO(e)) for e in results])
        logger.info(
            "Saving geocoded dataframe into csv file. Shape of dataframe : %s",
            geocoded_df.shape,
        )
        geocoded_df.to_csv(tmp_dir / "companies_geocoded.csv", index=False)

    @task
    def insert_companies_geocoded_data_to_database(tmp_dir):
        companies_geocoded_df = pd.read_csv(
            Path(tmp_dir) / "companies_geocoded.csv", dtype=str
        )

        client = get_dwh_client("raw_zone_referentials")

        logger.info("Creating table (if not exists) 'companies_geocoded_by_ban_tmp'.")
        client.command(COMPANIES_GEOCODED_DDL)

        logger.info(
            "Starting insertion of geocoded data (%s companies)",
            len(companies_geocoded_df),
        )
        insert_file(
            client,
            "companies_geocoded_by_ban_tmp",
            str(Path(tmp_dir) / "companies_geocoded.csv"),
            database="raw_zone_referentials",
            fmt="CSVWithNames",
        )
        logger.info("Finished inserting geocoded companies data.")

        logger.info("Removing existing table.")
        client.command(
            "DROP TABLE IF EXISTS raw_zone_referentials.companies_geocoded_by_ban"
        )
        logger.info("Finished removing existing table.")

        logger.info("Renaming temporary table.")
        client.command(
            "RENAME TABLE raw_zone_referentials.companies_geocoded_by_ban_tmp TO raw_zone_referentials.companies_geocoded_by_ban"
        )
        logger.info("Finished renaming temporary table.")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def cleanup_tmp_files(tmp_dir: str):
        shutil.rmtree(tmp_dir)

    tmp_dir = create_tmp_dir()
    (
        extract_companies_to_geocode(tmp_dir)
        >> geocode_with_ban(tmp_dir)
        >> insert_companies_geocoded_data_to_database(tmp_dir)
        >> cleanup_tmp_files(tmp_dir)
    )


companies_geocoding_dag = companies_geocoding()

if __name__ == "__main__":
    companies_geocoding_dag.test()
