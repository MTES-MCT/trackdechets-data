import logging
import shutil
import tempfile
import time
from pathlib import Path

import clickhouse_connect
import httpx
import pandas as pd
from clickhouse_connect.driver.tools import insert_file
from utils.alerting import send_alert_to_mattermost
from pendulum import datetime

from airflow.decorators import dag, task
from airflow.models import Connection
from airflow.utils.trigger_rule import TriggerRule

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger()

DWH_CON = Connection.get_connection_from_secrets("td_datawarehouse")


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
        logger.info("Retrieving companies to geolocalize.")

        con = DWH_CON.to_dict()

        client = clickhouse_connect.get_client(
            host=con.get("host"),
            port=con.get("port"),
            username=con.get("login"),
            password=con.get("password"),
            database="raw_zone_referentials",
        )
        companies_df = client.query_df(
            query="""
            select
                siret,
                coalesce(adresse_td,
                adresse_insee) as adresse,
                code_commune_insee
            from
                refined_zone_analytics.cartographie_des_etablissements
            where
                (latitude_td is null
                    or longitude_td is null)
                and (coalesce(adresse_td,
                adresse_insee) is not null)
            """,
        )

        logger.info("%s to geolocalize.", len(companies_df))

        companies_df.to_csv(Path(tmp_dir) / "companies_df.csv", index=False)

    @task
    def geocode_with_ban(tmp_dir: str):
        tmp_dir = Path(tmp_dir)
        with httpx.Client(timeout=6000) as client:
            files = {"data": open(tmp_dir / "companies_df.csv", "rb")}

            logger.info("Requesting BAN.")
            start_time = time.time()
            res = client.post(
                url="https://api-adresse.data.gouv.fr/search/csv/",
                data={"citycode": "code_commune_insee", "columns": "adresse"},
                files=files,
            )
            total_time = time.time() - start_time

            logger.info("BAN responded after : %s seconds", total_time)

        if res.status_code == 200:
            with open(tmp_dir / "companies_geocoded.csv", mode="w") as f:
                f.write(res.text)
        else:
            raise Exception("Problem requesting the ban", res.status_code, res.text)

    @task
    def insert_companies_geocoded_data_to_database(tmp_dir):
        companies_geocoded_df = pd.read_csv(
            Path(tmp_dir) / "companies_geocoded.csv", dtype=str
        )

        con = DWH_CON.to_dict()

        client = clickhouse_connect.get_client(
            host=con.get("host"),
            port=con.get("port"),
            username=con.get("login"),
            password=con.get("password"),
            database="raw_zone_referentials",
        )

        logger.info(
            "Starting insertion of geocoded data (%s companies)",
            len(companies_geocoded_df),
        )
        insert_file(
            client,
            "companies_geocoded_by_ban_tmp",
            Path(tmp_dir) / "companies_geocoded.csv",
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
