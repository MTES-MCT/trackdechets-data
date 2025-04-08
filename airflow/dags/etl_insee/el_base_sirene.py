import logging
import shutil
import tempfile
from typing import Any
import urllib
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Connection, Variable
from airflow.utils.trigger_rule import TriggerRule
import clickhouse_connect
from clickhouse_connect.driver.tools import insert_file

from dags_utils.alerting import send_alert_to_mattermost
from etl_insee.schemas.stock_etablissement import STOCK_ETABLISSEMENT_DDL

logging.basicConfig()
logger = logging.getLogger(__name__)


@dag(
    schedule_interval="@monthly",
    catchup=False,
    start_date=datetime(2022, 8, 31),
    on_failure_callback=send_alert_to_mattermost,
)
def el_base_sirene():
    """DAG qui met à jour la base SIRENE dans le Data Warehouse Clickhouse Trackdéchets."""

    @task
    def extract_stock_etablissement() -> str:
        url = Variable.get("BASE_SIRENE_URL")

        tmp_dir = Path(tempfile.mkdtemp(prefix="base_sirene_ETL"))

        logger.info(
            "Starting downloading stock_etablissement archive to path %s.", tmp_dir
        )
        urllib.request.urlretrieve(url, tmp_dir / "stock_etablissement.zip")
        logger.info(
            "Finished downloading stock_etablissement archive to path %s.", tmp_dir
        )

        logger.info("Starting extracting stock_etablissement archive.")
        shutil.unpack_archive(tmp_dir / "stock_etablissement.zip", tmp_dir)
        logger.info("Finished extracting stock_etablissement archive.")

        return str(tmp_dir)

    @task
    def insert_data_to_ch(tmp_dir: str, params: dict[str, Any] = None):
        tmp_dir = Path(tmp_dir)

        DWH_CON = Connection.get_connection_from_secrets("td_datawarehouse").to_dict()
        client = clickhouse_connect.get_client(
            host=DWH_CON.get("host"),
            port=DWH_CON.get("extra").get("http_port"),
            username=DWH_CON.get("login"),
            password=DWH_CON.get("password"),
            database="raw_zone_insee",
        )

        logger.info("Starting temporary table creation if not exists.")
        create_table_statement = STOCK_ETABLISSEMENT_DDL
        client.command(create_table_statement)
        logger.info("Finished temporary table creation.")

        logger.info("Truncating temporary table if exists.")
        client.command("TRUNCATE TABLE IF EXISTS stock_etablissement_tmp")
        logger.info("Finished truncating temporary table if exists.")

        logger.info("Starting inserting data into temporary table.")
        insert_file(
            client,
            "stock_etablissement_tmp",
            str((tmp_dir / "StockEtablissement_utf8.csv").absolute()),
            fmt="CSVWithNames",
        )
        logger.info("Finished inserting data into temporary table.")

        logger.info("Removing existing table.")
        client.command("DROP TABLE IF EXISTS raw_zone_insee.stock_etablissement")
        logger.info("Finished removing existing table.")

        logger.info("Renaming temporary table.")
        client.command("RENAME TABLE stock_etablissement_tmp TO stock_etablissement")
        logger.info("Finished renaming temporary table.")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def cleanup_tmp_files(tmp_dir: str):
        shutil.rmtree(tmp_dir)

    tmp_dir = extract_stock_etablissement()
    insert_data_to_ch(tmp_dir) >> cleanup_tmp_files(tmp_dir)


el_base_sirene_dag = el_base_sirene()

if __name__ == "__main__":
    el_base_sirene_dag.test()
