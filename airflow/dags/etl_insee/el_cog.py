import logging
import shutil
import tempfile
import urllib
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Connection
from airflow.utils.trigger_rule import TriggerRule
import clickhouse_connect
from clickhouse_connect.driver.tools import insert_file

from etl_insee.schemas.cog import (
    CODE_ARRONDISSEMENT_DDL,
    CODE_CANTON_DDL,
    CODE_COMMUNE_DDL,
    CODE_DEPARTEMENT_DDL,
    CODE_REGION_DDL,
    CODE_TERRITOIRES_OUTRE_MER_DDL,
)
from utils.alerting import send_alert_to_mattermost

logging.basicConfig()
logger = logging.getLogger(__name__)


configs = [
    {
        "name": "code_commune",
        "url": "https://www.data.gouv.fr/fr/datasets/r/8262de72-138f-4596-ad2f-10079e5f4d7c",
        "ddl": CODE_COMMUNE_DDL,
    },
    {
        "name": "code_departement",
        "url": "https://www.data.gouv.fr/fr/datasets/r/e436f772-b05d-47f8-b246-265faab8679f",
        "ddl": CODE_DEPARTEMENT_DDL,
    },
    {
        "name": "code_region",
        "url": "https://www.data.gouv.fr/fr/datasets/r/53cb77ce-8a93-4924-9d5d-920bbe7c679f",
        "ddl": CODE_REGION_DDL,
    },
    {
        "name": "code_territoires_outre_mer",
        "url": "https://www.data.gouv.fr/fr/datasets/r/09f50ab9-f5b6-400a-b599-643e283d7268",
        "ddl": CODE_TERRITOIRES_OUTRE_MER_DDL,
    },
    {
        "name": "code_arrondissement",
        "url": "https://www.data.gouv.fr/fr/datasets/r/21fdff26-33a9-4b8e-bfd9-ce6d2ed5659e",
        "ddl": CODE_ARRONDISSEMENT_DDL,
    },
    {
        "name": "code_canton",
        "url": "https://www.data.gouv.fr/fr/datasets/r/56be3980-13c1-4c04-91fd-60dc92e8ceb8",
        "ddl": CODE_CANTON_DDL,
    },
]


@dag(
    schedule_interval=None,
    catchup=False,
    start_date=datetime(2022, 8, 31),
    on_failure_callback=send_alert_to_mattermost,
)
def el_cog():
    """DAG qui met à jour le Code Officiel Géographique dans le Data Warehouse Clickhouse Trackdéchets."""

    @task
    def download_and_extract_all_cog_files() -> str:
        tmp_dir = Path(tempfile.mkdtemp(prefix="base_sirene_ETL"))

        logger.info(
            "Starting downloading stock_etablissement archive to path %s.", tmp_dir
        )

        for o in configs:
            file_path = tmp_dir / f"{o['name']}.csv"
            urllib.request.urlretrieve(o["url"], file_path)
            logger.info(
                "Finished downloading %s archive to path %s.", o["name"], file_path
            )

        return str(tmp_dir)

    @task
    def insert_data_to_ch(tmp_dir: str):
        tmp_dir = Path(tmp_dir)

        DWH_CON = Connection.get_connection_from_secrets("td_datawarehouse").to_dict()
        client = clickhouse_connect.get_client(
            host=DWH_CON.get("host"),
            port=DWH_CON.get("extra").get("http_port"),
            username=DWH_CON.get("login"),
            password=DWH_CON.get("password"),
            database="raw_zone_insee",
        )

        for o in configs:
            table_name = o["name"]
            table_dll = o["ddl"]
            logger.info(
                "Starting %s temporary table creation if not exists.", table_name
            )
            client.command(f"DROP TABLE IF EXISTS {table_name}_tmp")
            client.command(table_dll)
            logger.info("Finished %s temporary table creation.", table_name)

            logger.info("Starting inserting data into temporary table.")
            insert_file(
                client,
                f"{table_name}_tmp",
                str((tmp_dir / f"{table_name}.csv").absolute()),
                fmt="CSVWithNames",
                settings={"format_csv_null_representation": ""},
            )
            logger.info("Finished inserting data into %s temporary table.", table_name)

            logger.info("Removing %s existing table.", table_name)
            client.command(f"DROP TABLE IF EXISTS {table_name}")
            logger.info("Finished %s removing existing table.", table_name)

            logger.info("Renaming %s temporary table.", table_name)
            client.command(f"RENAME TABLE {table_name}_tmp TO {table_name}")
            logger.info("Finished %s renaming temporary table.", table_name)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def cleanup_tmp_files(tmp_dir: str):
        shutil.rmtree(tmp_dir)

    tmp_dir = download_and_extract_all_cog_files()
    insert_data_to_ch(tmp_dir) >> cleanup_tmp_files(tmp_dir)


cog_dag = el_cog()

if __name__ == "__main__":
    cog_dag.test()
