import logging
from datetime import datetime

import clickhouse_connect

from airflow.models import Variable
from airflow.decorators import dag, task

from dags_utils.datawarehouse_connection import get_dwh_client
from dags_utils.alerting import send_alert_to_mattermost

logging.basicConfig()
logger = logging.getLogger(__name__)


configs = {
    "bsdd": {
        "table_name": "refined_zone_observatoires.bsdd_observatoires",
        "date_expr": "toYear(coalesce(emetteur_date_signature_emission,date_creation))",
    },
    "bsda": {
        "table_name": "refined_zone_observatoires.bsda_observatoires",
        "date_expr": "toYear(coalesce(entreprise_travaux_date_signature,emetteur_date_signature_emission,date_creation))",
    },
    "bsff": {
        "table_name": "refined_zone_observatoires.bsff_observatoires",
        "date_expr": "toYear(coalesce(emetteur_date_signature_emission,date_creation))",
    },
    "bsdasri": {
        "table_name": "refined_zone_observatoires.bsdasri_observatoires",
        "date_expr": "toYear(coalesce(emetteur_date_signature_emission,date_creation))",
    },
    "bsvhu": {
        "table_name": "refined_zone_observatoires.bsvhu_observatoires",
        "date_expr": "toYear(coalesce(emetteur_date_signature_emission,date_creation))",
    },
}


@dag(
    schedule_interval="30 16 1 * *",
    catchup=False,
    start_date=datetime(2022, 8, 31),
    on_failure_callback=send_alert_to_mattermost,
)
def el_observatoires():
    """DAG qui permet d'uploader sur S3 les exports de données dédiées aux observatoires.
    La configuration stockée est stockée dans la configuration Clickhouse.
    """

    @task
    def extract_from_dwh_and_load_to_s3():
        gerico_bucket_name = Variable.get("GERICO_S3_BUCKET_NAME")

        client = get_dwh_client("raw_zone_referentials")

        for bs_type, config in configs.items():
            date_expr = config["date_expr"]
            table_name = config["table_name"]

            logger.info("Starting copying full table %s to S3.", bs_type)
            copy_stmt = f"""
            INSERT INTO FUNCTION
            s3(
                'https://s3.fr-par.scw.cloud/{gerico_bucket_name}/{bs_type}/{bs_type}.parquet',
                'parquet'
                )
            SELECT
                *
            FROM {table_name}
            SETTINGS s3_truncate_on_insert = 1
            """
            client.command(copy_stmt)
            logger.info("Finished copying full table %s to S3.", bs_type)

            years = list(range(2022, 2026))
            for i, year in enumerate(years):
                operator = "="
                if i == 0:
                    operator = "<="
                if i == (len(years) - 1):
                    operator = ">="

                logger.info(
                    "Starting copying table %s to S3 for year %s", bs_type, year
                )
                copy_stmt = f"""
                INSERT INTO FUNCTION
                s3(
                    'https://s3.fr-par.scw.cloud/{gerico_bucket_name}/{bs_type}/{bs_type}_{year}.parquet',
                    'parquet'
                    )
                SELECT
                    *
                FROM {table_name}
                WHERE {date_expr}{operator}{year}
                SETTINGS s3_truncate_on_insert = 1
                """
                client.command(copy_stmt)
                logger.info(
                    "Finished copying table %s to S3 for year %s", bs_type, year
                )

    extract_from_dwh_and_load_to_s3()


el_observatoires_dag = el_observatoires()

if __name__ == "__main__":
    el_observatoires_dag.test()
