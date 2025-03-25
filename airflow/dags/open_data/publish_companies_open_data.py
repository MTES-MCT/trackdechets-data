import logging
from datetime import datetime

import clickhouse_connect
import requests
from airflow.decorators import dag, task
from airflow.models import Connection, Variable
from utils.alerting import send_alert_to_mattermost

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger()

DWH_CON = Connection.get_connection_from_secrets("td_datawarehouse")


@dag(
    start_date=datetime(2022, 2, 7),
    schedule_interval="@daily",
    user_defined_macros={},
    catchup=False,
    on_failure_callback=send_alert_to_mattermost,
)
def publish_companies_open_data():
    """
    DAG dedicated to the loading of a subset of company data to data.gouv.fr
    """

    @task()
    def extract_transform_and_load_company_data():
        con = DWH_CON.to_dict()

        client = clickhouse_connect.get_client(
            host=con.get("host"),
            port=con.get("extra").get("http_port"),
            username=con.get("login"),
            password=con.get("password"),
            database="raw_zone_referentials",
        )
        df_company = client.query_df(
            query="""  
            SELECT
                siret,
                toDate(created_at) as date_inscription,
                toDate(updated_at) as date_derniere_mise_a_jour,
                "name" as nom,
                if(
                    not empty(ac.id),
                    'oui',
                    ''
                ) as "non_diffusible"
            FROM
                trusted_zone_trackdechets.company c
            left join trusted_zone_trackdechets.anonymous_company ac on
                c.org_id = ac.org_id 
            """,
        )
        df_company = df_company.set_index("siret")

        logger.info(f"Number of companies: {df_company.index.size}")

        api_key = Variable.get("DATAGOUV_API_KEY")
        dataset_id = Variable.get("DATAGOUV_ETABLISSEMENTS_DATASET_ID")
        resource_id = Variable.get("DATAGOUV_ETABLISSEMENTS_RESOURCE_ID")

        response = requests.post(
            url=f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/resources/{resource_id}/upload",
            headers={"X-API-KEY": api_key},
            files={
                "file": (
                    "etablissements_inscrits.csv",
                    df_company.to_csv(),
                )
            },
        )

        if response.status_code == 200:
            logger.info(f"Data.gouv response : {response.text}")
        else:
            raise Exception(
                "Problem requesting the ban", response.status_code, response.text
            )

    extract_transform_and_load_company_data()


publish_companies_open_data_dag = publish_companies_open_data()

if __name__ == "__main__":
    publish_companies_open_data_dag.test()
