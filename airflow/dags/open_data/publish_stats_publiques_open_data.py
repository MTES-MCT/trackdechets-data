import logging
from datetime import datetime, timedelta

import clickhouse_connect
import requests
from utils.alerting import send_alert_to_mattermost

from airflow.decorators import dag, task
from airflow.models import Connection, Variable

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger()

DWH_CON = Connection.get_connection_from_secrets("td_datawarehouse")

DATA_CONFIG = [
    {
        "filename": "bsdd_statistiques_hebdomadaires.csv",
        "table_name": "refined_zone_stats_publiques.bsdd_statistiques_hebdomadaires",
        "title": " Statistiques hebdomadaires des Bordereaux de Suivi de Déchets Dangereux",
        "rid": "cfdfe544-7d8f-4229-83f0-3d4b5d3b5c0a",
    },
    {
        "filename": "bsda_statistiques_hebdomadaires.csv",
        "table_name": "refined_zone_stats_publiques.bsda_statistiques_hebdomadaires",
        "title": "Statistiques hebdomadaires des Bordereaux de Suivi de Déchets Amiante",
        "rid": "6eece3b5-5f1b-41ea-95ee-6eb31122c455",
    },
    {
        "filename": "bsdasri_statistiques_hebdomadaires.csv",
        "table_name": "refined_zone_stats_publiques.bsdasri_statistiques_hebdomadaires",
        "title": "Statistiques hebdomadaires des Bordereaux de Suivi de Déchets d'Activités de Soins à Risques Infectieux",
        "rid": "fbd8d5ee-5450-46b0-95eb-7c0de216feec",
    },
    {
        "filename": "bsff_statistiques_hebdomadaires.csv",
        "table_name": "refined_zone_stats_publiques.bsff_statistiques_hebdomadaires",
        "title": "Statistiques hebdomadaires des Bordereaux de Suivi de Fluides Frigorigènes",
        "rid": "74cdf13d-bf08-4b10-90ca-969fa51dd67f",
    },
    {
        "filename": "bsvhu_statistiques_hebdomadaires.csv",
        "table_name": "refined_zone_stats_publiques.bsvhu_statistiques_hebdomadaires",
        "title": "Statistiques hebdomadaires des Bordereaux de Suivi de Véhicules Hors d'Usage",
        "rid": "78c77679-1b39-4880-aea3-6de210505846",
    },
    {
        "filename": "comptes_statistiques_hebdomadaires.csv",
        "table_name": "refined_zone_stats_publiques.accounts_created_by_week",
        "title": "Statistiques hebdomadaires des créations de compte sur l'application Trackdéchets",
        "rid": "0fdfa874-8093-471a-8ae2-8f9d9398364c",
    },
    {
        "filename": "quantite_dechets_traites_statistiques_hebdomadaires.csv",
        "table_name": "refined_zone_stats_publiques.weekly_waste_processed_stats",
        "title": "Statistiques hebdomadaires de la quantité de déchets traités par type de bordereaux, opération de traitement et type d'opération",
        "rid": "ad5280cd-d8d8-4bd0-8677-f9c926e8fddd",
    },
    {
        "filename": "creations_bordereaux_par_an_et_type_bordereaux.csv",
        "table_name": "refined_zone_stats_publiques.annual_number_of_bordereaux_created_by_bordereaux_type",
        "title": "Statistiques annuelles de création de bordereaux par type de bordereaux",
        "rid": "8c0d4554-98d8-4c03-a931-8e2baeb70f1f",
    },
    {
        "filename": "quantite_produite_par_an_et_code_naf.csv",
        "table_name": "refined_zone_stats_publiques.annual_waste_produced_by_naf",
        "title": "Statistiques annuelles de la quantité de déchets produits par code NAF",
        "rid": "2d2c2765-82d8-426f-a543-e14b08b06798",
    },
    {
        "filename": "comptes_etablissements_par_an_et_code_naf.csv",
        "table_name": "refined_zone_stats_publiques.annual_company_accounts_created_by_naf",
        "title": "Statistiques annuelles des créations de compte sur l'application Trackdéchets par code NAF",
        "rid": "4780d501-01e2-4262-9f87-9c77b404fb0a",
    },
]

datagouv_api_key = Variable.get("DATAGOUV_API_KEY")
datagouv_dataset_id = Variable.get("DATAGOUV_STATS_PUBLIQUES_DATASET_ID")


@dag(
    start_date=datetime(2023, 6, 26, 12),
    schedule_interval="0 8 * * 1",
    catchup=False,
    on_failure_callback=send_alert_to_mattermost,
)
def publish_stats_publiques_open_data():
    """
    DAG qui sert à publier chaque semaine les nouveaux fichiers relatifs aux stats publiques sur data.gouv.fr
    """

    @task()
    def extract_and_load_stats_publiques_data():
        con = DWH_CON.to_dict()

        client = clickhouse_connect.get_client(
            host=con.get("host"),
            port=con.get("extra").get("http_port"),
            username=con.get("login"),
            password=con.get("password"),
            database="raw_zone_referentials",
        )

        for config in reversed(DATA_CONFIG):
            table_name = config["table_name"]
            ressource_id = config["rid"]
            filename = config["filename"]
            title = config["title"]

            sql = f"SELECT * from {table_name} WHERE semaine>='2022-01-01'"
            if "annual" in table_name:
                sql = f"SELECT * from {table_name} WHERE annee>=2022"

            logger.info("Querying the datawarehouse with the following query : %s", sql)
            df = client.query_df(
                query=sql,
            )

            logger.info("Get following df : %s", df.describe())

            logger.info("Uploading file : %s", filename)
            resp = requests.post(
                url=f"https://www.data.gouv.fr/api/1/datasets/{datagouv_dataset_id}/resources/{ressource_id}/upload/",
                headers={"X-API-KEY": datagouv_api_key},
                files={
                    "file": (
                        filename,
                        df.to_csv(index=False),
                    )
                },
            )
            requests.put(
                url=f"https://www.data.gouv.fr/api/1/datasets/{datagouv_dataset_id}/resources/{ressource_id}/",
                headers={"X-API-KEY": datagouv_api_key},
                json={"title": title},
            )
            logger.info("Received response from data.gouv.fr API : %s", resp.json())

    extract_and_load_stats_publiques_data()


publish_stats_publiques_open_data_dag = publish_stats_publiques_open_data()
