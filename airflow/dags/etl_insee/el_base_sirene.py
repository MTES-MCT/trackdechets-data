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
    def insert_data_to_ch():
        url = Variable.get("BASE_SIRENE_URL")

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
        client.command(
            f"""
            INSERT INTO stock_etablissement_tmp
            SELECT *
            FROM url('{url}', 'Parquet', '
                siren                                           String,
                nic                                             String,
                siret                                           String,
                statutDiffusionEtablissement                    LowCardinality(String),
                dateCreationEtablissement                       Nullable(String),
                trancheEffectifsEtablissement                   LowCardinality(Nullable(String)),
                anneeEffectifsEtablissement                     Nullable(Int16),
                activitePrincipaleRegistreMetiersEtablissement  Nullable(String),
                dateDernierTraitementEtablissement              Nullable(DateTime),
                etablissementSiege                              Bool,
                nombrePeriodesEtablissement                     UInt8,
                complementAdresseEtablissement                  Nullable(String),
                numeroVoieEtablissement                         Nullable(String),
                indiceRepetitionEtablissement                   Nullable(String),
                dernierNumeroVoieEtablissement                  Nullable(String),
                indiceRepetitionDernierNumeroVoieEtablissement  Nullable(String),
                typeVoieEtablissement                           LowCardinality(Nullable(String)),
                libelleVoieEtablissement                        Nullable(String),
                codePostalEtablissement                         Nullable(String),
                libelleCommuneEtablissement                     Nullable(String),
                libelleCommuneEtrangerEtablissement             Nullable(String),
                distributionSpecialeEtablissement               LowCardinality(Nullable(String)),
                codeCommuneEtablissement                        Nullable(String),
                codeCedexEtablissement                          Nullable(String),
                libelleCedexEtablissement                       Nullable(String),
                codePaysEtrangerEtablissement                   Nullable(String),
                libellePaysEtrangerEtablissement                Nullable(String),
                identifiantAdresseEtablissement                 Nullable(String),
                coordonneeLambertAbscisseEtablissement          Nullable(String),
                coordonneeLambertOrdonneeEtablissement          Nullable(String),
                complementAdresse2Etablissement                 Nullable(String),
                numeroVoie2Etablissement                        Nullable(String),
                indiceRepetition2Etablissement                  Nullable(String),
                typeVoie2Etablissement                          LowCardinality(Nullable(String)),
                libelleVoie2Etablissement                       Nullable(String),
                codePostal2Etablissement                        Nullable(String),
                libelleCommune2Etablissement                    Nullable(String),
                libelleCommuneEtranger2Etablissement            Nullable(String),
                distributionSpeciale2Etablissement              LowCardinality(Nullable(String)),
                codeCommune2Etablissement                       Nullable(String),
                codeCedex2Etablissement                         Nullable(String),
                libelleCedex2Etablissement                      Nullable(String),
                codePaysEtranger2Etablissement                  Nullable(String),
                libellePaysEtranger2Etablissement               Nullable(String),
                dateDebut                                       Nullable(String),
                etatAdministratifEtablissement                  LowCardinality(Nullable(String)),
                enseigne1Etablissement                          Nullable(String),
                enseigne2Etablissement                          Nullable(String),
                enseigne3Etablissement                          Nullable(String),
                denominationUsuelleEtablissement                Nullable(String),
                activitePrincipaleEtablissement                 LowCardinality(Nullable(String)),
                nomenclatureActivitePrincipaleEtablissement     LowCardinality(Nullable(String)),
                caractereEmployeurEtablissement                 LowCardinality(Nullable(String))')
            """,
            settings={"max_http_get_redirects": 2},
        )
        logger.info("Finished inserting data into temporary table.")

        logger.info("Removing existing table.")
        client.command("DROP TABLE IF EXISTS raw_zone_insee.stock_etablissement")
        logger.info("Finished removing existing table.")

        logger.info("Renaming temporary table.")
        client.command("RENAME TABLE stock_etablissement_tmp TO stock_etablissement")
        logger.info("Finished renaming temporary table.")

    insert_data_to_ch()


el_base_sirene_dag = el_base_sirene()

if __name__ == "__main__":
    el_base_sirene_dag.test()
