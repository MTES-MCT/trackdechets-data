import logging
from datetime import datetime

from dags_utils.alerting import send_alert_to_mattermost
from dags_utils.datawarehouse_connection import get_dwh_client
import gzip
import csv
import io
import requests
from airflow.decorators import dag, task
from airflow.models import Variable
from etl_insee.schemas.stock_etablissement import STOCK_ETABLISSEMENT_DDL
from etl_insee.schemas.unite_legale import UNITE_LEGALE_DDL

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
    def insert_etablissement_data_to_ch():
        dst_database = "raw_zone_insee"
        dst_table = "stock_etablissement"
        tmp_table = f"{dst_table}_tmp"
        url = Variable.get("BASE_SIRENE_ETABLISSEMENT_URL")

        client = get_dwh_client()

        logger.info(f"Starting creation of database {dst_database} if not exists.")
        client.command(f"CREATE DATABASE IF NOT EXISTS {dst_database}")
        logger.info(f"Finished creation of database {dst_database} if not exists.")

        logger.info("Starting temporary table creation if not exists.")
        create_table_statement = STOCK_ETABLISSEMENT_DDL.format(database=dst_database, table=tmp_table)
        client.command(create_table_statement)
        logger.info(f"Finished temporary table creation {tmp_table}.")

        logger.info("Truncating temporary table if exists.")
        client.command(
            f"TRUNCATE TABLE IF EXISTS {dst_database}.{tmp_table}"
        )
        logger.info("Finished truncating temporary table if exists.")

        logger.info(f"Starting inserting data into temporary table {tmp_table}.")
        client.command(
            f"""
            INSERT INTO {dst_database}.{tmp_table}
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

        logger.info(f"Removing existing table {dst_database}.{dst_table}.")
        client.command(f"DROP TABLE IF EXISTS {dst_database}.{dst_table}")
        logger.info(f"Finished removing existing table {dst_database}.{dst_table}.")

        logger.info(f"Renaming temporary table {tmp_table} to {dst_table}.")
        client.command(
            f"RENAME TABLE {dst_database}.{tmp_table} TO {dst_database}.{dst_table}"
        )
        logger.info(f"Finished renaming temporary table {tmp_table} to {dst_table}.")

    @task
    def insert_unite_legale_annuaire_entreprises_data_to_ch():
        """
        Mise à jour des données Unite Legale à partir du fichier CSV de la base Sirene.
        """
        dst_database = "raw_zone_insee"
        dst_table = "annuaire_entreprises_unite_legale"
        tmp_table = f"{dst_table}_tmp"
        url = Variable.get("BASE_SIRENE_UNITE_LEGALE_ANNUAIRE_ENTREPRISES_URL")

        client = get_dwh_client()

        logger.info(f"Starting creation of database {dst_database} if not exists.")
        client.command(f"CREATE DATABASE IF NOT EXISTS {dst_database}")
        logger.info(f"Finished creation of database {dst_database} if not exists.")

        logger.info("Starting temporary table creation if not exists.")
        create_table_statement = UNITE_LEGALE_DDL.format(
            database=dst_database, table=tmp_table
        )
        client.command(create_table_statement)
        logger.info("Finished temporary table creation.")

        logger.info("Truncating temporary table if exists.")
        client.command(f"TRUNCATE TABLE IF EXISTS {dst_database}.{tmp_table}")
        logger.info("Finished truncating temporary table if exists.")

        logger.info(f"Starting inserting data into temporary table {tmp_table}.")

        logger.info("Starting downloading data from URL.")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with gzip.GzipFile(fileobj=response.raw, mode="rb") as gz:
            reader = csv.reader(io.TextIOWrapper(gz, newline=""))

            header = next(reader)  # skip header if present
            batch = []
            batch_size = 100000
            batch_number = 0

            for row in reader:
                batch.append(row)
                if len(batch) >= batch_size:
                    batch_number += 1
                    client.insert(
                        database=dst_database,
                        table=tmp_table,
                        data=batch,
                        column_names=header,
                    )
                    batch.clear()
                    logger.info(f"Batch {batch_number} inserted")

            if batch:
                client.insert(
                    database=dst_database,
                    table=tmp_table,
                    data=batch,
                    column_names=header,
                )
                logger.info(f"Batch {batch_number} inserted")

        logger.info(f"Finished inserting data into temporary table {tmp_table}.")
        logger.info(f"Removing existing table {dst_table}.")
        client.command(f"DROP TABLE IF EXISTS {dst_database}.{dst_table}")
        logger.info(f"Finished removing existing table {dst_table}.")

        logger.info(f"Renaming temporary table {tmp_table} to {dst_table}.")
        client.command(
            f"RENAME TABLE {dst_database}.{tmp_table} TO {dst_database}.{dst_table}"
        )
        logger.info("Finished renaming temporary table.")

    insert_etablissement_data_to_ch()
    insert_unite_legale_annuaire_entreprises_data_to_ch()


el_base_sirene_dag = el_base_sirene()

if __name__ == "__main__":
    el_base_sirene_dag.test()
