import logging
from datetime import datetime

from dags_utils.alerting import send_alert_to_mattermost
from dags_utils.datawarehouse_connection import get_dwh_client

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
        url = Variable.get("BASE_SIRENE_ETABLISSEMENT_URL")

        client = get_dwh_client()

        logger.info("Starting creation of database raw_zone_insee if not exists.")
        client.command("CREATE DATABASE IF NOT EXISTS raw_zone_insee")
        logger.info("Finished creation of database raw_zone_insee if not exists.")

        logger.info("Starting temporary table creation if not exists.")
        create_table_statement = STOCK_ETABLISSEMENT_DDL
        client.command(create_table_statement)
        logger.info("Finished temporary table creation.")

        logger.info("Truncating temporary table if exists.")
        client.command(
            "TRUNCATE TABLE IF EXISTS raw_zone_insee.stock_etablissement_tmp"
        )
        logger.info("Finished truncating temporary table if exists.")

        logger.info("Starting inserting data into temporary table.")
        client.command(
            f"""
            INSERT INTO raw_zone_insee.stock_etablissement_tmp
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
        client.command(
            "RENAME TABLE raw_zone_insee.stock_etablissement_tmp TO raw_zone_insee.stock_etablissement"
        )
        logger.info("Finished renaming temporary table.")

    @task
    def insert_unite_legale_data_to_ch():
        """
        Url stable pour le format parquet: https://www.data.gouv.fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/
        """

        url = Variable.get("BASE_SIRENE_UNITE_LEGALE_URL")

        client = get_dwh_client()

        logger.info("Starting creation of database raw_zone_insee if not exists.")
        client.command("CREATE DATABASE IF NOT EXISTS raw_zone_insee")
        logger.info("Finished creation of database raw_zone_insee if not exists.")

        logger.info("Starting temporary table creation if not exists.")
        create_table_statement = UNITE_LEGALE_DDL
        client.command(create_table_statement)
        logger.info("Finished temporary table creation.")

        logger.info("Truncating temporary table if exists.")
        client.command("TRUNCATE TABLE IF EXISTS raw_zone_insee.unite_legale_tmp")
        logger.info("Finished truncating temporary table if exists.")

        logger.info("Starting inserting data into temporary table.")
        client.command(
            f"""
            INSERT INTO raw_zone_insee.unite_legale_tmp
            SELECT *
            FROM url('{url}', 'Parquet', '
                siren                                        String,
                statutDiffusionUniteLegale                   LowCardinality(Nullable(String)),
                unitePurgeeUniteLegale                       Nullable(UInt8),
                dateCreationUniteLegale                      Nullable(Int64),
                sigleUniteLegale                             Nullable(String),
                sexeUniteLegale                              LowCardinality(Nullable(String)),
                prenom1UniteLegale                           Nullable(String),
                prenom2UniteLegale                           Nullable(String),
                prenom3UniteLegale                           Nullable(String),
                prenom4UniteLegale                           Nullable(String),
                prenomUsuelUniteLegale                       Nullable(String),
                pseudonymeUniteLegale                        Nullable(String),
                identifiantAssociationUniteLegale            Nullable(String),
                trancheEffectifsUniteLegale                  LowCardinality(Nullable(String)),
                anneeEffectifsUniteLegale                    Nullable(Int16),
                dateDernierTraitementUniteLegale             Nullable(DateTime),
                nombrePeriodesUniteLegale                    Nullable(Int64),
                categorieEntreprise                          LowCardinality(Nullable(String)),
                anneeCategorieEntreprise                     Nullable(Int16),
                dateDebut                                    Nullable(Int64),
                etatAdministratifUniteLegale                 LowCardinality(Nullable(String)),
                nomUniteLegale                               Nullable(String),
                nomUsageUniteLegale                          Nullable(String),
                denominationUniteLegale                      Nullable(String),
                denominationUsuelle1UniteLegale              Nullable(String),
                denominationUsuelle2UniteLegale              Nullable(String),
                denominationUsuelle3UniteLegale              Nullable(String),
                categorieJuridiqueUniteLegale                Nullable(Int64),
                activitePrincipaleUniteLegale                LowCardinality(Nullable(String)),
                nomenclatureActivitePrincipaleUniteLegale    LowCardinality(Nullable(String)),
                nicSiegeUniteLegale                          Nullable(Int64),
                economieSocialeSolidaireUniteLegale          LowCardinality(Nullable(String)),
                societeMissionUniteLegale                    LowCardinality(Nullable(String)),
                caractereEmployeurUniteLegale                Nullable(String)')
            """,
            settings={"max_http_get_redirects": 2},
        )
        logger.info("Finished inserting data into temporary table.")

        logger.info("Removing existing table.")
        client.command("DROP TABLE IF EXISTS raw_zone_insee.unite_legale")
        logger.info("Finished removing existing table.")

        logger.info("Renaming temporary table.")
        client.command(
            "RENAME TABLE raw_zone_insee.unite_legale_tmp TO raw_zone_insee.unite_legale"
        )
        logger.info("Finished renaming temporary table.")

    insert_etablissement_data_to_ch()
    insert_unite_legale_data_to_ch()


el_base_sirene_dag = el_base_sirene()

if __name__ == "__main__":
    el_base_sirene_dag.test()
