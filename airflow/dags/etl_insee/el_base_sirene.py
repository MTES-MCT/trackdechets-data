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
        url stable: https://www.data.gouv.fr/api/1/datasets/r/b8e5376c-c158-4d88-91f3-f6bb0d165332
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
                siren                                           String,
                siret_siege                                     String,
                nom_complet                                     String,
                etat_administratif                              LowCardinality(String),
                statut_diffusion                                LowCardinality(String),
                nombre_etablissements                           Int32,
                nombre_etablissements_ouverts                   Int32,
                colter_code                                     Nullable(String),
                colter_code_insee                               Nullable(String),
                colter_elus                                     Nullable(String),
                colter_niveau                                   LowCardinality(Nullable(String)),
                date_mise_a_jour_insee                          Nullable(Date),
                date_mise_a_jour_rne                            Nullable(Date),
                egapro_renseignee                               Bool,
                est_association                                 Bool,
                est_entrepreneur_individuel                     Bool,
                est_entrepreneur_spectacle                      Bool,
                statut_entrepreneur_spectacle                   Nullable(String),
                est_ess                                         Bool,
                est_organisme_formation                         Bool,
                est_qualiopi                                    Bool,
                est_service_public                              Bool,
                est_societe_mission                             Bool,
                liste_elus                                      Nullable(String),
                liste_id_organisme_formation                    Nullable(String),
                liste_idcc                                      Nullable(String),
                est_siae                                        Bool,
                type_siae                                       Nullable(String)')
            """,
            settings={"max_http_get_redirects": 2},
        )
        logger.info("Finished inserting data into temporary table.")

        logger.info("Removing existing table.")
        client.command("DROP TABLE IF EXISTS raw_zone_insee.unite_legale")
        logger.info("Finished removing existing table.")

        logger.info("Renaming temporary table.")
        client.command(
            "RENAME TABLE raw_zone_insee.stock_etablissement_tmp TO raw_zone_insee.unite_legale"
        )
        logger.info("Finished renaming temporary table.")

    insert_etablissement_data_to_ch()
    insert_unite_legale_data_to_ch()


el_base_sirene_dag = el_base_sirene()

if __name__ == "__main__":
    el_base_sirene_dag.test()
