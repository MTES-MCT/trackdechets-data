import logging
import shutil
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

from elasticsearch7 import Elasticsearch
import pandas as pd
import pendulum
from requests.exceptions import RequestException

from dags_utils.alerting import send_alert_to_mattermost
from airflow.decorators import dag, task
from airflow.models import Connection, Variable
from airflow.utils.trigger_rule import TriggerRule
from trackdechets_search_sirene.utils import (
    download_es_ca_pem,
    extract_companies,
    format_extracted_companies,
    git_clone_trackdechets,
    npm_install_build,
    read_output,
)

logging.basicConfig()
logger = logging.getLogger()


es_connection = Connection.get_connection_from_secrets(
    "trackdechets_search_sirene_elasticsearch_url"
)

es_credentials = ""
if es_connection.login and es_connection.password:
    es_credentials = f"{es_connection.login}:{es_connection.password}@"

es_schema = "http"
if es_connection.schema:
    es_schema = f"{es_connection.schema}"


env = Variable.get("AIRFLOW_ENV", "dev")
environ = {
    "FORCE_LOGGER_CONSOLE": True,
    "ELASTICSEARCH_URL": f"{es_schema}://{es_credentials}{es_connection.host}:{es_connection.port}",
    "DD_LOGS_ENABLED": True if env == "prod" else False,
    "DD_TRACE_ENABLED": True if env == "prod" else False,
    "DD_API_KEY": Variable.get("DD_API_KEY"),
    "DD_APP_NAME": Variable.get("DD_APP_NAME"),
    "DD_ENV": "production" if env == "prod" else "",
    "NODE_ENV": "production" if env == "prod" else "recette",
    "ELASTICSEARCH_CAPEM": Variable.get("ELASTICSEARCH_CAPEM"),
    "INSEE_CLIENT_ID": Variable.get("INSEE_CLIENT_ID"),
    "INSEE_CLIENT_SECRET": Variable.get("INSEE_CLIENT_SECRET"),
    "INSEE_USERNAME": Variable.get("INSEE_USERNAME"),
    "INSEE_PASSWORD": Variable.get("INSEE_PASSWORD"),
}

# Constant pointing to the node git indexation repo
TRACKDECHETS_SIRENE_SEARCH_GIT = "trackdechets-sirene-search"
TRACKDECHETS_SIRENE_SEARCH_GIT_BRANCH = Variable.get(
    "TRACKDECHETS_SIRENE_SEARCH_GIT_BRANCH", "main"
)


@dag(
    schedule_interval="0 0/12 * * *",
    catchup=False,
    start_date=datetime(2025, 4, 5),
    on_failure_callback=send_alert_to_mattermost,
)
def incremental_update_search_sirene():
    """
    DAG permettant d'indexer les dernières modifications
    de SIRENE durant les 24h passées dans ElasticSearch
    https://api.insee.fr/catalogue/site/themes/wso2/subthemes/insee/pages/item-info.jag?name=Sirene&version=V3&provider=insee
    """

    @task
    def create_tmp_dir() -> str:
        """
        Generate a temporatory directory for artifacts.
        """
        output_path = Path(tempfile.mkdtemp(prefix="trackdechets_update_daily_sirene"))
        return str(output_path)

    @task
    def task_git_clone_trackdechets(tmp_dir) -> str:
        return git_clone_trackdechets(
            tmp_dir,
            TRACKDECHETS_SIRENE_SEARCH_GIT,
            TRACKDECHETS_SIRENE_SEARCH_GIT_BRANCH,
        )

    @task
    def task_npm_install_build(tmp_dir) -> str:
        """
        npm install && npm run build
        """
        return npm_install_build(tmp_dir, TRACKDECHETS_SIRENE_SEARCH_GIT)

    @task
    def task_download_es_ca_pem(tmp_dir) -> str:
        return download_es_ca_pem(
            tmp_dir, environ["ELASTICSEARCH_CAPEM"], TRACKDECHETS_SIRENE_SEARCH_GIT
        )

    @task
    def task_get_most_recent_document_datetime_from_es_index(tmp_dir) -> str:
        capem_path = Path(tmp_dir) / "trackdechets-sirene-search/dist/common/es.cert"

        # Connexion sécurisée avec certificat et authentification (si nécessaire)
        es = Elasticsearch(environ["ELASTICSEARCH_URL"], ca_certs=capem_path)

        # Définition du pattern d'index avec wildcard pour le suffixe variable
        index_pattern = f"stocketablissement-{environ['NODE_ENV']}"

        # Recherche avec agrégation sur le champ 'date'
        response = es.search(
            index=index_pattern,
            size=0,
            aggs={"max_date": {"max": {"field": "dateDernierTraitementEtablissement"}}},
        )

        max_date = response["aggregations"]["max_date"]["value_as_string"]

        return datetime.fromisoformat(max_date)

    @task
    def task_query_and_index(
        tmp_dir,
        start_date: datetime,
    ) -> str:
        """
        query INSEE Sirene api the run index
        """
        tmp_dir = Path(tmp_dir)

        logger.info(
            f"INSEE API query data after : {start_date:%Y-%m-%d}",
        )

        try:
            companies = extract_companies(
                client_id=environ["INSEE_CLIENT_ID"],
                client_secret=environ["INSEE_CLIENT_SECRET"],
                username=environ["INSEE_USERNAME"],
                password=environ["INSEE_PASSWORD"],
                date_start=start_date,
            )
            companies_formatted = format_extracted_companies(companies)
            df = pd.DataFrame.from_dict(companies_formatted)
            # print the items
            path_or_buf = tmp_dir / f"{start_date:%Y%m%d}.csv"
            df.to_csv(path_or_buf=path_or_buf, index=False)

            index_command = f"npm run index:siret:csv -- {path_or_buf}"

            environ_clean = {
                k: v if not isinstance(v, bool) else str(v).lower()
                for k, v in environ.items()
            }  # Transform python booleans to json booleans
            node_process = subprocess.Popen(
                index_command,
                shell=True,
                cwd=tmp_dir / TRACKDECHETS_SIRENE_SEARCH_GIT,
                env=environ_clean,
                stdout=subprocess.PIPE,
                text=True,
            )
            # read the output
            while True:
                line = node_process.stdout.readline()
                if not line:
                    break
                read_output(line)

            while node_process.wait():
                if node_process.returncode != 0:
                    raise Exception(node_process)

            return str(tmp_dir)

        except RequestException as error:
            if error.errno == 404:
                print("nothing")
                pass
            else:
                raise Exception(error)

        return str(tmp_dir)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def task_cleanup_tmp_files(tmp_dir: str):
        """Clean DAG's artifacts"""
        shutil.rmtree(tmp_dir)

    """
    Dag workflow
    """
    tmp_dir = create_tmp_dir()
    setup = (
        task_git_clone_trackdechets(tmp_dir)
        >> task_npm_install_build(tmp_dir)
        >> task_download_es_ca_pem(tmp_dir)
    )
    start_date = task_get_most_recent_document_datetime_from_es_index(tmp_dir)
    (
        setup
        >> start_date
        >> task_query_and_index(tmp_dir, start_date)
        >> task_cleanup_tmp_files(tmp_dir)
    )


trackdechets_search_sirene_incremental_dag = incremental_update_search_sirene()

if __name__ == "__main__":
    trackdechets_search_sirene_incremental_dag.test()
