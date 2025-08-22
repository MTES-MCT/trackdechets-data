import logging
import shutil
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

from trackdechets_search_sirene.utils import (
    download_es_ca_pem,
    git_clone_trackdechets,
    npm_install_build,
    read_output,
    get_es_connection,
)
from dags_utils.alerting import send_alert_to_mattermost

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

logging.basicConfig()
logger = logging.getLogger()


logger = logging.getLogger(__name__)


@dag(
    schedule_interval="0 22 1 * *",
    catchup=False,
    start_date=datetime(2022, 12, 1),
    on_failure_callback=send_alert_to_mattermost,
)
def full_update_search_sirene():
    """DAG permettant d'indexer la base SIRENE de l'INSEE dans ElasticSearch"""

    # Constant pointing to the node git indexation repo
    TRACKDECHETS_SIRENE_SEARCH_GIT = "trackdechets-sirene-search"

    @task
    def task_git_clone_trackdechets() -> str:
        tmp_dir = Path(tempfile.mkdtemp(prefix="trackdechets_search_sirene"))
        return git_clone_trackdechets(
            tmp_dir,
            TRACKDECHETS_SIRENE_SEARCH_GIT,
            Variable.get("TRACKDECHETS_SIRENE_SEARCH_GIT_BRANCH", "main"),
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
            tmp_dir, Variable.get("ELASTICSEARCH_CAPEM"), TRACKDECHETS_SIRENE_SEARCH_GIT
        )

    @task
    def task_npm_run_index(tmp_dir) -> str:
        """
        npm run index
        """
        airflow_env = Variable.get("AIRFLOW_ENV", "dev")

        environ = {
            "FORCE_LOGGER_CONSOLE": True,
            "ELASTICSEARCH_URL": get_es_connection(),
            "DD_LOGS_ENABLED": True if airflow_env == "prod" else False,
            "DD_TRACE_ENABLED": True if airflow_env == "prod" else False,
            "DD_API_KEY": Variable.get("DD_API_KEY"),
            "DD_APP_NAME": Variable.get("DD_APP_NAME"),
            "DD_ENV": "production" if airflow_env == "prod" else "",
            "NODE_ENV": "production" if airflow_env == "prod" else "recette",
            "NODE_OPTIONS": "--max_old_space_size=10240",  # node.js memory allocation
            "INDEX_CHUNK_SIZE": "2500",
            "INDEX_SIRET_ONLY": False,
            "TD_SIRENE_INDEX_MAX_CONCURRENT_REQUESTS": "4",
            "TD_SIRENE_INDEX_MAX_HIGHWATERMARK": "16384",
            "TD_SIRENE_INDEX_SLEEP_BETWEEN_CHUNKS": "0",
        }

        if environ["INDEX_SIRET_ONLY"]:
            command = "npm run index:siret"
        else:
            command = "npm run index"

        tmp_dir = Path(tmp_dir)

        environ_clean = {
            k: v if not isinstance(v, bool) else str(v).lower()
            for k, v in environ.items()
        }  # Transform python booleans to json booleans
        node_process = subprocess.Popen(
            command,
            shell=True,
            cwd=tmp_dir / TRACKDECHETS_SIRENE_SEARCH_GIT,
            env=environ_clean,
            stdout=subprocess.PIPE,
        )
        # read the output
        while True:
            line = node_process.stdout.readline()
            if not line:
                break
            read_output(line)

        while node_process.wait():
            if node_process.returncode != 0:
                logger.error(node_process.stderr)
                logger.error(node_process.stdout)
                logger.error(node_process.stdin)
                raise Exception(node_process)

        return str(tmp_dir)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def task_cleanup_tmp_files(tmp_dir: str):
        """Clean DAG's artifacts"""
        shutil.rmtree(tmp_dir)

    """
    Dag workflow
    """
    tmp_dir = task_git_clone_trackdechets()
    (
        task_npm_install_build(tmp_dir)
        >> task_download_es_ca_pem(tmp_dir)
        >> task_npm_run_index(tmp_dir)
        >> task_cleanup_tmp_files(tmp_dir)
    )


trackdechets_search_sirene_dag = full_update_search_sirene()

if __name__ == "__main__":
    trackdechets_search_sirene_dag.test()
