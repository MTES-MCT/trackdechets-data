import urllib

from airflow.models import Variable
from mattermost.operators.mattermost_operator import MattermostOperator


# Credit to : https://github.com/mattermost/mattermost-data-warehouse/blob/master/airflow/dags/mattermost_dags/airflow_utils.py
def create_alert_body(context):
    """
    Creates post body to be sent to mattermost channel.
    """

    base_url = "http://localhost:8080"
    execution_date = context["ts"]
    dag_context = context["dag"]
    dag_name = dag_context.dag_id
    dag_id = context["dag"].dag_id
    task_name = context["task"].task_id
    task_id = context["task_instance"].task_id
    error_message = str(context["exception"])

    # Generate the link to the task
    task_params = urllib.parse.urlencode(
        {"dag_id": dag_id, "task_id": task_id, "execution_date": execution_date}
    )
    task_link = f"{base_url}/task?{task_params}"
    dag_link = f"{base_url}/tree?dag_id={dag_id}"

    # TODO create templates for other alerts
    body = f""":red_circle: {error_message}\n**Dag**: [{dag_name}]({dag_link})\n**Task**: [{task_name}]({task_link})"""
    return body


def send_alert_to_mattermost(context):
    """
    Function to be used as a callable for on_failure_callback.
    Sends a post to mattermost channel using mattermost operator
    """
    current_env = Variable.get("AIRFLOW_ENV", "dev")
    if current_env != "prod":
        return

    task_id = str(context["task_instance"].task_id) + "_failed_alert"
    MattermostOperator(
        mattermost_conn_id="mattermost",
        text=create_alert_body(context),
        username="Airflow",
        task_id=task_id,
    ).execute(context)
