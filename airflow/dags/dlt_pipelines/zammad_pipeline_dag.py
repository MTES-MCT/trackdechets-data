from datetime import timedelta
import os
from airflow.decorators import dag
from airflow.models import Variable

import dlt
from dlt.common import pendulum
from dlt.helpers.airflow_helper import PipelineTasksGroup

from utils.alerting import send_alert_to_mattermost


# modify the default task arguments - all the tasks created for dlt pipeline will inherit it
# - set e-mail notifications
# - we set retries to 0 and recommend to use `PipelineTasksGroup` retry policies with tenacity library, you can also retry just extract and load steps
# - execution_timeout is set to 20 hours, tasks running longer that that will be terminated

default_task_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "execution_timeout": timedelta(hours=20),
}

# modify the default DAG arguments
# - the schedule below sets the pipeline to `@daily` be run each day after midnight, you can use crontab expression instead
# - start_date - a date from which to generate backfill runs
# - catchup is False which means that the daily runs from `start_date` will not be run, set to True to enable backfill
# - max_active_runs - how many dag runs to perform in parallel. you should always start with 1


@dag(
    schedule_interval="@daily",
    start_date=pendulum.datetime(2023, 7, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_task_args,
    on_failure_callback=send_alert_to_mattermost,
)
def load_zammad_data():
    # set `use_data_folder` to True to store temporary data on the `data` bucket. Use only when it does not fit on the local storage
    tasks = PipelineTasksGroup(
        "pipeline_decomposed",
    )

    # import your source from pipeline script
    from dlt_pipelines.zammad_pipeline import zammad_source

    # secrets
    # what you can do is reassign env variables:
    os.environ["DESTINATION__CLICKHOUSE__CREDENTIALS__DATABASE"] = "raw_zone_zammad"
    os.environ["DESTINATION__CLICKHOUSE__CREDENTIALS__PASSWORD"] = Variable.get(
        "DWH_PASSWORD"
    )
    os.environ["DESTINATION__CLICKHOUSE__CREDENTIALS__USERNAME"] = Variable.get(
        "DWH_USERNAME"
    )
    os.environ["DESTINATION__CLICKHOUSE__CREDENTIALS__PORT"] = Variable.get("DWH_PORT")
    os.environ["DESTINATION__CLICKHOUSE__CREDENTIALS__HTTP_PORT"] = Variable.get(
        "DWH_HTTP_PORT"
    )
    os.environ["DESTINATION__CLICKHOUSE__CREDENTIALS__HOST"] = Variable.get("DWH_HOST")
    os.environ["DESTINATION__CLICKHOUSE__CREDENTIALS__SECURE"] = "0"

    # modify the pipeline parameters
    pipeline = dlt.pipeline(
        pipeline_name="zammad_pipeline",
        destination="clickhouse",
        full_refresh=False,  # must be false if we decompose
    )
    # create the source, the "serialize" decompose option will converts dlt resources into Airflow tasks. use "none" to disable it
    tasks.add_run(
        pipeline,
        zammad_source(),
        decompose="serialize",
        trigger_rule="all_done",
        retries=0,
        provide_context=True,
    )


load_zammad_data()
