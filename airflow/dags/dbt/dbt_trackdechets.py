from datetime import datetime

from cosmos import DbtDag, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import TestBehavior
from cosmos.profiles import ClickhouseUserPasswordProfileMapping

from airflow.models import Variable

profile_config = ProfileConfig(
    profile_name="trackdechets",
    target_name=Variable.get("DBT_ENV"),
    profile_mapping=ClickhouseUserPasswordProfileMapping(
        conn_id="td_datawarehouse",
    ),
)

my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        "/opt/airflow/dbt",
    ),
    profile_config=profile_config,
    operator_args={
        "install_deps": True,  # install any necessary dependencies before running any dbt command
    },
    # selection
    render_config=RenderConfig(
        select=["trusted_zone.trackdechets+"],
        exclude=["refined_zone.analytics.ad_hoc"],
        test_behavior=TestBehavior.AFTER_ALL,
    ),
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="dbt_trackdechets",
    default_args={"retries": 0},
    concurrency=2,
)
