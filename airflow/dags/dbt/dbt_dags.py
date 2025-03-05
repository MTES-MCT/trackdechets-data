from datetime import datetime

from cosmos import DbtDag, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import TestBehavior

from dbt.profile import profile_config


def dag_generator(
    dag_id: str, select: list[str], schedule: str, profile_config: ProfileConfig
):
    return DbtDag(
        project_config=ProjectConfig(
            "/opt/airflow/dbt",
        ),
        profile_config=profile_config,
        operator_args={
            "install_deps": True,  # install any necessary dependencies before running any dbt command
        },
        # selection
        render_config=RenderConfig(
            select=select,
            exclude=["refined_zone.analytics.ad_hoc"],
            test_behavior=TestBehavior.AFTER_ALL,
        ),
        # normal dag parameters
        schedule_interval=schedule,
        start_date=datetime(2023, 1, 1),
        catchup=False,
        dag_id=dag_id,
        default_args={"retries": 0},
        concurrency=2,
    )


dbt_trackdechets_dag = dag_generator(
    dag_id="dbt_trackdechets",
    select=["trusted_zone.trackdechets+"],
    schedule="@daily",
    profile_config=profile_config,
)

dbt_gerico_dag = dag_generator(
    dag_id="dbt_gerico",
    select=["trusted_zone.gerico+"],
    schedule="5 0/4 * * *",
    profile_config=profile_config,
)

dbt_icpe_dag = dag_generator(
    dag_id="dbt_icpe",
    select=["trusted_zone.icpe+"],
    schedule="@daily",
    profile_config=profile_config,
)

dbt_zammad_dag = dag_generator(
    dag_id="dbt_zammad",
    select=["trusted_zone.zammad+"],
    schedule="0 4 * * *",
    profile_config=profile_config,
)
