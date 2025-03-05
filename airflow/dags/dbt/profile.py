from cosmos import ProfileConfig
from cosmos.profiles import ClickhouseUserPasswordProfileMapping
from airflow.models import Variable

profile_config = ProfileConfig(
    profile_name="trackdechets",
    target_name=Variable.get("DBT_ENV"),
    profile_mapping=ClickhouseUserPasswordProfileMapping(
        conn_id="td_datawarehouse",
    ),
)
