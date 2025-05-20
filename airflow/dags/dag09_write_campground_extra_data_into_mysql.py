from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
import utils.l03_campground_extrainfo_to_mysql as l03
import pendulum
import pandas as pd

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["sia1940@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

@dag(
    dag_id="09_l_write_campground_extra_data",
    default_args=default_args,
    description="把露營場其他資訊寫入MySQL",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    tags=["campground", "load"]  # Optional: Add tags for better filtering in the UI
)

def l03_write_campground_extra_data():
    
    wait_for_t05 = ExternalTaskSensor(
        task_id="wait_for_t05",
        external_dag_id="08_t_review_add_id",
        external_task_id="__all__", 
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="poke", 
        poke_interval=60, 
        timeout=60 * 60 * 1
    )

    @task
    def load_data():
        df = l03.load_data()
        return df.to_json(orient="split")
    
    @task
    def connect_db():
        session = l03.connect_db()
        return session
    
    @task
    def write_data(session, df_json):
        df = pd.read_json(df_json, orient="split")
        try:
            l03.write_data(session, df)
        finally:
            session.close()
    
    df = load_data()
    wait_for_t05 >> df
    session = connect_db()
    write_data(session, df)

l03_write_campground_extra_data()
