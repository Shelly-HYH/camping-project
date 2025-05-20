from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
import utils.l04_review_to_mysql as l04
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
    dag_id="10_l_write_review_data",
    default_args=default_args,
    description="把評論資訊寫入MySQL",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    tags=["review", "load"]
)

def l04_write_reviews():
    
    wait_for_103 = ExternalTaskSensor(
        task_id="wait_for_l03",
        external_dag_id="09_l_write_campground_extra_data",
        external_task_id="__all__", 
        allowed_states=["success"], 
        failed_states=["failed", "skipped"],
        mode="poke",
        poke_interval=60,
        timeout=60 * 60 * 1 
    )

    @task
    def load_data():
        df = l04.load_data()
        return df.to_json(orient="split")

    @task
    def connect_db():
        session = l04.connect_db()
        return session
    
    @task
    def write_data(session, df_json):
        df = pd.read_json(df_json, orient="split")
        try:
            l04.write_data(session, df)
        finally:
            session.close()
    
    df = load_data()
    wait_for_103 >> df
    session = connect_db()
    write_data(session, df)

l04_write_reviews()
