from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
import utils.t03_create_mysql_database as t03
import pendulum

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
    dag_id="04_create_db",
    default_args=default_args,
    description="創建MYSQL DB",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    tags=["mysql", "db"]  # Optional: Add tags for better filtering in the UI
    )

def t03_create_db():

    wait_for_t02 = ExternalTaskSensor(
        task_id="wait_for_t02", 
        external_dag_id="03_t_clean_reviews",
        external_task_id="__all__",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="poke",
        poke_interval=60,
        timeout=60 * 60 * 1
    )

    @task
    def create_db():
        conn = t03.connect_db()
        try:
            t03.create_tables(conn)
        finally:
            conn.close()
    
    wait_for_t02 >> create_db()

t03_create_db()
