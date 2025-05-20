from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
import utils.l01_write_camper_to_mysql as l01
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
    dag_id="05_l_write_camper_to_mysql",
    default_args=default_args,
    description="campers寫入db產生ID",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    tags=["mysql", "db"]  # Optional: Add tags for better filtering in the UI
    )

def t04_write_camper_data():

    wait_for_t03 = ExternalTaskSensor(
        task_id="wait_for_t03",  # 
        external_dag_id="04_create_db",
        external_task_id="__all__",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="poke",
        poke_interval=60,
        timeout=60 * 60 * 1
    )

    @task
    def connect_db():
        session = l01.connect_db()
        return session
    
    @task
    def write_data(session):
        try:
            l01.write_data(session)
        finally:
            session.close()
    
    session = connect_db()
    wait_for_t03 >> session
    write_data(session)

t04_write_camper_data()
    