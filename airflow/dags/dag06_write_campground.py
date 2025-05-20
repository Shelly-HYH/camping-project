from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
import utils.l02_write_campground_to_mysql as l02
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
    dag_id="06_l_write_campground_to_mysql",
    default_args=default_args,
    description="campground寫入db產生ID",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    tags=["mysql", "db"]  # Optional: Add tags for better filtering in the UI
    )

def l02_write_campground_data():

    wait_for_l01 = ExternalTaskSensor(
        task_id="wait_for_l01",  # 
        external_dag_id="05_l_write_camper_to_mysql",
        external_task_id="__all__",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="poke",
        poke_interval=60,
        timeout=60 * 60 * 1
    )

    @task
    def connect_db():
        session = l02.connect_db()
        return session
    
    @task
    def write_data(session):
        try:
            l02.write_data(session)
        finally:
            session.close()
    
    session = connect_db()
    wait_for_l01 >> session
    write_data(session)

l02_write_campground_data()
    