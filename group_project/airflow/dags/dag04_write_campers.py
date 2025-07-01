from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import utils.l01_write_camper_to_mysql as l01
import pendulum

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

@dag(
    dag_id="04_l_write_camper_to_mysql",
    default_args=default_args,
    description="campers寫入db產生ID",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["mysql", "db"]  # Optional: Add tags for better filtering in the UI
    )

def t04_write_camper_data():

    # wait_for_t03 = ExternalTaskSensor(
    #     task_id="wait_for_t03",  # 
    #     external_dag_id="04_create_db",
    #     external_task_id="__all__",
    #     allowed_states=["success"],
    #     failed_states=["failed", "skipped"],
    #     mode="poke",
    #     poke_interval=60,
    #     timeout=60 * 60 * 1,
    #     execution_delta=timedelta(0),
    # )
    
    @task
    def write_data_to_db():
        session = l01.connect_db()
        try:
            l01.write_data(session)
            print("成功寫入資料表")
        finally:
            session.close()
    
    # 觸發下一個DAG
    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_write_campground_to_mysql",
        trigger_dag_id="05_l_write_campground_to_mysql",  # 對應的 DAG id
        wait_for_completion=False,
        reset_dag_run=True
    )
    
    # wait_for_t03 >> session
    result = write_data_to_db() 
    result >> trigger_clean

    return result, trigger_clean

t04_write_camper_data()
    