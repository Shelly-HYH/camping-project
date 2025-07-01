from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import utils.l01_write_campground_to_db as l01
import pendulum

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

@dag(
    dag_id="03_l_write_campground_to_mysql",
    default_args=default_args,
    description="campground寫入db",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["to_db", "campground"]  # Optional: Add tags for better filtering in the UI
    )

def l01_write_campground_data():

    @task
    def write_data_to_db():
        session = l01.connect_db()
        try:
            l01.write_data(session)
        finally:
            session.close()
    
    # 觸發下一個DAG
    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_dag04",
        trigger_dag_id="04_t_clean_reviews",  # 對應的 DAG id
        wait_for_completion=False,
        reset_dag_run=True
    )
    
    result = write_data_to_db() 
    result >> trigger_clean

    return result, trigger_clean

l01_write_campground_data()
    