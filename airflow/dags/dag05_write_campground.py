from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import utils.l02_write_campground_to_mysql as l02
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
    dag_id="05_l_write_campground_to_mysql",
    default_args=default_args,
    description="campground寫入db產生ID",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["mysql", "db"]  # Optional: Add tags for better filtering in the UI
    )

def l02_write_campground_data():

    @task
    def write_data_to_db():
        session = l02.connect_db()
        try:
            l02.write_data(session)
        finally:
            session.close()
    
    # 觸發下一個DAG
    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_t_campground_add_id",
        trigger_dag_id="06_t_campground_add_id",  # 對應的 DAG id
        wait_for_completion=False,
        reset_dag_run=True
    )
    
    result = write_data_to_db() 
    result >> trigger_clean

    return result, trigger_clean

l02_write_campground_data()
    