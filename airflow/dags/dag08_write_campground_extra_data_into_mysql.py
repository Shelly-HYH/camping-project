from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import utils.l03_campground_extrainfo_to_mysql as l03
import pendulum
import pandas as pd

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
    dag_id="08_l_write_campground_extra_data",
    default_args=default_args,
    description="把露營場其他資訊寫入MySQL",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["campground", "load"]  # Optional: Add tags for better filtering in the UI
)

def l03_write_campground_extra_data():
    
    @task
    def write_data():
        df = l03.load_data()
        session = l03.connect_db()
        try:
            l03.write_data(session, df)
        finally:
            session.close()

    # 觸發下一個DAG
    trigger_clean = TriggerDagRunOperator(
        task_id="trigger__write_review_data",
        trigger_dag_id="09_l_write_review_data",  # 對應的 DAG id
        wait_for_completion=False,
        reset_dag_run=True
    )

    result = write_data() 
    result >> trigger_clean

    return result, trigger_clean

l03_write_campground_extra_data()
