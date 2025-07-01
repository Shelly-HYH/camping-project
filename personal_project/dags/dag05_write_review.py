from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import utils.l02_write_review_to_db as l02
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
    dag_id="05_l_write_review_data",
    default_args=default_args,
    description="把評論資訊寫入MySQL",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["review", "to_db"]
)

def l02_write_reviews():

    @task
    def write_data():
        session = l02.connect_db()
        df = l02.load_data()
        try:
            l02.write_data(session, df)
        finally:
            session.close()

    trigger_next = TriggerDagRunOperator(
        task_id="trigger_dag06",
        trigger_dag_id="06_mart_cut_keywords",  # 對應的 DAG id
        wait_for_completion=False,
        reset_dag_run=True
    )

    result = write_data()
    
    result >> trigger_next
    
    return result, trigger_next

l02_write_reviews()
