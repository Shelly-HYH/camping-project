from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import utils.create_mysql_database as create
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
    dag_id="00_create_db",
    default_args=default_args,
    description="創建MYSQL DB",
    schedule_interval="0 0 * * 0",
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["mysql", "db"]  # Optional: Add tags for better filtering in the UI
    )

def create_mysql_db():

    @task
    def create_db():
        conn = create.connect_db()
        try:
            create.create_tables(conn)
        finally:
            print("成功建立表單")


    # 觸發下一個DAG
    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_crawl_data",
        trigger_dag_id="01_e_crawl_google_map",  # 對應的 DAG id
        wait_for_completion=False,
        reset_dag_run=True
    )

    create_db() >> trigger_clean

    return create_db, trigger_clean

create_mysql_db()
