from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
from datetime import timedelta
import utils.e_gcp_selenium_crawl as crawl

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

@dag(
    dag_id="01_e_crawl_google_map",
    default_args=default_args,
    description="selenium爬google map",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["crawl", "google map"]  # Optional: Add tags for better filtering in the UI
)

def e_crawl_data():

    @task
    def crawl_data():
        try:
            crawl.main()
        except Exception as e:
            raise RuntimeError(f"Crawling 發生錯誤: {e}")

    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_dag02",
        trigger_dag_id="02_t_clean_campground",  # 對應的 DAG id
        wait_for_completion=False,
        reset_dag_run=True
    )
    crawl_task = crawl_data()
    crawl_task >> trigger_clean

    return crawl_task, trigger_clean

e_crawl_data()
