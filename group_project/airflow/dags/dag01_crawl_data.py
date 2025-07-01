from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from docker.types import Mount
import pendulum
from datetime import timedelta

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
    run_crawler = DockerOperator(
        task_id="run_crawler",
        image="my-crawler-image:latest",
        command="python3 e_gcp_selenium_crawl.py",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="tjr101_project_crawler-net",
        mounts=[
            Mount(source="/home/Tibame/tjr101_project/output", target="/app/output", type="bind")
        ],
        mount_tmp_dir=False,
    )

    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_clean_campground",
        trigger_dag_id="02_t_clean_campground",  # 對應的 DAG id
        wait_for_completion=False,
        reset_dag_run=True
    )

    run_crawler >> trigger_clean

    return run_crawler, trigger_clean



e_crawl_data()