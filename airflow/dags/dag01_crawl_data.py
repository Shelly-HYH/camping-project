from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
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
    schedule_interval="0 0 * * 0",
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    tags=["crawl", "google map"]  # Optional: Add tags for better filtering in the UI
)

def e_crawl_data():
    run_crawler = DockerOperator(
        task_id="run_crawler",
        image="my-crawler-image:latest", 
        command="python3 /app/e_gcp_selenium_crawl.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove=True,
        mount_tmp_dir=False,
        shm_size="1g",
        mem_limit='4g',
        timeout=1200
    )

    run_crawler()

e_crawl_data()