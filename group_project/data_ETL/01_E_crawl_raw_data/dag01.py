from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import pendulum

# test change to force reload
with DAG(
    dag_id="crawl-google-map",
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    schedule="0 11 * * 6",
    catchup=False,
) as dag:
    run_crawler = DockerOperator(
        task_id="run_crawler",
        image="my-crawler-image:latest", 
        command="python3 /app/e_gcp_selenium_crawl.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove=True,
        mounts=[
            Mount(source="/home/Tibame/tjr101_project", target="/app", type="bind"),
        ], 
        mount_tmp_dir=False,
        shm_size="1g",
        mem_limit='4g',
        timeout=1200
    )

    run_crawler
