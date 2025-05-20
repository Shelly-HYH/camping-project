from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
import utils.t05_add_id_review as t05
import pendulum
import pandas as pd
from pathlib import Path

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
    dag_id="08_t_review_add_id",
    default_args=default_args,
    description="評論比對MySQL參考表寫入露營場ID",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    tags=["review", "campground", "id"]
)

def t05_review_add_id():
    
    wait_for_t04 = ExternalTaskSensor(
        task_id="wait_for_t04",
        external_dag_id="07_t_campground_add_id",
        external_task_id="__all__",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="poke",
        poke_interval=60, 
        timeout=60 * 60 * 1
    )

    @task
    def read_ref_data():
        ref_df = t05.load_ref()
        return ref_df.to_json(orient="split")
    
    @task
    def read_camper_data():
        camper_df = t05.load_campers_id()
        return camper_df.to_json(orient="split")

    @task
    def merged_id(ref_json, camper_json):
        ref_df = pd.read_json(ref_json, orient="split")
        camper_df = pd.read_json(camper_json, orient="split")

        input_path = Path("/home/Tibame/tjr101_project") / "final_clean_reviews.csv"
        df = pd.read_csv(input_path, encoding="utf-8-sig")

        merged_df = t05.merged_id(df, ref_df, camper_df)
        return merged_df.to_json(orient="split")
    
    @task
    def clean_final_data(merged_json):
        df = pd.read_json(merged_json, orient="split")
        result_df = t05.clean_data(df)
        return result_df.to_json(orient="split")
    
    @task
    def save_file(result_json):
        df = pd.read_json(result_json, orient="split")
        t05.save_file(df)
    
    ref = read_ref_data()
    wait_for_t04 >> ref
    camper = read_camper_data()
    merged = merged_id(ref, camper)
    result = clean_final_data(merged)
    save_file(result)

t05_review_add_id()
