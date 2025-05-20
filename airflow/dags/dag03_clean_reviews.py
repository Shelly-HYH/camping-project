from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
import utils.t02_final_clean_review as t02
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
    dag_id="03_t_clean_reviews",
    default_args=default_args,
    description="清洗評論raw data",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    tags=["raw data", "reviews"]  # Optional: Add tags for better filtering in the UI
)

def t02_clean_reviews():
    
    wait_for_t01 = ExternalTaskSensor(
        task_id="wait_for_t01",
        external_dag_id="02_t_clean_campground",
        external_task_id="__all__",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="poke",
        poke_interval=60,
        timeout=60 * 60 * 1
    )

    @task
    def read_raw_data():
        input_path = Path("/home/Tibame/tjr101_project") / "camp_reviews_final.csv"
        df = pd.read_csv(input_path, encoding="utf-8-sig")
        df = df[df["Campsite"] != "安平漁人馬桶"]
        return df.to_json(orient="split")
    
    @task
    def task01(json_df):
        df = pd.read_json(json_df, orient="split")
        df["Campsite"] = t02.clean_campground_name(df["Campsite"])
        return df.to_json(orient="split") 
    
    @task
    def task02(json_df):
        df = pd.read_json(json_df, orient="split")
        df = t02.clean_campground_duplicate(df)
        return df.to_json(orient="split")
    
    @task
    def save_campers(json_df):
        df = pd.read_json(json_df, orient="split")
        t02.take_campers(df["Reviewer"])

    @task
    def save_result(json_df):
        save_path = Path("/home/Tibame/tjr101_project") 
        output_path = save_path / "reviews_final_clean.csv"

        df = pd.read_json(json_df, orient="split")
        cols = df.columns.tolist()
        df["Name"] = "shelly"
        cols.insert(0, cols.pop(cols.index("Name")))
        df = df[cols]

        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"成功儲存清洗結果，共 {len(df)} 筆資料到 {output_path}")
    
    raw = read_raw_data()
    wait_for_t01 >> raw
    
    step1 = task01(raw)
    step2 = task02(step1)
    save_campers(step2)
    save_result(step2)

t02_clean_reviews()
