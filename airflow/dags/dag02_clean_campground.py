from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
import utils.t01_final_clean_campground as t01
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
    dag_id="02_t_clean_campground",
    default_args=default_args,
    description="清洗露營場基本資料",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    tags=["raw data", "campground"]  # Optional: Add tags for better filtering in the UI
)

def t01_clean_campground():
    wait_for_d01 = ExternalTaskSensor(
        task_id="wait_for_dag01",
        external_dag_id="01_e_crawl_google_map",
        external_task_id="__all__",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="poke",
        poke_interval=60,
        timeout=60 * 60 * 1
    )

    save_path = Path("/home/Tibame/tjr101_project") 
    input_path = save_path / "All_campsite_final.csv"
    output_path = save_path / "final_campground_clean.csv"

    @task
    def read_raw_data():
        df = pd.read_csv(input_path, encoding="utf-8-sig")
        df = df[df["Campsite"] != "安平漁人馬桶"]
        return df.to_json(orient="split")

    @task
    def task01(json_df):
        df = pd.read_json(json_df, orient="split")
        df["Campsite"] = t01.clean_campground_name(df["Campsite"])
        df["Campsite_clean"] = df["Campsite"].apply(t01.get_prefix)
        df = t01.clean_campground_duplicate(df)
        return df.to_json(orient="split")

    @task
    def task02(json_df):
        df = pd.read_json(json_df, orient="split")
        df["Address"] = t01.clean_address(df["Address"])
        df = t01.change_address(df)
        return df.to_json(orient="split")

    @task
    def task03(json_df):
        df = pd.read_json(json_df, orient="split")
        df["Reviews"] = t01.clean_review_number(df["Reviews"])
        return df.to_json(orient="split")

    @task
    def save_result(json_df):
        df = pd.read_json(json_df, orient="split")
        df["Name"] = "shelly"
        cols = df.columns.tolist()
        cols.insert(0, cols.pop(cols.index("Name")))
        df = df[cols]

        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"成功儲存共{len(df)}筆資料到{output_path}")

        # 儲存露營場清單
        t01.save_campground_list(df)
        
    raw = read_raw_data()
    wait_for_d01 >> raw
    
    step1 = task01(raw)
    step2 = task02(step1)
    step3 = task03(step2)
    save_result(step3)
    
t01_clean_campground()
