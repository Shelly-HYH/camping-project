from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import utils.t02_final_clean_review as t02
import pendulum
import pandas as pd
from pathlib import Path

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
    dag_id="03_t_clean_reviews",
    default_args=default_args,
    description="清洗評論raw data",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["raw data", "reviews"]  # Optional: Add tags for better filtering in the UI
)

def t02_clean_reviews():

    @task
    def read_raw_data():
        input_path = Path("output") / "camp_reviews_final.csv"
        return str(input_path)
    
    @task
    def task01(input_path):
        df = pd.read_csv(input_path, encoding="utf-8-sig")
        df = df[df["Campsite"] != "安平漁人馬桶"]
        df["Campsite"] = t02.clean_campground_name(df["Campsite"])
        
        # 暫存處理後結果
        output_path = Path("output") / "reviews_clean_step1.csv"
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        return str(output_path)
    
    @task
    def task02(input_path):
        df = pd.read_csv(input_path, encoding="utf-8-sig")
        df = t02.clean_campground_duplicate(df)

        output_path = Path("output") / "reviews_clean_step2.csv"
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        return str(output_path)
    
    @task
    def save_campers(input_path):
        df = pd.read_csv(input_path, encoding="utf-8-sig")
        t02.take_campers(df["Reviewer"])

    @task
    def save_result(input_path):
        save_path = Path("output") 
        output_path = save_path / "reviews_final_clean.csv"

        df = pd.read_csv(input_path, encoding="utf-8-sig")
        df.insert(0, "Name", "shelly")

        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"成功儲存清洗結果，共 {len(df)} 筆資料到 {output_path}")
    
    # 觸發下一個DAG
    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_write_camper",
        trigger_dag_id="04_l_write_camper_to_mysql",  # 對應的 DAG id
        wait_for_completion=False,
        reset_dag_run=True
    )
    
    raw_path = read_raw_data()    
    step1 = task01(raw_path)
    step2 = task02(step1)
    save_campers(step2)
    save_result(step2) >> trigger_clean

    return save_campers, save_result, trigger_clean

t02_clean_reviews()
