from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import utils.t02_clean_review as t02
import pendulum
import pandas as pd
from pathlib import Path

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
    dag_id="04_t_clean_reviews",
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
        df = df[df["Campground"] != "安平漁人馬桶"]

        # 處理露營場名稱
        df["Campground"] = t02.clean_campground_name(df["Campground"])
        
        # 暫存處理後結果
        output_path = Path("output") / "reviews_clean_step1.csv"
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        return str(output_path)
    
    @task
    def task02(input_path):
        df = pd.read_csv(input_path, encoding="utf-8-sig")

        # 去重
        df = t02.clean_campground_duplicate(df)

        output_path = Path("output") / "reviews_clean_step2.csv"
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        return str(output_path)

    @task
    def task03(input_path):
        df = pd.read_csv(input_path, encoding="utf-8-sig")
        
        # 讀取露營場表
        df_ref = t02.load_ref()
        
        # 合併ID
        df2 = t02.merged_id(df, df_ref)
        
        # 清理資料
        df3 = t02.clean_data(df2)

        output_path = Path("output") / "reviews_clean_step3.csv"
        df3.to_csv(output_path, index=False, encoding="utf-8-sig")
        return str(output_path)

    @task
    def save_result(input_path):
        df = pd.read_csv(input_path, encoding="utf-8-sig")
        t02.save_file(df)

    
    # 觸發下一個DAG
    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_dag05",
        trigger_dag_id="05_l_write_review_data",  # 對應的 DAG id
        wait_for_completion=False,
        reset_dag_run=True
    )
    
    raw_path = read_raw_data()    
    step1 = task01(raw_path)
    step2 = task02(step1)
    step3 = task03(step2)
    save_result(step3) >> trigger_clean

    return save_result, trigger_clean

t02_clean_reviews()
