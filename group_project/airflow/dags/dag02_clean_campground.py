from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import utils.t01_final_clean_campground as t01
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
    dag_id="02_t_clean_campground",
    default_args=default_args,
    description="清洗露營場基本資料",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["raw data", "campground"]  # Optional: Add tags for better filtering in the UI
)

def t01_clean_campground():

    save_path = Path("output") 
    input_path = save_path / "All_campsite_final.csv"
    output_path = save_path / "final_campground_clean.csv"

    @task
    def read_raw_data():
        df = pd.read_csv(input_path, encoding="utf-8-sig")
        df = df[df["Campsite"] != "安平漁人馬桶"]

        # 暫存處理後結果
        output_path = Path("output") / "campground_clean_step1.csv"
        df.to_csv(output_path, index=False, encoding="utf-8-sig")

        return str(output_path)

    @task
    def task01(input_path):
        df = pd.read_csv(input_path, encoding="utf-8-sig")
        df["Campsite"] = t01.clean_campground_name(df["Campsite"])
        df["Campsite_clean"] = df["Campsite"].apply(t01.get_prefix)
        df = t01.clean_campground_duplicate(df)

        # 暫存處理後結果
        output_path = Path("output") / "campground_clean_step2.csv"
        df.to_csv(output_path, index=False, encoding="utf-8-sig")

        return str(output_path)

    @task
    def task02(input_path):
        df = pd.read_csv(input_path, encoding="utf-8-sig")
        df["Address"] = t01.clean_address(df["Address"])
        df = t01.change_address(df)

        # 暫存處理後結果
        output_path = Path("output") / "campground_clean_step3.csv"
        df.to_csv(output_path, index=False, encoding="utf-8-sig")

        return str(output_path)

    @task
    def task03(input_path):
        df = pd.read_csv(input_path, encoding="utf-8-sig")
        df["Reviews"] = t01.clean_review_number(df["Reviews"])

        # 暫存處理後結果
        output_path = Path("output") / "campground_clean_step4.csv"
        df.to_csv(output_path, index=False, encoding="utf-8-sig")

        return str(output_path)
    
    @task
    def task04(input_path):
        df = pd.read_csv(input_path, encoding="utf-8-sig")
        df_county = t01.load_county_id()
        result_df = t01.add_county_id(df_county, df)

        # 暫存處理後結果
        output_path = Path("output") / "campground_clean_step5.csv"
        result_df.to_csv(output_path, index=False, encoding="utf-8-sig")

        return str(output_path)

    @task
    def save_result(input_path):
        df = pd.read_csv(input_path, encoding="utf-8-sig")
        df.insert(0, "Name", "shelly")

        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"成功儲存共{len(df)}筆資料到{output_path}")

        # 儲存露營場清單
        t01.save_campground_list(df)

    # 觸發下一個DAG
    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_clean_reviews",
        trigger_dag_id="03_t_clean_reviews",  # 對應的 DAG id
        wait_for_completion=False,
        reset_dag_run=True
    )
        
    step1_path = read_raw_data()
    step2_path = task01(step1_path)
    step3_path = task02(step2_path)
    step4_path = task03(step3_path)
    step5_path = task04(step4_path)
    save_result(step5_path) >> trigger_clean

    return save_result, trigger_clean
   
t01_clean_campground()
