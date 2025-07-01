from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import utils.t01_clean_campground as t01
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
    dag_id="02_t_clean_campground",
    default_args=default_args,
    description="清洗露營場資料",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["raw data", "campground"]  # Optional: Add tags for better filtering in the UI
)

def t01_clean_campground():

    save_path = Path("output") 
    raw_file = save_path / "All_Campground_final.csv"
    step1_file = save_path / "campground_clean_step1.csv"
    step2_file = save_path / "campground_clean_step2.csv"
    step3_file = save_path / "campground_clean_step3.csv"
    final_file = save_path / "final_campground_clean.csv"

    @task
    def read_raw_data():
        df = pd.read_csv(raw_file, encoding="utf-8-sig")
        df = df[df["Campground"] != "安平漁人馬桶"]

        # 暫存處理後結果
        df.to_csv(step1_file, index=False, encoding="utf-8-sig")

        return str(step1_file)

    @task
    def task01(input_path):
        df = pd.read_csv(input_path, encoding="utf-8-sig")
        # 清理名稱
        df["Campground"] = t01.clean_campground_name(df["Campground"])
        
        #去重
        df["Campground_clean"] = df["Campground"].apply(t01.get_prefix)
        df = t01.clean_campground_duplicate(df)

        #清理地址跟評論數
        df["address_clean"] = t01.clean_address(df["Address"])
        # df["Reviews"] = t01.clean_review_number(df["Reviews"])

        # 刪除不用欄位
        df = df.drop(columns=["名稱字數", "Name_map", "Campground_clean", "Address"], errors="ignore")
        
        # 暫存處理後結果
        df.to_csv(step2_file, index=False, encoding="utf-8-sig")

        return str(step2_file)

    @task
    def task02(input_path):
        df = pd.read_csv(input_path, encoding="utf-8-sig")
        df_county = t01.load_county_id()
        result_df = t01.add_county_id(df_county, df)

        # 暫存處理後結果
        result_df.to_csv(step3_file, index=False, encoding="utf-8-sig")

        return str(step3_file)

    @task
    def save_result(input_path):
        df = pd.read_csv(input_path, encoding="utf-8-sig")

        df.to_csv(final_file, index=False, encoding="utf-8-sig")
        print(f"成功儲存共{len(df)}筆資料到{final_file}")


    # 觸發下一個DAG
    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_dag03",
        trigger_dag_id="03_l_write_campground_to_mysql",  # 對應的 DAG id
        wait_for_completion=False,
        reset_dag_run=True
    )
        
    step1_path = read_raw_data()
    step2_path = task01(step1_path)
    step3_path = task02(step2_path)
    save_result(step3_path) >> trigger_clean

    return save_result, trigger_clean
   
t01_clean_campground()
