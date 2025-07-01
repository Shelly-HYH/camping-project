from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import utils.t03_add_id_campground as t03
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
    dag_id="06_t_campground_add_id",
    default_args=default_args,
    description="比對參考表寫入露營場ID",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["campground", "id"]  # Optional: Add tags for better filtering in the UI
)

def t03_campground_add_id():

    @task
    def read_ref_data():
        ref_df = t03.load_ref()

        # 暫存處理後結果
        output_path = Path("output") / "ref_data.csv"
        ref_df.to_csv(output_path, index=False, encoding="utf-8-sig")

        return str(output_path)
    
    @task
    def merged_id(ref_input_path):
        # campground data
        input_path = Path("output") / "final_campground_clean.csv"
        df1 = pd.read_csv(input_path, encoding="utf-8-sig")

        # ref campground id
        df_ref = pd.read_csv(ref_input_path, encoding="utf-8-sig")

        # merged
        df2 = t03.merge_data(df1 , df_ref)
        
        # 暫存處理後結果
        output_path = Path("output") / "campground_addID_step1.csv"
        df2.to_csv(output_path, index=False, encoding="utf-8-sig")

        return str(output_path)

    @task
    def final_clean_data(input_path):
        df1 = pd.read_csv(input_path, encoding="utf-8-sig")
        df2 = t03.final_clean(df1)

        # 暫存處理後結果
        output_path = Path("output") / "campground_addID_step2.csv"
        df2.to_csv(output_path, index=False, encoding="utf-8-sig")

        return str(output_path)

    @task
    def save_result(input_path):

        save_path = Path("output")
        output_path = save_path / "add_id_campground.csv"

        df = pd.read_csv(input_path, encoding="utf-8-sig")
        df.to_csv(output_path, index=False, encoding="utf-8-sig")

        print(f"成功儲存清洗結果，共{len(df)}筆資料到 {output_path}")

    # 觸發下一個DAG
    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_t_review_add_id",
        trigger_dag_id="07_t_review_add_id",  # 對應的 DAG id
        wait_for_completion=False,
        reset_dag_run=True
    )

    ref_path = read_ref_data()
    step1_path = merged_id(ref_path)
    step2_path = final_clean_data(step1_path)
    result = save_result(step2_path) 

    result >> trigger_clean

    return result, trigger_clean

t03_campground_add_id()

