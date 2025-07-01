from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import utils.t04_add_id_review as t04
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
    dag_id="07_t_review_add_id",
    default_args=default_args,
    description="評論比對MySQL參考表寫入露營場ID",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["review", "campground", "id"]
)

def t04_review_add_id():

    @task
    def read_ref_data():
        ref_df = t04.load_ref()

        # 暫存處理後結果
        output_path = Path("output") / "ref_data.csv"
        ref_df.to_csv(output_path, index=False, encoding="utf-8-sig")

        return str(output_path)
    
    @task
    def read_camper_data():
        camper_df = t04.load_campers_id()

        # 暫存處理後結果
        output_path = Path("output") / "ref_camper_data.csv"
        camper_df.to_csv(output_path, index=False, encoding="utf-8-sig")

        return str(output_path)

    @task
    def merged_id(ref_path, camper_path):
        ref_df = pd.read_csv(ref_path, encoding="utf-8-sig")
        camper_df = pd.read_csv(camper_path, encoding="utf-8-sig")

        input_path = Path("output") / "reviews_final_clean.csv"
        df = pd.read_csv(input_path, encoding="utf-8-sig")

        merged_df = t04.merged_id(df, ref_df, camper_df)

        # 暫存處理後結果
        output_path = Path("output") / "reviews_merge_step1.csv"
        merged_df.to_csv(output_path, index=False, encoding="utf-8-sig")

        return str(output_path)
    
    @task
    def clean_final_data(input_path):
        df = pd.read_csv(input_path, encoding="utf-8-sig")
        result_df = t04.clean_data(df)

        output_path = Path("output") / "reviews_merge_step2.csv"
        result_df.to_csv(output_path, index=False, encoding="utf-8-sig")

        return str(output_path)
    
    @task
    def save_file(input_path):
        df = pd.read_csv(input_path, encoding="utf-8-sig")
        t04.save_file(df)
    
    # 觸發下一個DAG
    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_write_campground_extra_data",
        trigger_dag_id="08_l_write_campground_extra_data",  # 對應的 DAG id
        wait_for_completion=False,
        reset_dag_run=True
    )

    ref_path = read_ref_data()
    camper_path = read_camper_data()
    merged_path = merged_id(ref_path, camper_path)
    result_path = clean_final_data(merged_path)
    final_step = save_file(result_path) 
    final_step >> trigger_clean

    return final_step, trigger_clean

t04_review_add_id()
