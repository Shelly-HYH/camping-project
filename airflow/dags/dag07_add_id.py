from datetime import timedelta
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
import utils.t04_add_id_campground as t04
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
    dag_id="07_t_campground_add_id",
    default_args=default_args,
    description="比對參考表寫入露營場ID",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    tags=["campground", "id"]  # Optional: Add tags for better filtering in the UI
)

def t04_campground_add_id():
    
    wait_for_l02 = ExternalTaskSensor(
        task_id="wait_for_l02",
        external_dag_id="06_l_write_campground_to_mysql",
        external_task_id="__all__",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="poke", 
        poke_interval=60, 
        timeout=60 * 60 * 1 
    )

    @task
    def read_ref_data():
        ref_df = t04.load_ref()
        return ref_df.to_json(orient="split")
    
    @task
    def read_county_data():
        county_df = t04.load_county_id()
        return county_df.to_json(orient="split")
    
    @task
    def read_cleaned_data():
        input_path = Path("/home/Tibame/tjr101_project") / "final_campground_clean.csv"
        df1 = pd.read_csv(input_path, encoding="utf-8-sig")
        return df1.to_json(orient="split")
    
    @task
    def merged_id(county_json, ref_json, df_json):
        county_df = pd.read_json(county_json, orient="split")
        ref_df = pd.read_json(ref_json, orient="split")
        df = pd.read_json(df_json, orient="split")
        df2 = t04.merge_data(df, county_df, ref_df)
        return df2.to_json(orient="split")

    @task
    def final_clean_data(json_df):
        df = pd.read_json(json_df, orient="split")
        df3 = t04.final_clean(df)
        return df3.to_json(orient="split")

    @task
    def save_result(json_df):
        save_path = Path("/home/Tibame/tjr101_project") 
        output_path = save_path / "add_id_campground.csv"

        df = pd.read_json(json_df, orient="split")
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"成功儲存清洗結果，共{len(df)}筆資料到 {output_path}")

    ref = read_ref_data()
    wait_for_l02 >> ref
    county = read_county_data()

    df1 = read_cleaned_data()
    df2 = merged_id(df1, county, ref)
    df3 = final_clean_data(df2)
    save_result(df3)

t04_campground_add_id()

