from datetime import timedelta
from airflow.decorators import dag, task
import utils.mart04_create_view as m04
import utils.mart03_taginfo_write_to_mysql as m03
import pendulum
from pathlib import Path
import pandas as pd

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
    dag_id="08_create_view_table",
    default_args=default_args,
    description="寫入TAG資料並創建n8n查詢用的view表",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["view"]  # Optional: Add tags for better filtering in the UI
    )

def write_tag_and_create_view():
    @task
    def connect_db_and_write():
        conn = m03.connect_db()
        m03.create_tables(conn)

        file_path = Path("output", "mart")
        mart_tag_file = file_path / "MART02_ai_add_campground_tag.csv"
        df = pd.read_csv(mart_tag_file, encoding="utf-8-sig")
        m03.write_data(df)

    @task
    def create_view():
        conn = m04.connect_db()
        try:
            m04.create_tables(conn)

        finally:
            print("成功建立view表")
    
    step1 = connect_db_and_write()
    step2 = create_view()

    step1 >> step2
    
    return step1, step2

write_tag_and_create_view()
