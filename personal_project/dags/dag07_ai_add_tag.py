from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
from pathlib import Path
import pandas as pd
import utils.mart02_ai_add_campground_tag as m02


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
    dag_id="07_mart_ai_add_tag",
    default_args=default_args,
    description="串接gemini上Tag",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["tag", "keywords"]
)

def ai_add_tags():
    save_path = Path("output", "mart")   
    
    @task
    def add_tags():
        # 載入jieba已經切好的關鍵字檔案
        keywords_file = save_path / "MART01_cut_keyword_groupby_campground.csv"
        df_keyword = pd.read_csv(keywords_file, encoding="utf-8-sig")

        # 設定gemini ai
        model = m02.ai_model()

        # 匯入tag清單並做成字串
        tag_string = m02.load_tag()

        # 串接AI上TAG
        df = m02.ai_add_tag(model, df_keyword, tag_string)
        
        # 清洗關鍵字欄位格式
        result_df = m02.clean_template(df)

        m02.save_result(result_df)
    
    trigger_next = TriggerDagRunOperator(
        task_id="trigger_dag08",
        trigger_dag_id="08_create_view_table",  # 對應的 DAG id
        wait_for_completion=False,
        reset_dag_run=True
    )

    add_tags() >> trigger_next
    return add_tags, trigger_next

ai_add_tags()


