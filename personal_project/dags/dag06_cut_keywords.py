from datetime import timedelta
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
from pathlib import Path
import pandas as pd
import utils.mart01_jieba_cut_text_by_campground as m01


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
    dag_id="06_mart_cut_keywords",
    default_args=default_args,
    description="評論擷取關鍵字",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["review", "keywords"]
)

def mart01_keywords():
    read_path = Path("output")
    
    @task
    def load_reviews():
        df = m01.load_data()
        raw_data_path = read_path / "articles.csv"
        df.to_csv(raw_data_path, index=False, encoding="utf-8-sig")
        return str(raw_data_path)

    @task
    def jieba_keywords(input_path):
        df = pd.read_csv(input_path, encoding="utf-8-sig")

        # 讀取自訂的停用詞清單
        file_path = Path("myfiles")
        stopwords_path = file_path / "stopwords.txt"
        with open(stopwords_path, "r", encoding="utf-8") as f:
            stopwords = set(line.strip() for line in f)

        df2 = m01.group_content(df, stopwords)
        step01_path = read_path / "cut_keywords_step01.csv"
        df2.to_csv(step01_path , index=False, encoding="utf-8-sig")
        return str(step01_path )
    
    
    @task
    def save_result(input_path):
        df = pd.read_csv(input_path, encoding="utf-8-sig")
        m01.save_file(df)
    
    # 觸發下一個DAG
    trigger_next = TriggerDagRunOperator(
        task_id="trigger_dag07",
        trigger_dag_id="07_mart_ai_add_tag",  # 對應的 DAG id
        wait_for_completion=False,
        reset_dag_run=True
    )

    step1_path = load_reviews()
    step2_path = jieba_keywords(step1_path)
    save_result(step2_path) >> trigger_next
    return save_result, trigger_next

mart01_keywords()

