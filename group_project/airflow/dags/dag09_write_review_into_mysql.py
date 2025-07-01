from datetime import timedelta
from airflow.decorators import dag, task
import utils.l04_review_to_mysql as l04
import pendulum
import pandas as pd

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
    dag_id="09_l_write_review_data",
    default_args=default_args,
    description="把評論資訊寫入MySQL",
    schedule_interval=None,
    start_date=pendulum.yesterday(tz="Asia/Taipei"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["review", "load"]
)

def l04_write_reviews():

    @task
    def write_data():
        session = l04.connect_db()
        df = l04.load_data()
        try:
            l04.write_data(session, df)
        finally:
            session.close()

    result = write_data()
    
    return result


l04_write_reviews()
