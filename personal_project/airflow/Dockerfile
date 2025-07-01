FROM apache/airflow:2.8.1

ENV TZ=Asia/Taipei

USER root

# 安裝 docker CLI 並修正指令錯誤
RUN apt-get update && apt-get install -y docker.io && apt-get clean

# 建立工作資料夾並設權限
RUN mkdir -p /opt/airflow/dags /opt/airflow/utils /opt/airflow/tasks /opt/airflow/output /opt/airflow/logs && \
    chown -R airflow: /opt/airflow/dags /opt/airflow/utils /opt/airflow/tasks /opt/airflow/output /opt/airflow/logs && \
    touch /opt/airflow/utils/__init__.py && \
    touch /opt/airflow/tasks/__init__.py && \
    touch /opt/airflow/dags/__init__.py
    
USER airflow

COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt


