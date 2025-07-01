FROM apache/airflow:2.8.1

ENV TZ=Asia/Taipei

USER root

# 安裝 docker CLI 並修正指令錯誤
RUN apt-get update && apt-get install -y docker.io && apt-get clean

# 建立工作資料夾並設權限
RUN mkdir -p /opt/airflow/utils /opt/airflow/tasks /opt/airflow/output  /opt/airflow/logs && \
    chown -R airflow: /opt/airflow/utils /opt/airflow/tasks && \
    usermod -aG docker airflow


USER airflow

COPY requirements.txt requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt


