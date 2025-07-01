FROM python:3.12.10

ENV TZ=Asia/Taipei

# 安裝必要套件
RUN apt-get update && \
    apt-get install -y wget gnupg curl unzip

# 設定工作目錄
WORKDIR /app

# 複製 requirements
COPY crawler/requirements.txt .

# 安裝 python 套件
RUN pip install --upgrade pip && pip install -r requirements.txt

# 複製爬蟲檔案
COPY utils/e_gcp_selenium_crawl.py .

# # 預設啟動
# CMD ["python3", "e_gcp_selenium_crawl.py"]
