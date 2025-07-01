FROM python:3.12.10

ENV TZ=Asia/Taipei

# 安裝必要套件
RUN apt-get update && \
    apt-get install -y wget gnupg curl unzip \
    fonts-liberation libayatana-appindicator3-1 libasound2 libatk-bridge2.0-0 libatk1.0-0 \
    libcups2 libdbus-1-3 libgdk-pixbuf2.0-0 libnspr4 libnss3 libx11-xcb1 \
    libxcomposite1 libxdamage1 libxrandr2 xdg-utils lsb-release \
    libgbm-dev && \
    rm -rf /var/lib/apt/lists/*

# 安裝穩定版 Chrome
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google-linux-signing-keyring.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux-signing-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    apt-get install -y google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

# 設定工作目錄
WORKDIR /app

# 複製 requirements
COPY requirements.txt .

# 安裝 python 套件
RUN pip install --upgrade pip && pip install -r requirements.txt

# 複製爬蟲檔案
COPY *.py .

# 預設啟動
CMD ["python3", "e_gcp_selenium_crawl.py"]
