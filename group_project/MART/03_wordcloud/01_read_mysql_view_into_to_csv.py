# 把MySQL的VIEW表下載成csv檔案

from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine

# 建立連線-----------------------
host='104.199.214.113' # 主機位置
user='test' # 使用者名稱
port="3307" # 埠號
password='PassWord_1' # 密碼
url = f"mysql+pymysql://{user}:{password}@{host}:{port}/test2_db"
engine = create_engine(url, echo=True, pool_pre_ping=True)

save_path = Path(".venv", "MART", "data_from_mysql")
# save_path.mkdir(parents=True, exist_ok=True)

# # 用pandas讀取露營場TAG的VIEW表-----------------------
# with engine.connect() as connection:
#     df = pd.read_sql("SELECT * FROM MART_campground_tag_and_geo", con=engine)

# mart_tag_county = save_path / "MySQL_MART_各縣市露營場_TAG及地理資訊表.csv"
# df.to_csv(mart_tag_county, encoding="utf-8-sig", index=False)
# print("MySQL_MART_各縣市露營場_TAG資訊表 OK")

# 用pandas讀取露營場TAG的VIEW表-----------------------
with engine.connect() as connection:
    df = pd.read_sql("SELECT * FROM articles", con=engine)

print(f"內容總字數: {df["content"].str.len().sum()}")
mart_tag_county = save_path / "articles.csv"
df.to_csv(mart_tag_county, encoding="utf-8-sig", index=False)
print("OK")

# # 用pandas讀取露營場熱度的VIEW表-----------------------
# with engine.connect() as connection:
#     df = pd.read_sql("SELECT * FROM MART_campground_emo_geo", con=engine)

# mart_emotion_county = save_path / "MySQL_MART_各縣市露營場_熱度及地理資訊表.csv"
# df.to_csv(mart_emotion_county, encoding="utf-8-sig", index=False)
# print("MySQL_MART_各縣市露營場_熱度與情緒資訊表 OK")


# # 用pandas讀取露營場tag+熱度的VIEW表-----------------------
# with engine.connect() as connection:
#     df = pd.read_sql("SELECT * FROM MART_tag_emo_geo", con=engine)

# mart_emotion_county = save_path / "MySQL_MART_各縣市露營場_熱度及TAG表.csv"
# df.to_csv(mart_emotion_county, encoding="utf-8-sig", index=False)
# print("MySQL_MART_各縣市露營場_熱度+TAG表 OK")


# # 用pandas讀取露營場tag+星數的VIEW表-----------------------
# with engine.connect() as connection:
#     df = pd.read_sql("SELECT * FROM MART_county_tag_rank", con=engine)

# mart_emotion_county = save_path / "MySQL_MART_各縣市露營場_星數評論數及TAG表.csv"
# df.to_csv(mart_emotion_county, encoding="utf-8-sig", index=False)
# print("MySQL_MART_各縣市露營場_星數評論數及TAG表 OK")

# # 用pandas讀取露營場服務表-----------------------
# with engine.connect() as connection:
#     df = pd.read_sql("SELECT * FROM service", con=engine)

# mart_emotion_county = save_path / "MySQL_MART_service表.csv"
# df.to_csv(mart_emotion_county, encoding="utf-8-sig", index=False)
# print("MySQL_MART_各縣市露營場_service表 OK")

# # 用pandas讀取露營場服務表-----------------------
# with engine.connect() as connection:
#     df = pd.read_sql("SELECT * FROM equipment", con=engine)

# mart_emotion_county = save_path / "MySQL_MART_equipment表.csv"
# df.to_csv(mart_emotion_county, encoding="utf-8-sig", index=False)
# print("MySQL_MART_各縣市露營場_equipment表 OK")

# # 用pandas讀取露營場服務表-----------------------
# with engine.connect() as connection:
#     df = pd.read_sql("SELECT * FROM camping_site", con=engine)

# mart_emotion_county = save_path / "MySQL_MART_camping_site表.csv"
# df.to_csv(mart_emotion_county, encoding="utf-8-sig", index=False)
# print("MySQL_MART_各縣市露營場_camping_site表 OK")