# 熱門露營場、好評差評等資訊寫入MySQL

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, VARCHAR, FLOAT, DECIMAL
import pandas as pd
from pathlib import Path

# 資料處理-------------------
# 載入MART02_campground_add_extra_info.csv

file_path = Path(".venv", "MART", "result_csv")
info_file = file_path / "MART02_campground_add_extra_info.csv"
df = pd.read_csv(info_file, encoding="utf-8-sig")

# 以下欄位如果全部都是空的資料就刪除
columns_to_check = [
    "campground_category", # 好評差評
    "hot", # 熱門
    "negative_ratio", # 負評比例
    "positive_ratio", # 正評比例
    "hot_but_negative" # 熱門但負評高
]
df = df.dropna(subset=columns_to_check, how="all")

# 刪除露營場表已經有的欄位
df.drop(columns=["address", "county_ID", "total_rank", "total_comments_count", "altitude", "traffic_rating", "bathroom_rating", "view_rating", "service_rating", "facility_rating"], inplace=True)

# 增加後續分析時計算用的欄位
df["count"] = 1

# # 合併經緯度資料--------------------------------
# # 載入MART03_address_change_to_latitude.csv
# geo_file = file_path / "MART03_address_change_to_latitude.csv"
# df_geo = pd.read_csv(geo_file, encoding="utf-8-sig")

# result_df = df.merge(df_geo[["campground_ID", "lat", "lng"]], on="campground_ID", how="left")

# # 存檔--------------------------------
# save_name = file_path / "MART04_cleaned_campground_with_emption.csv"
# result_df.to_csv(save_name, encoding="utf-8-sig", index=False)

# print("OK")

# 寫入MySQL表單-------------------
# 已寫入，勿再重複執行
# 已寫入，勿再重複執行
# 已寫入，勿再重複執行

# 建立連線
# host='104.199.214.113' # 主機位置
# user='test' # 使用者名稱
# port="3307" # 埠號
# password='PassWord_1' # 密碼
# url = f"mysql+pymysql://{user}:{password}@{host}:{port}/test2_db"

# engine = create_engine(url, echo=True)

# Base = declarative_base()
# DBSession = sessionmaker(bind=engine)
# session = DBSession()


# MySQL需要先建好表單

class MART_campground_with_emtion_geo(Base):
    __tablename__ = 'MART_campground_with_emotion_geo'
    campground_emotion_ID = Column(Integer, primary_key=True)
    campground_ID = Column(Integer, nullable=False)
    camping_site_name = Column(VARCHAR(40), nullable=False)
    campground_category = Column(VARCHAR(5), nullable=False)
    hot = Column(VARCHAR(5), nullable=False)
    negative_ratio = Column(FLOAT, nullable=False)
    positive_ratio = Column(FLOAT, nullable=False)
    hot_but_negative = Column(VARCHAR(15), nullable=False)
    # latitude = Column(DECIMAL(10,7), nullable=False)
    # longitude = Column(DECIMAL(10,7), nullable=False)
    count = Column(Integer, nullable=False)

# 寫入-------------------
records = []

def safe_value(value):
    if pd.isna(value):
        return None
    return value


for i in range(len(df)):
    row = df.iloc[i]
    record = MART_campground_with_emtion_geo(
        campground_ID=int(row["campground_ID"]),
        camping_site_name=str(row["camping_site_name"]),
        campground_category=safe_value(row["campground_category"]),
        hot=safe_value(row["hot"]),
        negative_ratio=safe_value(row["negative_ratio"]),
        positive_ratio=safe_value(row["positive_ratio"]),
        hot_but_negative=safe_value(row["hot_but_negative"]),
        # latitude=float(row["lat"]),
        # longitude=float(row["lng"]),
        count=int(row["count"])
    )
    records.append(record)

# 一次寫入全部
try:
    session.add_all(records)
    session.commit()
    print(f"資料寫入成功，共寫入{len(records)}筆")
except Exception as e:
    session.rollback()
    print("資料寫入失敗:", e)
finally:
    session.close()

