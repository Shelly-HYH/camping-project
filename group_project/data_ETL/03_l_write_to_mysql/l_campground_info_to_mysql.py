# 把露營場的總星數跟總評論數寫進MySQL資料庫

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, Float
import pandas as pd
from pathlib import Path

camp_file = Path(r"C:\TJR101_03_Project\.venv\project\ETL_data_to_mysql\add_id_campground.csv")
df = pd.read_csv(camp_file, encoding="utf-8-sig")
print("載入成功")
print("-"*30)

# 檢查空值
print(df["camping_site_name"].isna().sum())
print(df["total_rank"].isna().sum())
print(df["total_comments_count"].isna().sum())
print(df["campground_ID"].isna().sum())
print(df["county_ID"].isna().sum())
print("-"*30)

# 最長名稱字數
max_length = df["camping_site_name"].str.len().max()
print(f"最長的露營場有{max_length}個字")

longest_name = df.loc[df["camping_site_name"].str.len().idxmax(), "camping_site_name"]
print(longest_name)

print("-"*30)


# 資料已寫入，再執行會覆蓋
# ---------------------------

# # 建立連線
# host='104.199.214.113' # 主機位置
# user='test' # 使用者名稱
# port="3307" # 埠號
# password='PassWord_1' # 密碼
# url = f"mysql+pymysql://{user}:{password}@{host}:{port}/test2_db"

# engine = create_engine(url, echo=True)

# Base = declarative_base()
# DBSession = sessionmaker(bind=engine)
# session = DBSession()

# class Campground(Base):
#     __tablename__ = 'campground'
#     campground_ID = Column(Integer, primary_key=True)
#     total_rank = Column(Float, nullable=False)
#     total_comments_count = Column(Integer, nullable=False)

# updated_count = 0
# for _, row in df.iterrows():
#     affected = session.query(Campground).filter_by(campground_ID=row["campground_ID"]).update({
#         Campground.total_rank: row["total_rank"],
#         Campground.total_comments_count: row["total_comments_count"]
#         })
#     if affected:
#         updated_count += 1

# session.commit()
# session.close()


# print(f"共更新 {updated_count} 筆資料")
