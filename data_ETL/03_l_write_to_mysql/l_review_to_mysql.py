# 把所有評論寫進MySQL資料庫
# 資料已經寫入，勿再執行後半段程式碼!會重複寫入!
# 資料已經寫入，勿再執行後半段程式碼!會重複寫入!
# 資料已經寫入，勿再執行後半段程式碼!會重複寫入!

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, VARCHAR, Integer, Date, Float, Text
import pandas as pd
from pathlib import Path


review_file = Path(r".venv/project/ETL_data_to_mysql/add_id_reviews.csv")
df = pd.read_csv(review_file, encoding="utf-8-sig")
print("載入成功")
print("-"*30)

# 檢查空值
print(df["article_rank"].isna().sum())
print(df["publish_date"].isna().sum())
print(df["content"].isna().sum())
print(df["campground_ID"].isna().sum())
print(df["camper_ID"].isna().sum())

# 總共幾篇評論
total_reviews = df["content"].count()
print(f"總篇數: {total_reviews}")

# 最長評論字數
max_length = df["content"].str.len().max()
print(f"最長的評論有 {max_length} 個字")

longest_review = df.loc[df["content"].str.len().idxmax(), "content"]
print("最長評論的內容：")
print(longest_review)

# 所有評論字數
total_length = df["content"].str.len().sum()
print(f"所有評論加總共有 {total_length} 個字")

print("-"*30)

# 4. 日期轉為 datetime.date 型別
df["publish_date"] = pd.to_datetime(df["publish_date"]).dt.date
print("已轉換")


# 資料已經寫入，勿再執行後半段程式碼!會重複寫入!
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

# class review(Base):
#     __tablename__ = 'articles'
#     article_ID = Column(Integer, primary_key=True, autoincrement=True)
#     publish_date = Column(Date, nullable=False)
#     article_rank = Column(Float, nullable=False)
#     content = Column(Text, nullable=False)
#     campground_ID = Column(Integer, nullable=False)
#     camper_ID = Column(Integer, nullable=False)
#     platform_ID = Column(Integer, nullable=False)
#     article_type = Column(VARCHAR(10), nullable=False)

# # 分批寫入
# batch_size = 1000
# for i in range(0, len(df), batch_size):
#     batch = df.iloc[i:i+batch_size]
#     articles = [
#         review(
#             publish_date=row["publish_date"],
#             article_rank=row["article_rank"],
#             content=row["content"],
#             campground_ID=row["campground_ID"],
#             camper_ID=row["camper_ID"],
#             platform_ID=row["platform_ID"],
#             article_type=row["article_type"]
#         )
#         for _, row in batch.iterrows()
#     ]
#     session.add_all(articles)
#     session.commit()
#     print(f"已寫入第{i + len(batch)}筆")

# session.close()

# print("已完成寫入")
