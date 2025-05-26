# 把所有評論寫進MySQL資料庫

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, VARCHAR, Integer, Date, Float, Text
import pandas as pd
from pathlib import Path

Base = declarative_base()

class Review(Base):
    __tablename__ = 'articles'
    article_ID = Column(Integer, primary_key=True, autoincrement=True)
    publish_date = Column(Date, nullable=False)
    article_rank = Column(Float, nullable=False)
    content = Column(Text, nullable=False)
    campground_ID = Column(Integer, nullable=False)
    camper_ID = Column(Integer, nullable=False)
    platform_ID = Column(Integer, nullable=False)
    article_type = Column(VARCHAR(10), nullable=False)

def load_data():
    input_path = Path("output", "add_id_reviews.csv")
    df = pd.read_csv(input_path, encoding="utf-8-sig")
    return df

# ---------------------------
def connect_db():
    # 建立連線
    host='35.229.197.153' # 主機位置
    user='shelly' # 使用者名稱
    port="3306" # 埠號
    password='shelly-password' # 密碼
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/test3_db"
    engine = create_engine(url, echo=True)
    
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    return session

def write_data(session, df):

    # 分批寫入
    batch_size = 1000
    for i in range(0, len(df), batch_size):

        batch = df.iloc[i:i+batch_size]
        articles = []

        for _, row in batch.iterrows():
            
            row = row.where(pd.notnull(row), None)  # 處理 NaN

            if row["camper_ID"] is None:  # 如果 camper_ID 缺值就略過
                continue

            article = Review(
                publish_date=row["publish_date"],
                article_rank=row["article_rank"],
                content=row["content"],
                campground_ID=row["campground_ID"],
                camper_ID=row["camper_ID"],
                platform_ID=row["platform_ID"],
                article_type=row["article_type"]
            )
            articles.append(article)
        try:
            if articles:
                session.add_all(articles)
                session.commit()

        except Exception as e:
            session.rollback()
            print(f"[錯誤] 新增失敗：{e}")

    session.close()

