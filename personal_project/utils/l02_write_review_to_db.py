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
    campground_ID = Column(Integer, nullable=False)
    camper = Column(VARCHAR(50), nullable=True)
    comment_date = Column(Date, nullable=True)
    article_rank = Column(Float, nullable=True)
    content = Column(Text, nullable=True)


def load_data():
    input_path = Path("output", "add_id_reviews.csv")
    df = pd.read_csv(input_path, encoding="utf-8-sig")
    return df

# ---------------------------

def connect_db():
    # 建立連線
    host='35.201.136.64' # 主機位置
    user='shelly' # 使用者名稱
    port="3306" # 埠號
    password='shelly-password' # 密碼
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/campground_db"
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

            article = Review(
                comment_date = row["comment_date"],
                article_rank = row["article_rank"],
                content = row["content"],
                campground_ID = row["campground_ID"],
                camper = row["camper"],
            )
            articles.append(article)
        try:
            if articles:
                session.add_all(articles)
                session.commit()

        except Exception as e:
            session.rollback()
            print(f"[錯誤] 新增失敗：{e}")



