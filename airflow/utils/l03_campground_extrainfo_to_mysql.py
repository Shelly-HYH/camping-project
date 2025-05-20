# 把露營場的總星數跟總評論數寫進MySQL資料庫

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, Float, VARCHAR, DECIMAL
import pandas as pd
from pathlib import Path

Base = declarative_base()

class Campground(Base):
    __tablename__ = 'campground'
    campground_ID = Column(Integer, primary_key=True)
    address =Column(VARCHAR(50), nullable=True)
    total_rank = Column(Float, nullable=True)
    total_comments_count = Column(Integer, nullable=True)
    latitude = Column(DECIMAL(10,7), nullable=True)
    longitude = Column(DECIMAL(10,7), nullable=True)

def load_data():
    input_path = Path(r"/home/Tibame/tjr101_project/add_id_campground.csv")
    df = pd.read_csv(input_path, encoding="utf-8-sig")
    return df


# ---------------------------
def connect_db():
    # 建立連線
    host='104.199.166.199' # 主機位置
    user='shelly' # 使用者名稱
    port='3306' # 埠號
    password='shelly-password' # 密碼
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/test3_db"
    engine = create_engine(url, echo=True)
    
    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    return session

def write_data(session, df):
    
    updated_count = 0
    for _, row in df.iterrows():
        affected = session.query(Campground).filter_by(campground_ID=row["campground_ID"]).update({
            Campground.address: row["address"],
            Campground.total_rank: row["total_rank"],
            Campground.total_comments_count: row["total_comments_count"],
            Campground.latitude: row["latitude"],
            Campground.longitude: row["longitude"],
            })
        if affected:
            updated_count += 1

    session.commit()
    print(f"共更新 {updated_count}筆資料")

