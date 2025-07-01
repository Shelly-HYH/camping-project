# 把campground寫入MySQL

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, VARCHAR, Integer, Float, DECIMAL
import pandas as pd
from pathlib import Path

Base = declarative_base()

class Campground(Base):
    __tablename__ = 'campground'
    campground_ID = Column(Integer, primary_key=True, autoincrement=True)
    campground_name = Column(VARCHAR(50), nullable=False)
    county_ID = Column(Integer, nullable=False)
    address = Column(VARCHAR(50), nullable=True)
    total_rank = Column(Float, nullable=True)
    total_comments_count = Column(Integer, nullable=True)
    latitude = Column(DECIMAL(10,7), nullable=True)
    longitude = Column(DECIMAL(10,7), nullable=True)

def connect_db():
    # 建立連線
    host='35.201.136.64' # 主機位置
    user='shelly' # 使用者名稱
    port='3306' # 埠號
    password='shelly-password' # 密碼
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/campground_db"
    engine = create_engine(url, echo=False)

    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    return session

def write_data(session):
    input_path = Path("output")
    input_file = input_path / "final_campground_clean.csv"
    df = pd.read_csv(input_file, encoding="utf-8-sig")
    df = df.where(pd.notnull(df), None)

    new_campgrounds = []
    for _, row in df.iterrows():
        campground_name = row["Campground"]
        county_ID = row["county_ID"]
        address = row["address_clean"]
        
        latitude = float(row["Latitude"]) if row["Latitude"] is not None else None
        longitude = float(row["Longitude"]) if row["Longitude"] is not None else None
        total_rank = float(row["Rank"]) if row["Rank"] is not None else None
        total_comments_count = int(row["Reviews"]) if row["Reviews"] is not None else None

        if campground_name:  # 避免空值
            new_campgrounds.append(
                                Campground(campground_name=campground_name,
                                              county_ID=int(county_ID),
                                              address = address,
                                              total_rank = total_rank,
                                              total_comments_count = total_comments_count,
                                              latitude = latitude,
                                              longitude = longitude,
                                              ))

    if new_campgrounds:
        session.add_all(new_campgrounds)
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            
            print(f"[錯誤] 寫入失敗：{e}")
            raise 
