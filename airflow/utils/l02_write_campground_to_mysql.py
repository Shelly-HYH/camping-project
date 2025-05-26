# 把campground寫入MySQL的表單產生ID

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, VARCHAR, Integer
import pandas as pd
from pathlib import Path

Base = declarative_base()

class Campground(Base):
    __tablename__ = 'campground'
    campground_ID = Column(Integer, primary_key=True, autoincrement=True)
    camping_site_name = Column(VARCHAR(40), nullable=False)
    county_ID = Column(Integer, nullable=False)

def connect_db():
    # 建立連線
    host='35.229.197.153' # 主機位置
    user='shelly' # 使用者名稱
    port='3306' # 埠號
    password='shelly-password' # 密碼
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/test3_db"
    engine = create_engine(url, echo=True)

    DBSession = sessionmaker(bind=engine)
    session = DBSession()
    return session

def write_data(session):
    input_path = Path("output")
    input_file = input_path / "campgrounds.csv"
    df = pd.read_csv(input_file, encoding="utf-8-sig")

    new_campgrounds = []
    for _, row in df.iterrows():
        campground_name = row["camping_site_name"]
        county_ID = row["county_ID"]
        if campground_name:  # 避免空值
            new_campgrounds.append(Campground(camping_site_name=campground_name,
                                              county_ID=int(county_ID)))

    if new_campgrounds:
        session.add_all(new_campgrounds)
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            print(f"[錯誤] 寫入失敗：{e}")
            raise 
