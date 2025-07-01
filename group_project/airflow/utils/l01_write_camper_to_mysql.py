# 把camper名稱寫入MySQL的campers表單

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, VARCHAR, Integer
import pandas as pd
from pathlib import Path

Base = declarative_base()

class Camper(Base):
    __tablename__ = 'campers'
    camper_ID = Column(Integer, primary_key=True, autoincrement=True)
    camper_name = Column(VARCHAR(50), nullable=False)
    platform_ID = Column(Integer, nullable=False, default=1)

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
    input_file = input_path / "campers.csv"
    campers_df = pd.read_csv(input_file, header=None, names=["camper_name"], encoding="utf-8-sig")

    # 分批寫入
    batch_size = 1000
    for i in range(0, len(campers_df), batch_size):
        batch = campers_df["camper_name"].iloc[i:i+batch_size].dropna()
        new_campers = []

        for name in batch:
            # 檢查資料庫中是否已有該 camper_name
            exists = session.query(Camper).filter_by(camper_name=name).first()

            if not exists:
                new_campers.append(Camper(camper_name=name))

        if new_campers:
            session.add_all(new_campers)
            try:
                session.commit()
                print(f"[OK] 成功新增 {len(new_campers)} 筆露營客")
            except Exception as e:
                session.rollback()
                print(f"[錯誤] 新增失敗：{e}")

