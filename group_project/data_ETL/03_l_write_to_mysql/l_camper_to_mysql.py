# 把camper名稱寫入MySQL的campers表單
# 已寫入，勿再執行，會重複寫入
# 已寫入，勿再執行，會重複寫入
# 已寫入，勿再執行，會重複寫入

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, VARCHAR, Integer
import pandas as pd
from pathlib import Path

# 建立連線
host='104.199.214.113' # 主機位置
user='test' # 使用者名稱
port="3307" # 埠號
password='PassWord_1' # 密碼
url = f"mysql+pymysql://{user}:{password}@{host}:{port}/test2_db"
# print(url)
engine = create_engine(url, echo=True)

Base = declarative_base()
DBSession = sessionmaker(bind=engine)
session = DBSession()

# # ---------------------------
# class camper(Base):
#     __tablename__ = 'campers'
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     camper_name = Column(VARCHAR(50), nullable=False)
#     platform_ID = Column(Integer, nullable=False, default=1)

# camper_file = Path(r"C:\TJR101_03_Project\.venv\project\cleaned\campers.csv")
# campers_df = pd.read_csv(camper_file, header=None, names=["camper_name"], encoding="utf-8-sig")

# # 分批寫入
# batch_size = 1000
# for i in range(0, len(campers_df), batch_size):
#     batch = campers_df["camper_name"].iloc[i:i+batch_size]
#     session.add_all([camper(camper_name=name) for name in batch])
#     session.commit()

# session.close()
