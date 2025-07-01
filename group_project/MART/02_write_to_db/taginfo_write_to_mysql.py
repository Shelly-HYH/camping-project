# 把露營場的TAG寫入MySQL
# 已寫入，勿再重複執行
# 已寫入，勿再重複執行
# 已寫入，勿再重複執行

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, VARCHAR
import pandas as pd
from pathlib import Path

file_path = Path(".venv", "MART", "result_csv")
mart_tag_file = file_path / "MART01_ai_add_campground_tag.csv"
df = pd.read_csv(mart_tag_file, encoding="utf-8-sig")

# 寫入MySQL表單-------------------

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

# 要先建立表單
class Campground_with_tag(Base):
    __tablename__ = 'MART_campground_with_tag'
    campground_tag_ID = Column(Integer, primary_key=True)
    campground_ID = Column(Integer, nullable=False)
    county_ID = Column(Integer, nullable=False)
    camping_site_name = Column(VARCHAR(40), nullable=False)
    address = Column(VARCHAR(50), nullable=False)
    Tag = Column(VARCHAR(15), nullable=False)
    ref_keywords = Column(VARCHAR(60), nullable=False)
    tag_count = Column(Integer, nullable=False)


# 寫入-------------------
records = []

for i in range(len(df)):
    row = df.iloc[i]
    record = Campground_with_tag(
        campground_ID=int(row["campground_ID"]),
        county_ID=int(row["county_ID"]),
        camping_site_name=str(row["camping_site_name"]),
        address=str(row["address"]),
        Tag=str(row["Tag"]),
        ref_keywords=str(row["ref_keywords"]),
        tag_count=int(row["tag_count=int"])
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
