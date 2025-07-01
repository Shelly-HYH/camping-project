# 把露營場的TAG寫入MySQL

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, VARCHAR, Text
import pandas as pd
from pathlib import Path
import pymysql

Base = declarative_base()

class Campground_with_tag(Base):
        __tablename__ = 'mart_campground_tag'
        mart_campground_tag_ID = Column(Integer, primary_key=True, autoincrement=True)
        campground_ID = Column(Integer, nullable=False)
        Tag = Column(Text, nullable=True)
        ref_keywords = Column(VARCHAR(100), nullable=True)

# 建立表單-----

def connect_db():
    # 寫入MySQL表單-------------------
    host='35.201.136.64' # 主機位置
    user='shelly' # 使用者名稱
    port=3306 # 埠號
    password='shelly-password' # 密碼

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        charset='utf8mb4',
        autocommit=True
    )

    return conn

def create_tables(conn):
    sql_statements = [
        "USE campground_db;",

        """
        CREATE TABLE IF NOT EXISTS mart_campground_tag (
            `mart_campground_tag_ID` int not null auto_increment comment 'tag表單ID(PK)',
            `campground_ID` int not null comment '露營場ID(FK)',
            `Tag` varchar(20) comment 'tag',
            `ref_keywords` text comment '對應關鍵字',
            primary key (mart_campground_tag_ID),
            constraint fk_mart_campground_ID foreign key(campground_ID) references campground(campground_ID) on update cascade on delete restrict
            ) comment="tag表";
        """
    ]
    # 執行語法
    try:
        with conn.cursor() as cursor:
            for i, stmt in enumerate(sql_statements, 1):
                try:
                    cursor.execute(stmt)
                except Exception as e:
                    print(f"[錯誤] 第 {i} 條語法執行失敗：\n{stmt}")
                    print(f"[錯誤內容]：{e}")
    finally:
        conn.close()


# 寫入資料-----

def write_data(df):
    # 建立連線--------------------------------------
    url = "mysql+pymysql://shelly:shelly-password@35.201.136.64:3306/campground_db"
    engine = create_engine(url, echo=False)
    DBSession = sessionmaker(bind=engine)
    session = DBSession()   

    # 寫入-------------------
    records = []
    df = df.where(pd.notnull(df), None)

    for i in range(len(df)):
        row = df.iloc[i]
        record = Campground_with_tag(
            campground_ID=int(row["campground_ID"]),
            Tag=str(row["Tag"]),
            ref_keywords=str(row["ref_keywords"]),
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
