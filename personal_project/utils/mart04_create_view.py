# 創建後續查詢用的view表

import pymysql

def connect_db():
    # 建立連線------------------------
    host='35.201.136.64' # 主機位置
    user='shelly' # 使用者名稱
    port=3306 # 埠號
    password='shelly-password' # 密碼
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database='campground_db',
        charset='utf8mb4',
        autocommit=True
    )
    return conn


def create_tables(conn):
    sql_statements = [
        """
        CREATE OR REPLACE VIEW MART_campground_county_tag
        as
        select 
            county.region, county.county_name,
            c.campground_name, c.total_rank, c.total_comments_count, c.address, c.latitude, c.longitude,
            tag.Tag, tag.ref_keywords
        from 
            mart_campground_tag as tag
              join campground as c
                on c.campground_ID = tag.campground_ID
              join county
                on c.county_ID = county.county_ID;
        """,
        ]
    
    # 執行語法
    try:
        with conn.cursor() as cursor:
            for i, stmt in enumerate(sql_statements, 1):
                try:
                    cursor.execute(stmt)
                except Exception as e:
                    print(f"[錯誤] 第 {i} 條語法執行失敗：\n{stmt}")
    finally:
        conn.close()