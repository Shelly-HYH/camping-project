# 建立MYSQL資料庫

import pymysql

def connect_db():
    # 建立連線------------------------
    host='35.201.136.64' # 主機位置
    user='root' # 使用者名稱
    port=3306 # 埠號
    password='my-password' # 密碼
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
        # 建資料庫
        "CREATE DATABASE IF NOT EXISTS campground_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",

        "USE campground_db;",

        # 依順序建立表格
        """
        CREATE TABLE `county` (
            `county_ID` int not null auto_increment comment '縣市ID(PK)',
            `region` char(2) not null comment '區域',
            `county_name` varchar(5) not null comment '縣市',
            primary key (county_ID)
            ) comment="縣市表";
        """,

        """
        CREATE TABLE `tag` (
            `TAG_ID` int not null auto_increment comment 'TAG ID(PK)',
            `TAG_name` varchar(20),
            primary key (TAG_ID)
            ) comment="TAG表"
        """,

        """
        CREATE TABLE `campground` (
            `campground_ID` int not null auto_increment comment '露營場ID(PK)',
            `county_ID` int not null comment '縣市ID(FK)',
            `campground_name` varchar(50) not null comment '露營場',
            `address` varchar(50) comment '地址',
            `total_rank` float comment '露營場評級',
            `total_comments_count` int comment '總評論數',
            `latitude` decimal(10,7) comment '緯度',
            `longitude` decimal(10,7) comment '經度',
            primary key (campground_ID),
            constraint fk_county_ID foreign key(county_ID) references county(county_ID) on update cascade on delete restrict
            ) comment="露營場表"
        """,

        """
        CREATE TABLE `articles` (
            `articles_ID` int not null auto_increment comment '文章ID(PK)',
            `campground_ID` int not null comment '露營場ID(FK)',
            `camper` varchar(50) comment '露營客',
            `comment_date` date comment '發表日期',
            `article_rank` float comment '評論星數',
            `content` text comment '內容',
            primary key (articles_ID),
            constraint fk_articles_campground_ID foreign key(campground_ID) references campground(campground_ID) on update cascade on delete restrict
            ) comment="評論表"
        """,

        # 插入初始資料
        """
        INSERT IGNORE INTO county(region, county_name) VALUES
            ('北區', '台北市'), 
            ('北區', '新北市'), 
            ('北區', '基隆市'),
            ('北區', '桃園市'), 
            ('北區', '苗栗縣'), 
            ('北區', '新竹縣'),
            ('北區', '新竹市'), 
            ('北區', '宜蘭縣'), 
            ('中區', '台中市'),
            ('中區', '彰化縣'), 
            ('中區', '南投縣'), 
            ('中區', '雲林縣'),
            ('南區', '嘉義縣'), 
            ('南區', '台南市'), 
            ('南區', '高雄市'),
            ('南區', '屏東縣'), 
            ('東區', '花蓮縣'), 
            ('東區', '台東縣')
        """,

        """
        INSERT IGNORE INTO tag(TAG_name) VALUES
            ('適合單人露營'),
            ('適合騎機車到達'),
            ('親子友善'), 
            ('適合團體出遊'),
            ('懶人露營'), 
            ('廁所整潔'), 
            ('淋浴設備佳'),
            ('營地環境好'), 
            ('寵物友善'), 
            ('電信訊號良好'), 
            ('蚊蟲少'),
            ('交通便利'),
            ('營地安靜')
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
    finally:
        conn.close()