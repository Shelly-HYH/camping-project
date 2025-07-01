# 讀取SQL檔案
# 建立MYSQL資料庫

import pymysql

def connect_db():
    # 建立連線------------------------
    host='35.229.197.153' # 主機位置
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
        # 建資料庫
        "CREATE DATABASE IF NOT EXISTS test3_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
        "USE test3_db",

        # 依順序建立表格
        """
        CREATE TABLE `county` (
            `county_ID` int not null auto_increment comment '縣市ID(PK)',
            `region` char(2) not null comment '區域',
            `county_name` char(3) not null comment '縣市',
            primary key (county_ID)
            ) comment="縣市表";
        """,

        """
        CREATE TABLE `TAG` (
            `TAG_ID` int not null auto_increment comment 'TAG ID(PK)',
            `TAG_name` varchar(20),
            primary key (TAG_ID)
            ) comment="TAG表"
        """,

        """
        CREATE TABLE `campground` (
            `campground_ID` int not null auto_increment comment '露營場ID(PK)',
            `county_ID` int not null comment '縣市ID(FK)',
            `camping_site_name` varchar(40) not null comment '露營場',
            `address` varchar(50) not null comment '地址',
            `total_rank` float comment '露營場評級',
            `total_comments_count` int comment '總評論數',
            `altitude` int comment '海拔高度',
            `traffic_rating` int comment '交通評級',
            `bathroom_rating` int comment '浴廁評級',
            `view_rating` int comment '景觀評級',
            `service_rating` int comment '服務評級',
            `facility_rating` int comment '設備評級',
            `latitude` decimal(10,7) comment '緯度',
            `longitude` decimal(10,7) comment '經度',
            primary key (campground_ID),
            constraint fk_county_id foreign key(county_id) references county(county_ID) on update cascade on delete restrict
            ) comment="露營場表"
        """,

        """
        CREATE TABLE `camping_site` (
            `camping_site_ID` int not null auto_increment comment '營位ID(PK)',
            `campground_ID` int not null comment '露營場ID(FK)',
            `camping_site_type_name` varchar(10) not null comment '營位',
            `price` int comment '營位價格',  
            primary key (camping_site_ID),
            constraint fk_camping_site_campground_ID foreign key(campground_ID) references campground(campground_ID) on update cascade on delete restrict
            ) comment="營位表"
        """,

        """
        CREATE TABLE `equipment` (
            `equipment_ID` int not null auto_increment comment '設備ID(PK)',
            `campground_ID` int not null comment '露營場ID(FK)',
            `equipment_details` varchar(80) not null comment '設備',
            primary key (equipment_ID) ,
            constraint fk_equipment_campground_ID foreign key(campground_ID) references campground(campground_ID) on update cascade on delete restrict
            ) comment="設備表"
        """,

        """
        CREATE TABLE `service` (
            `service_ID` int not null auto_increment comment '服務ID(PK)',
            `campground_ID` int not null comment '露營場ID(FK)',
            `service_details` varchar(80) not null comment '服務',
            primary key (service_ID),
            constraint fk_service_campground_ID foreign key(campground_ID) references campground(campground_ID) on update cascade on delete restrict
            ) comment="服務表"
        """,

        """
        CREATE TABLE `platform` (
            `platform_ID` int not null auto_increment comment '媒體平台ID(PK)',
            `platform_name` varchar(15) not null comment '媒體平台',
            primary key (platform_ID)
            ) comment="媒體平台表"
        """,

        """
        CREATE TABLE `campers` (
            `camper_ID` int not null auto_increment comment '露營客ID(PK)',
            `platform_ID` int not null comment '媒體平台ID(FK)',
            `camper_name` varchar(50) not null comment '露營客',
            primary key (camper_ID),
            constraint fk_campers_platform_ID foreign key(platform_ID) references platform(platform_ID) on update cascade on delete restrict
            ) comment="露營客表"
        """,

        """
        CREATE TABLE `articles` (
            `articles_ID` int not null auto_increment comment '文章ID(PK)',
            `platform_ID` int not null comment '媒體平台ID(FK)',
            `camper_ID` int not null comment '露營客ID(FK)',
            `campground_ID` int not null comment '露營場ID(FK)',
            `article_type` varchar(10) not null comment '類型',
            `publish_date` date comment '發表日期',
            `article_rank` float comment '評論星數',
            `content` text comment '內容',
            primary key (articles_ID),
            constraint fk_articles_platform_ID foreign key(platform_ID) references platform(platform_ID) on update cascade on delete restrict,
            constraint fk_articles_camper_ID foreign key(camper_ID) references campers(camper_ID) on update cascade on delete restrict,
            constraint fk_articles_campground_ID foreign key(campground_ID) references campground(campground_ID) on update cascade on delete restrict
            ) comment="發表文章表"
        """,

        # 插入初始資料
        """
        INSERT INTO county(region, county_name) VALUES
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
        INSERT INTO platform(platform_name) VALUES
            ('Google_map'), 
            ('痞客邦'), 
            ('ETtoday旅遊雲'), 
            ('露營樂'), 
            ('愛露營')
        """,

        """
        INSERT INTO TAG(TAG_name) VALUES
            ('親子友善'), 
            ('豪華露營'), 
            ('免搭帳棚(懶人露營)'), 
            ('廁所整潔'), 
            ('淋浴設備佳'),
            ('營地環境維護佳'), 
            ('寵物友善'), 
            ('電信訊號良好'), 
            ('蚊蟲少'),
            ('景觀漂亮'), 
            ('交通便利')
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