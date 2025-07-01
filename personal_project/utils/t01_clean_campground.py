# final版本清洗露營場名稱跟地址
# 名稱刪除特殊符號及廣告詞
# 清洗google map地址和評論數字
# 清洗名稱相似的、是同一個地點的狀況，例如: 碧山露營場 , 碧山露營場森林營區

from collections import defaultdict
import pandas as pd
from pathlib import Path
import time
import re
from rapidfuzz import fuzz
from sqlalchemy import create_engine

def clean_campground_name(series: pd.Series):
    """
    處理google map爬下來的露營場名稱，傳入pd.Series，回傳pd.Series
    """
    # 用正規表達式合併為一個 pattern
    split_symbols = [
        r"｜", r"\|",   # 全形與半形的直線
        r"－", r"-",   # 全形與半形的 dash
        r"／", r"/",   # 全形與半形的 slash
        r"、", r"‧",   # 中文列點符號
        r"》", r"》",   # 全形右雙括號
        r"】", r"\]",  # 全形右方括號
        r"（",  # 括號
        r"」", r"「", 
        r"《", r"：",
    ]
    pattern = r"|".join(split_symbols)

    name_list = []
    
    for _, row in series.items():
        if pd.isna(row):
            name_list.append("")
            continue
        
        # 轉成字串並且刪除有些露營場會在名稱前面加上縣市
        row_str = str(row)
        remove_prefixes = ["高雄茂林_", "高雄茂林", "拉拉山．", "拉拉山", "苗栗泰安", "桃園復興", "苗栗南庄"]
        for p in remove_prefixes:
            row_str = row_str.replace(p, "")
        
        matches = list(re.finditer(pattern, row_str))
        sharp_matches = list(re.finditer(r"#", row_str))

        if pd.isna(row):
            name_list.append("")
            continue
        
        # 檢查是否有兩個以上的"#""   
        elif len(sharp_matches) >= 2:
            second_sharp = sharp_matches[1]
            name_list.append(row_str[:second_sharp.start()].strip())

        elif matches:
            match = matches[0]
            # 特殊符號的起始位置>=3才切，避開少數1-2號露營地等狀況
            if match.start() > 3:
                name_list.append(row_str[:match.start()].strip())
            else:
                name_list.append(row_str.strip().replace("「", ""))
        else:
            name_list.append(row_str.strip())

    return pd.Series(name_list)


def clean_address(series: pd.Series):
    """
    處理google map的地址，傳入pd.Series，回傳pd.Series
    """
    cleaned_address = []
    for _, row in series.items():
        # 清除開頭郵遞區號
        row = re.sub(r"^\d{3,6}\s*", "", str(row)).replace(" ", "")

        # 清除其他雜字
        row = row.replace("、", "").replace("No.", "").replace("No", "").replace("TW", "").replace("。", "")

        # 先抓出最後的地址號碼
        match_house_number = re.search(r"\d{1,3}(-\d{1,3})?(\d{1,3})?(之\d+)?號", row)
        house_number = ""
        if match_house_number:
            house_number = match_house_number.group()
            row = re.sub(re.escape(house_number), "", row)

        # 清除座標
        row = re.sub(r"\d{2,3}(?:\.\d+)+", "", row) #小數點座標
        row = re.sub(r"\d{1,3}°\d{1,3}'\d{0,3}(\.\d+)?[NSEW]?", "", row)  # 度分秒座標
        row = re.sub(r"\d{1,3}°\d{1,3}'(\.\d+)?[NSEW]?", "", row)        # 度分座標
        row = re.sub(r"\d+[°\'\"″]{1,2}[NSEW]", "", row)
        row = row.replace("°", "") # 清除可能漏掉的°

        # 清理地址尾巴的郵遞區號
        row = re.sub(r"\d{1,6}$", "", str(row)).strip()
        
        # 清除縣市區後面接郵遞區號
        row = re.sub(r"([區鄉鎮])\d{3,5}", r"\1", row)

        # 寫回格式正常的地址
        if len(row) <= 2 or (row[2] in ["縣", "市", "鄉"]):
            # 加回門牌
            if house_number and house_number not in row:
                row += house_number
            cleaned_address.append(row)
            continue 

        # 開始清理地址倒反放的狀況
        keywords = ["縣", "市", "區", "段", "鄉", "鎮", "鄰", "鄉道", "路", "巷", "街", "號", "樓", "UnnamedRd", "UnnamedRoad", "BaishiChi", "村民石"]

        address_clean = row
        for key in keywords:
            address_clean = re.sub(f"({key})", r"\1 ", address_clean)

        address_parts = address_clean.split()
        t_address = " ".join(address_parts[::-1]).replace(" ", "")

        # 反轉完成後，再檢查「號 + 路街巷」是否顛倒 → 修正
        t_address = re.sub(r"(\d{1,4})([^\d之號]{1,5}[路街巷])", r"\2\1號", t_address)

        # 加回門牌
        if house_number and house_number not in t_address:
            t_address += house_number
        
        cleaned_address.append(t_address)

    return pd.Series(cleaned_address)

def clean_review_number(series: pd.Series):
    """
    清洗且把所有的總評論數轉為數值型態，沒有值的填0
    傳入pd.Series，回傳pd.Series
    """
    cleaned_number = []
    for _, row in series.items():
        if pd.isna(row):
            cleaned_number.append(0)
        else:
            # row = str(row).replace('"', '').replace(",", "").strip()
            try:
                row = int(row)
            except ValueError:
                row = 0
            cleaned_number.append(row)
    return pd.Series(cleaned_number)


def get_prefix(name, n=8):
    """
    比較露營場名稱前
    先移除會影響判斷分數的字
    再取前N個字做判斷
    """
    remove_words = ["森林營區", "豪華露營", "辦公室", "淋浴間", "汽車露營區", "雨棚區", "景觀", "露營區", "茶園", "農場", "露營場", "營區"]
    
    for word in remove_words:
        name = name.replace(word, "")
    
    return name[:n].strip()

def clean_campground_duplicate(df: pd.DataFrame):
    
    # 建立前綴群組
    # 建立一個可以自動新增key的字典
    prefix_groups = defaultdict(list)

    for name in df["Campground"]:
        prefix = get_prefix(name)
        prefix_groups[prefix].append(name)
    
    # 進一步細分類（模糊比對完整名稱）
    final_groups = {}
    group_id = 0

    for prefix, names in prefix_groups.items():
        group_list = []
        
        for name in names:
            name_clean = get_prefix(name)
            found = False

            for group in group_list:
                group_clean = get_prefix(group[0])

                # 模糊比對80分以上就歸在同組
                if fuzz.partial_ratio(name_clean, group_clean) >= 80:
                    group.append(name)
                    found = True
                    break

            if not found:
                group_list.append([name])
        
        # 將每組結果放入 final_groups
        for group in group_list:
            final_groups[group_id] = group
            group_id += 1

    # 決定每組的統一名稱（取最短的當主名稱）
    name_mapping = {}

    for gid, names in final_groups.items():
        # 取最短名稱當主名稱
        main_name = sorted(names, key=len)[0]
        for name in names:
            name_mapping[name] = main_name

    # 加入到 dataframe
    df["Name_map"] = df["Campground"].map(name_mapping)

    # 計算 Campsite 的字數，加入新欄位
    df["名稱字數"] = df["Campground"].apply(len)

    # 依照 Name_map + 名稱字數 排序（名稱字數少的會排前面）
    df_sorted = df.sort_values(by=["Name_map", "名稱字數"])

    # 只保留每個 Name_map 的第一筆（也就是名稱字數最少的那筆）
    df_unique = df_sorted.drop_duplicates(subset=["Name_map"], keep="first")

    # 取代Campground
    df_unique["Campground"] = df_unique["Name_map"]  

    return df_unique


def load_county_id():
    host='35.201.136.64' # 主機位置
    user='shelly' # 使用者名稱
    port='3306' # 埠號
    password='shelly-password' # 密碼
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/campground_db"
    engine = create_engine(url, echo=False)
    with engine.connect() as connection:
        df_county = pd.read_sql("SELECT * FROM county", con=engine)

    df_county.to_csv("ref_county.csv", index=False, encoding="utf-8-sig")
    return df_county

def add_county_id(df_county, df):
    df_county["cleaned_county"] = df_county["county_name"].apply(
            lambda x: x if x == "新竹市" else x[:2]
            )

    result_df = df.merge(df_county[["cleaned_county", "county_ID"]], left_on="City", right_on="cleaned_county", how="left")

    return result_df

    