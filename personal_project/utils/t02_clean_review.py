# 清洗評論

import re
from rapidfuzz import fuzz
from collections import defaultdict
import pandas as pd
from pathlib import Path
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

    # 計算 Campground的字數，加入新欄位
    df["名稱字數"] = df["Campground"].apply(len)

    # 找出每個 Name_map 群組中，名稱字數最短的是哪個名稱
    shortest_name = df.groupby("Name_map")["名稱字數"].transform("min")

    # 只保留名稱字數等於最小值的（也就是最短名稱的那組評論）
    df_review_unique = df[df["名稱字數"] == shortest_name]

    # 刪除
    df_review_unique = df_review_unique.drop(columns=["名稱字數", "Name_map"])

    return df_review_unique

# merge 露營場ID-----------------------------

def load_ref():
    # 載入參考用的表
    host='35.201.136.64' # 主機位置
    user='shelly' # 使用者名稱
    port='3306' # 埠號
    password='shelly-password' # 密碼
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/campground_db"

    engine = create_engine(url, echo=True)
    with engine.connect() as connection:
        df_ref = pd.read_sql("SELECT * FROM campground", con=engine)

    return df_ref

    
def merged_id(review_df, ref_df):
    # 合併露營場ID
    review_result_df = review_df.merge(ref_df[["campground_name", "campground_ID"]], left_on="Campground", right_on="campground_name", how="left")
    review_result_df["campground_ID"] = review_result_df["campground_ID"].fillna(99999)
    review_result_df["campground_ID"] = review_result_df["campground_ID"].astype(int)

    return review_result_df


def clean_data(df):

    # 日期轉為 datetime.date 型別
    df["Review_time"] = pd.to_datetime(df["Review_time"]).dt.date

    # 刪除不用的欄位
    df = df.drop(columns=["check_ID", "Campground", "campground_name", ])

    # 露營場沒寫入的，評論也不寫入
    # 沒有任何內容的評論也不寫入
    # 營主回復的內容也不寫入
    review_result_df = df[df["campground_ID"] != 99999]
    review_result_df = review_result_df[review_result_df["Review_content"] != "No content in this review."]
    review_result_df = review_result_df[~review_result_df["Review_content"].str.strip().str.match(r"^(謝謝|您好)[，,。 　]*", na=False)]

    # 欄位名稱調整與資料庫上一致
    review_result_df = review_result_df.rename(columns={
                                        "Review_time": "comment_date",
                                        "Review_rank": "article_rank",
                                        "Review_content": "content",
                                        "Reviewer": "camper",
                                    })
    return review_result_df

def save_file(review_result_df):
    save_path = Path("output")
    output_file = save_path / "add_id_reviews.csv"
    
    review_result_df.to_csv(output_file, index=False, encoding="utf-8-sig")

    print("合併後的評論儲存成功")


