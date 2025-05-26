# 清洗評論csv的露營場名稱

from pathlib import Path
import pandas as pd
import re
from rapidfuzz import fuzz
from collections import defaultdict

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

    for name in df["Campsite"]:
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
    df["Name_map"] = df["Campsite"].map(name_mapping)

    # 計算 Campsite 的字數，加入新欄位
    df["名稱字數"] = df["Campsite"].apply(len)

    # 找出每個 Name_map 群組中，名稱字數最短的是哪個名稱
    shortest_name = df.groupby("Name_map")["名稱字數"].transform("min")

    # 只保留名稱字數等於最小值的（也就是最短名稱的那組評論）
    df_review_unique = df[df["名稱字數"] == shortest_name]

    # 刪除
    df_review_unique = df_review_unique.drop(columns=["名稱字數", "Name_map"])

    return df_review_unique

def take_campers(series: pd.Series):
    campers = []
    for _, row in series.items():
        row = str(row)
        campers.append(row)
    
    cleaned_campers = pd.Series(campers).drop_duplicates()
    df = pd.DataFrame({"Reviewer": cleaned_campers})
    return df

def main():

    # 清洗評論csv
    review_file_path = Path(r"C:\TJR101_03_Project\.venv\project\camp_reviews_final.csv")
    df = pd.read_csv(review_file_path, encoding="utf-8")

    # 清洗名稱
    df["Campsite"] = clean_campground_name(df["Campsite"])

    # 開始處理相似名稱的露營場----------------------------
    # 每個露營場有40筆評論，需要保留全部評論資料
    # 程式碼與清洗名稱不同
    df = clean_campground_duplicate(df)

    # 儲存檔案-----------------------
    df["Name"] = "shelly"
    cols = df.columns.tolist()
    cols.insert(0, cols.pop(cols.index("Name")))
    df = df[cols]

    output_path = Path(".venv", "project", "cleaned")
    df.to_csv(output_path / "final_clean_reviews.csv", index=False, encoding="utf-8-sig")
    print("評論資料清洗成功執行")

    # 3.評論csv另存一個露營客的csv
    file_name = "campers.csv"
    df3 = take_campers(df["Reviewer"])
    df3.to_csv(output_path / file_name, index=False, header=False, encoding="utf-8-sig")
    print("露營客檔案完成")


main()
