import pandas as pd
from pathlib import Path

# 載入爬下來的露營場資料
map_file = Path(r"C:\TJR101_03_Project\.venv\project\cleaned\final_campground_clean.csv")
map_df = pd.read_csv(map_file, encoding="utf-8-sig")
# print(map_df.head())
# print("-"*30)

# 載入參考用的表
ref_path = Path(".venv", "project", "ETL_data_to_mysql", "ref")
ref_file = Path(ref_path / "ref_campground.csv")
ref_df = pd.read_csv(ref_file, encoding="utf-8-sig")
# print(ref_df.head())
# print("-"*30)

# 處理露營場ID-----
result_df = map_df.merge(ref_df[["camping_site_name", "campground_ID"]], left_on="Campsite", right_on="camping_site_name", how="left")
# print(result_df.head())

# 處理不用寫入的露營場，號碼設定為99999
# 全部轉為int
result_df["campground_ID"] = result_df["campground_ID"].fillna(99999)
result_df["campground_ID"] = result_df["campground_ID"].astype(int)

# 處理縣市ID-----
county_file = Path(r"C:\TJR101_03_Project\.venv\project\l_data_to_mysql\ref_county.csv")
county_df = pd.read_csv(county_file, encoding="utf-8-sig")
county_df["cleaned_county"] = county_df["county_name"].str[:2]

result_df = result_df.merge(county_df[["cleaned_county", "county_ID"]], left_on="City", right_on="cleaned_county", how="left")

# 刪除不用的欄位
result_df = result_df.drop(columns=['Name', "camping_site_name", "cleaned_county", 'City', 'Address'])

# 刪除沒有要寫入的露營場 id =99999
result_df = result_df[result_df["campground_ID"] != 99999]

# 欄位名稱調整與資料庫上一致
result_df = result_df.rename(columns={
                                    "Campsite": "camping_site_name",
                                    "Rank": "total_rank",
                                    "Reviews": "total_comments_count",
                                })

# 儲存
save_path = Path(".venv", "project", "ETL_data_to_mysql")
result_df.to_csv(save_path / "add_id_campground.csv", index=False, encoding="utf-8-sig")
print("合併後的露營場表儲存成功")
