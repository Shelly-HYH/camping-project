import pandas as pd
from pathlib import Path

# 處理評論檔案
review_file = Path(r"C:\TJR101_03_Project\.venv\project\cleaned\final_clean_reviews.csv")
review_df = pd.read_csv(review_file, encoding="utf-8-sig")

# 載入參考用的表
ref_path = Path(".venv", "project", "ETL_data_to_mysql", "ref")

ref_campground = Path(ref_path / "ref_campground.csv")
ref_campground_df = pd.read_csv(ref_campground, encoding="utf-8-sig")

ref_campers = Path(ref_path / "ref_campers.csv")
ref_camper_df = pd.read_csv(ref_campers, encoding="utf-8-sig")

# 合併露營場ID
review_result_df = review_df.merge(ref_campground_df[["camping_site_name", "campground_ID"]], left_on="Campsite", right_on="camping_site_name", how="left")
review_result_df["campground_ID"] = review_result_df["campground_ID"].fillna(99999)
review_result_df["campground_ID"] = review_result_df["campground_ID"].astype(int)

# 合併露營客ID
review_result_df = review_result_df.merge(ref_camper_df[["camper_name", "camper_ID"]], left_on="Reviewer", right_on="camper_name", how="left")

# 新增欄位
review_result_df["platform_ID"] = 1
review_result_df["article_type"] = "評論"

# 刪除不用的欄位
review_result_df = review_result_df.drop(columns=['Name', "check_ID", "Campsite", "camping_site_name", "Reviewer", "camper_name"])

# 露營場沒寫入的，評論也不寫入
# 沒有任何內容的評論也不寫入
review_result_df = review_result_df[review_result_df["campground_ID"] != 99999]
review_result_df = review_result_df[review_result_df["Review_content"] != "No content in this review."]

# 欄位名稱調整與資料庫上一致
review_result_df = review_result_df.rename(columns={
                                    "Review_time": "publish_date",
                                    "Review_rank": "article_rank",
                                    "Review_content": "content",
                                })

save_path = Path(".venv", "project", "ETL_data_to_mysql")
review_result_df.to_csv(save_path / "add_id_reviews.csv", index=False, encoding="utf-8-sig")
print("合併後的評論檔儲存成功")
