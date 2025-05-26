import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine


def load_ref():
    # 載入參考用的表
    host='35.229.197.153' # 主機位置
    user='shelly' # 使用者名稱
    port='3306' # 埠號
    password='shelly-password' # 密碼
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/test3_db"

    engine = create_engine(url, echo=True)
    with engine.connect() as connection:
        df_ref = pd.read_sql("SELECT * FROM campground", con=engine)

    return df_ref

def load_campers_id():
    host='35.229.197.153' # 主機位置
    user='shelly' # 使用者名稱
    port='3306' # 埠號
    password='shelly-password' # 密碼
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/test3_db"

    engine = create_engine(url, echo=True)
    with engine.connect() as connection:
        df_camper = pd.read_sql("SELECT * FROM campers", con=engine)

    return df_camper
    
def merged_id(review_df, ref_df, camper_df):
    # 合併露營場ID
    review_result_df = review_df.merge(ref_df[["camping_site_name", "campground_ID"]], left_on="Campsite", right_on="camping_site_name", how="left")
    review_result_df["campground_ID"] = review_result_df["campground_ID"].fillna(99999)
    review_result_df["campground_ID"] = review_result_df["campground_ID"].astype(int)

    # 合併露營客ID
    review_result_df = review_result_df.merge(camper_df[["camper_name", "camper_ID"]], left_on="Reviewer", right_on="camper_name", how="left")
    return review_result_df

def clean_data(df):
    # 新增欄位
    df["platform_ID"] = 1
    df["article_type"] = "評論"

    # 日期轉為 datetime.date 型別
    df["Review_time"] = pd.to_datetime(df["Review_time"]).dt.date

    # 刪除不用的欄位
    df = df.drop(columns=['Name', "check_ID", "Campsite", "camping_site_name", "Reviewer", "camper_name"])

    # 露營場沒寫入的，評論也不寫入
    # 沒有任何內容的評論也不寫入
    # 營主回復的內容也不寫入
    review_result_df = df[df["campground_ID"] != 99999]
    review_result_df = review_result_df[review_result_df["Review_content"] != "No content in this review."]
    review_result_df = review_result_df[~review_result_df["Review_content"].str.strip().str.match(r"^(謝謝|您好)[，,。 　]*", na=False)]

    # 欄位名稱調整與資料庫上一致
    review_result_df = review_result_df.rename(columns={
                                        "Review_time": "publish_date",
                                        "Review_rank": "article_rank",
                                        "Review_content": "content",
                                    })
    return review_result_df

def save_file(review_result_df):
    save_path = Path("output")
    output_file = save_path / "add_id_reviews.csv"
    
    review_result_df.to_csv(output_file, index=False, encoding="utf-8-sig")

    print("合併後的評論檔儲存成功")
