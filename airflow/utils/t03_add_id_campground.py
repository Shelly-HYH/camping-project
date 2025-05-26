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

    output_path = Path("output")
    output_file = output_path / "ref_campground.csv"
    df_ref.to_csv(output_file, index=False, encoding="utf-8-sig")
    
    return df_ref

def merge_data(map_df, df_ref):
    # 處理露營場ID-----
    result_df = map_df.merge(df_ref[["camping_site_name", "campground_ID"]], left_on="Campsite", right_on="camping_site_name", how="left")

    # 處理不用寫入的露營場，號碼設定為99999
    # 全部轉為int
    result_df["campground_ID"] = result_df["campground_ID"].fillna(99999)
    result_df["campground_ID"] = result_df["campground_ID"].astype(int)

    return result_df

def final_clean(df):
    # 刪除不用的欄位
    result_df = df.drop(columns=['Name', "camping_site_name", "cleaned_county", 'City'])

    # 刪除沒有要寫入的露營場 id =99999
    result_df = result_df[result_df["campground_ID"] != 99999]

    # 欄位名稱調整與資料庫上一致
    result_df = result_df.rename(columns={
                                        "Campsite": "camping_site_name",
                                        "Address": "address",
                                        "Rank": "total_rank",
                                        "Reviews": "total_comments_count",
                                    })
    return result_df


