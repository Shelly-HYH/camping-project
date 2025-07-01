# 新增MART層分析需要的欄位
# 露營場分類：好評 ; 差評；普通
# 熱門露營場：總評論數屬於前25%的露營場
# 評論文章的正負面比率：負面(文章星數<4)數量 / 總數量 ； 正面：1-負面

from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path

# 建立連線-----------------------
host='104.199.214.113' # 主機位置
user='test' # 使用者名稱
port="3307" # 埠號
password='PassWord_1' # 密碼
url = f"mysql+pymysql://{user}:{password}@{host}:{port}/test2_db"
engine = create_engine(url, echo=True, pool_pre_ping=True)

# 用pandas讀取
with engine.connect() as connection:
    df_campground = pd.read_sql("SELECT * FROM campground", con=engine)

# 露營場評價類別-----------------------

# 加上分類好評；壞評；普通的欄位
# 好評 = 星數>=前25%的露營場
# 差評 = 星數<後25%的露營場
# 普通 = 介於中間的露營場
rank_q3 = df_campground["total_rank"].quantile(0.75) # 75%
print(f"星數前25%: {rank_q3}")
rank_q1 = df_campground["total_rank"].quantile(0.25) # 25%
print(f"星數後25%: {rank_q1}")

df_campground.loc[df_campground["total_rank"].notnull() & (df_campground["total_rank"] >= rank_q3), "campground_category"] = "好評"
df_campground.loc[df_campground["total_rank"].notnull() & (df_campground["total_rank"] < rank_q1), "campground_category"] = "差評"
rank_middle = df_campground["total_rank"].notnull() & (df_campground["total_rank"] > rank_q1) & (df_campground["total_rank"] < rank_q3)
df_campground.loc[rank_middle, "campground_category"] = "普通"


# 熱門露營場-----------------------

# 總評論數>=前25%的露營場
if df_campground["total_comments_count"].notnull().sum() > 0:
    comment_q3 = df_campground["total_comments_count"].quantile(0.9)
    df_campground.loc[df_campground["total_comments_count"].notnull() & (df_campground["total_comments_count"] >= comment_q3), "hot"] = "Hot"
print(f"總評數前25%: {comment_q3}")



# 處理正負面評論比例-----------------------

# 建立連線-----------------------
host='104.199.214.113' # 主機位置
user='test' # 使用者名稱
port="3307" # 埠號
password='PassWord_1' # 密碼
url = f"mysql+pymysql://{user}:{password}@{host}:{port}/test2_db"
engine = create_engine(url, echo=True, pool_pre_ping=True)

# 用pandas讀取營位表
with engine.connect() as connection:
    df_reviews = pd.read_sql("SELECT * FROM articles", con=engine)


# 只保留有星數的評論
df_valid = df_reviews[df_reviews["article_rank"].notnull()]

# 加上情緒分類欄位
df_valid.loc[df_valid["article_rank"] >= 4.0, "article_emotion"] = "positive"
df_valid.loc[df_valid["article_rank"] < 4.0, "article_emotion"] = "negative"

# 計算每個露營場的負面數量與總數
emotion_counts = df_valid.groupby(["campground_ID", "article_emotion"]).size().unstack(fill_value=0)

# 計算總數（有星數的評論才納入）
emotion_counts["total_valid"] = emotion_counts.sum(axis=1)

# 計算比例（除以有星數的評論總數）
emotion_counts["negative_ratio"] = round(emotion_counts.get("negative", 0) / emotion_counts["total_valid"], 2)
emotion_counts["positive_ratio"] = round(1 - emotion_counts["negative_ratio"], 2)

# 整理結果
result_df = emotion_counts[["negative_ratio", "positive_ratio"]].reset_index()

# 合併------------------------
df = df_campground.merge(result_df, on="campground_ID", how="left")
df = df.drop_duplicates(subset=["campground_ID"], keep="first")

# 新增欄位：熱門但負評比例高的露營場------------------
df["hot_but_negative"] = None
df.loc[(df["hot"] == "Hot") & (df["negative_ratio"] > 0.49), "hot_but_negative"] = "熱門但負評比例超過50%"


# 存檔------------------------
save_path = Path(".venv", "MART", "result_csv")
save_path.mkdir(parents=True, exist_ok=True)
df.to_csv(save_path / "MART02_campground_add_extra_info.csv", index=False, encoding="utf-8-sig")

print("OK")
