# 用GCP的vertex AI透過露營場的關鍵字去上TAG
# 需要先去GCP啟用vertex AI的API
# 然後再去IAM開對應的service account (role = vertex AI user)跟下載金鑰的json檔案

from vertexai.generative_models import GenerativeModel
import vertexai
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine
import re

save_path = Path(".venv", "MART", "result_csv")

# 設定Gemini API資訊----------------------

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\TJR101_03_Project\.venv\project\gemini-api-user.json"

# 初始化GCP專案與地區
vertexai.init(
            project="glass-archway-457401-e6", # 要改自己GCP的專案ID
            location="us-central1" # 固定不用改
            )

# Gemini 模型
model = GenerativeModel("gemini-2.0-flash")

# 建立連線--------------------------------------
host='104.199.214.113' # 主機位置
user='test' # 使用者名稱
port="3307" # 埠號
password='PassWord_1' # 密碼
url = f"mysql+pymysql://{user}:{password}@{host}:{port}/test2_db"
engine = create_engine(url, echo=True, pool_pre_ping=True)

# 用pandas讀取
with engine.connect() as connection:
    df1 = pd.read_sql("SELECT * FROM TAG", con=connection)

# 存檔與輸出
tag_file = save_path / "TAG_list.csv"
df1.to_csv(tag_file, encoding="utf-8", index=False)
tag_list = df1["TAG_name"]
tag_string = "｜".join(tag_list.tolist())

# 載入jieba已經切好的關鍵字檔案-------------------------

keywords_file = save_path / "MART00_cut_keyword_groupby_campground.csv"
df_keyword = pd.read_csv(keywords_file, encoding="utf-8-sig")

# # 隨機挑選數個露營場先做測試---------------
# test_df = df_keyword.sample(n=200)

# 儲存token做金額預估
total_prompt_tokens = 0
total_completion_tokens = 0
total_token = 0

# 儲存AI分析的結果
results = [] 

for _, row in df_keyword.iterrows():
    campground_ID = row["campground_ID"]
    keywords = row["關鍵字"]

    prompt = f"""
                我下面會給你一個露營場的關鍵字列表，關鍵字是從多篇評論中取出，並用｜區隔。
                請扮演一位熟悉露營場評論分析的資料科學家，從關鍵字判斷該露營場評論的特徵，並挑選最適合的Tag。
                請注意以下規則：
                1. 僅從下面Tag清單中選擇，不要創造新Tag。
                2. 若無明顯關聯的Tag可選，請留空白，不要亂選。
                3. 每一列輸出代表一個標籤（若一個露營場有多個標籤，請輸出多列）。
                4. 每一列請包含：露營場 ID、Tag與對應的關鍵字，並以「----------」分隔每筆資料。
                    Tag清單：{tag_string}

                請注意以下重點：
                1. 僅當關鍵字呈現正面語氣時，才可對應至Tag（如：「蚊蟲少」、「排水很好」）
                2. 由於你看到的資料是斷詞後的關鍵字，若無法從關鍵字明確判斷是正面語氣，請不要標註任何Tag。
                3. 廁所只有在同時有"乾淨"或其他詞彙時，才可能是廁所整潔。
                4. 蚊蟲只有在同時有其他可以判斷的詞彙時，才可能是蚊蟲少。
                3. 請注意，下列詞彙常出現在負面評論中，若關鍵字中僅出現這些詞，請不要標註對應的Tag。

                【常見露營負面詞彙】：
                螞蟻、蒼蠅、蚊子、排水、沒有廁所、很貴、吵、冷、熱、潮濕、硬、髒、水溝、漏水、味道、臭、蟲子、不方便、沒插座、沒有遮蔽、網路很慢、收訊不好

                【露營場關鍵字資料】
                露營場 ID：{campground_ID}
                關鍵字：{keywords}

                
                【請以下面格式回覆】
                露營場ID：{campground_ID}
                TAG：（從Tag清單中選）
                對應的關鍵字：（對應到這個Tag的關鍵詞）

                """
    # 開始執行
    response = model.generate_content(prompt)
    print(response.text)

    # 統計每一筆的token
    usage = response.usage_metadata
    total_prompt_tokens += usage.prompt_token_count
    total_completion_tokens += usage.candidates_token_count
    total_token = total_prompt_tokens + total_completion_tokens

    print("="*30)
    print(f"露營場ID:{campground_ID}，分析結束")
    print("="*30)

    # 分析結果轉存至DataFrame
    entries = response.text.strip().split("----------")
    for entry in entries:
        lines = entry.strip().splitlines()
        if len(lines) < 3:
            continue

        id = lines[0].strip().replace("露營場ID：", "")
        tags = lines[1].strip().replace("TAG：", "")
        keywords_for_tag = lines[2].strip().replace("對應的關鍵字：", "")
        results.append({
                        "露營場ID": id,
                        "Tag": tags,
                        "對應的關鍵字": keywords_for_tag,
                        })

# 計算這次的花費----------------------

# 定價參數
COST_PROMPT = 0.35 / 100_0000
COST_COMPLETION = 1.05 / 100_0000
TWD_RATE = 33

print(f"Token使用：\nPrompt={total_prompt_tokens} \nCompletion={total_completion_tokens}")
print(f"本次總token數：{total_token}")

# 計算費用
prompt_usd = total_prompt_tokens * COST_PROMPT
completion_usd = total_completion_tokens * COST_COMPLETION
total_usd = prompt_usd + completion_usd
total_twd = total_usd * TWD_RATE

# 輸出費用
print(f"預估本次花費：{total_twd:.2f}元")

# 清理輸出的關鍵字格式----------------------

def clean_keywords(text):
    if pd.isna(text):
        return ""
    # 將所有常見分隔符（、 , ， ｜ 空格）統一轉為「｜」
    text = re.sub(r"[、,\s｜]+", "｜", text)
    # 去除重複的分隔符與前後多餘分隔符
    text = re.sub(r"\|{2,}", "｜", text)
    return text.strip("｜")  # 去除頭尾的分隔符


result_df = pd.DataFrame(results)
# 套用清洗函式
result_df["對應的關鍵字"] = result_df["對應的關鍵字"].apply(clean_keywords)

# 儲存----------------------

output_file = Path(save_path, "MART01_ai_add_campground_tag.csv")
result_df.to_csv(output_file, index=False, encoding="utf-8-sig")
print(f"分析完成，結果已儲存")
