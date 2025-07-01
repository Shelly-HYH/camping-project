# 讓gemini ai根據目前露營場有的關鍵字，建議新的TAG

from vertexai.generative_models import GenerativeModel
import vertexai
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine

save_path = Path(".venv", "MART", "result_csv")

# 設定Gemini API資訊-------------------------------

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\TJR101_03_Project\.venv\project\gemini-api-user.json"

# 初始化GCP專案與地區
vertexai.init(
            project="glass-archway-457401-e6", # 要改自己GCP的專案ID
            location="us-central1" # 固定不用改
            )

# Gemini 模型
model = GenerativeModel("gemini-2.0-flash")

# 載入MySQL最新的TAG表--------------------------------------
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

# 讓AI建議新TAG------------------------
keywords_file = save_path / "MART00_cut_keyword_groupby_campground.csv"
df_keyword = pd.read_csv(keywords_file, encoding="utf-8-sig")

# 隨機挑選露營場
test_df = df_keyword.sample(n=300)

prompt_corpus = ""

for _, row in test_df.iterrows():
    prompt_corpus += f"""
                    露營場 ID：{row['campground_ID']}
                    關鍵字：{row['關鍵字']}
                    ----------
                    """

prompt = f"""
            以下是多個露營場的評論關鍵字，這些關鍵字是從每個露營場的多篇評論中取出，已經過斷詞與清洗。
            請你先閱讀這些露營場的關鍵字，根據出現頻率或潛在主題，判斷是否有目前未被列入的共同特徵。

            目前我們的TAG清單如下：
            {tag_string}

            請你思考：是否有尚未出現在Tag清單中，但根據這些關鍵字，值得加入的補充Tag？
            【注意事項】
            1. 請勿建議跟已經有的Tag相似的新Tag，例如：原本有「營主用心經營」，則不需再新增「營地管理良好」
            2. 請寫出新Tag的建議名稱，並舉例這些特徵在哪些露營場 ID 中出現、搭配了哪些關鍵詞。
            3. 請針對這些潛在主題，建議更通用、更具歸納性的Tag名稱（適合整體露營趨勢標示，避免只針對單一物件或設施）
                例如： 不建議：「有魚池」「提供電風扇」「有吹風機」; 建議：「水上活動」「生活設備便利」

            建議格式如下：

            建議新增的Tag：
            1. 【TAG名稱】：出現在下列露營場，例如 ID：16、18，關鍵字包含「電風扇」「吹風機」
            2. 【TAG名稱】：（說明…）

            請用簡潔格式列出即可。若無建議，請直接說「無明顯可補充的新TAG」。
            ----------
            {prompt_corpus}
            """
response = model.generate_content(prompt)
usage = response.usage_metadata  # Token統計

print("-"*30)
print(response.text)
print(f"{usage.total_token_count}")
print("-"*30)
