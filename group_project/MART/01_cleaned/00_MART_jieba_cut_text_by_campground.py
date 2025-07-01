# 用jieba挑出關鍵字
# 先合併單一露營場的所有評論後再取關鍵字

from sqlalchemy import create_engine
import pandas as pd
import jieba
import jieba.analyse
from pathlib import Path
import re

# 建立連線-----------------------
host='104.199.214.113' # 主機位置
user='test' # 使用者名稱
port="3307" # 埠號
password='PassWord_1' # 密碼
url = f"mysql+pymysql://{user}:{password}@{host}:{port}/test2_db"
engine = create_engine(url, echo=True, pool_pre_ping=True)

# 用pandas讀取營位表
with engine.connect() as connection:
    df = pd.read_sql("SELECT * FROM articles", con=engine)

# 讀取自訂的停用詞清單
stopwords_path = Path(r"C:\TJR101_03_Project\.venv\MART\stopwords.txt")

with open(stopwords_path, "r", encoding="utf-8") as f:
    stopwords = set(line.strip() for line in f)

def is_campsite_code(word):
    return re.match(r"^[A-Za-z]\d{1,2}$", word) is not None

def is_only_symbols(word):
    # 只由標點或符號組成
    return re.fullmatch(r"[\W_、，。！？：「」『』《》〈〉【】…—─·\-_.]+", word) is not None

def remove_punctuations(text):
    return re.sub(r"[，。、！？：；（）「」『』《》〈〉【】\[\]()~～…—\-\"\'`!@#$%^&*+=|\\/<>\n ]", "", text)


# 同樣斷詞與關鍵字
def clean_and_extract(text):
    words = jieba.lcut(text)
    words = [
        w for w in words
        if w.strip()
        and w not in stopwords
        and not re.match(r"^\d+(\.\d+)?$", w)       # 數字或浮點數
        and not is_campsite_code(w)                 # 新增過濾營位編號
        and not re.match(r"^\d+$", w)
        and not re.match(r"^[\d\.]+[a-zA-Z%]+$", w)
        and not (w.isascii() and w.isalpha() and len(w) <= 5)
        and not is_only_symbols(w)
    ]
    clean_text = " ".join(words)
    tfidf_kw = set(jieba.analyse.extract_tags(clean_text, topK = 30))
    textrank_kw = set(jieba.analyse.textrank(clean_text, topK = 30))
    combined_kw = sorted(set(tfidf_kw | textrank_kw))
    return "｜".join(combined_kw)


# Groupby 合併所有評論文字
grouped_df = df.groupby("campground_ID")["content"].apply(
                        lambda texts: remove_punctuations(" ".join(t for t in texts if isinstance(t, str)))
                        ).reset_index(name="合併評論")

grouped_df["關鍵字"] = grouped_df["合併評論"].apply(clean_and_extract)

# 儲存結果
save_path = Path(".venv", "MART", "result_csv")
save_path.mkdir(parents=True, exist_ok=True)
grouped_df.to_csv(save_path / "MART00_cut_keyword_groupby_campground.csv", index=False, encoding="utf-8-sig")

print("OK")

