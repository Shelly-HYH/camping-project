# 用jieba挑出關鍵字
# 先合併單一露營場的所有評論後再取關鍵字

from sqlalchemy import create_engine
import pandas as pd
import jieba
import jieba.analyse
from pathlib import Path
import re


# 建立連線-----------------------
def load_data():
    host='35.201.136.64' # 主機位置
    user='shelly' # 使用者名稱
    port="3306" # 埠號
    password='shelly-password' # 密碼
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/campground_db"
    engine = create_engine(url, echo=False)

    # 用pandas讀取營位表
    with engine.connect() as connection:
        df = pd.read_sql("SELECT * FROM articles", con=engine)

    return df


def is_campsite_code(word):
    return re.match(r"^[A-Za-z]\d{1,2}$", word) is not None

def is_only_symbols(word):
    # 只由標點或符號組成
    return re.fullmatch(r"[\W_、，。！？：「」『』《》〈〉【】…—─·\-_.]+", word) is not None

def remove_punctuations(text):
    return re.sub(r"[，。、！？：；（）「」『』《》〈〉【】\[\]()~～…—\-\"\'`!@#$%^&*+=|\\/<>\n ]", "", text)


# 清洗並排除不需要的詞
def clean_and_extract(text, stopwords):
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

def group_content(df, stopwords):
    # Groupby 合併所有評論文字
    grouped_df = df.groupby("campground_ID")["content"].apply(
                            lambda texts: remove_punctuations(" ".join(t for t in texts if isinstance(t, str)))
                            ).reset_index(name="合併評論")

    grouped_df["關鍵字"] = grouped_df["合併評論"].apply(lambda x: clean_and_extract(x, stopwords))
    return grouped_df

def save_file(df):
    # 儲存結果
    save_path = Path("output", "mart")
    save_path.mkdir(parents=True, exist_ok=True)
    df.to_csv(save_path / "MART01_cut_keyword_groupby_campground.csv", index=False, encoding="utf-8-sig")

    print("OK")

