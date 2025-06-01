from wordcloud import WordCloud
import matplotlib.pyplot as plt
from pathlib import Path
import pandas as pd
import matplotlib.font_manager as fm
import re
from collections import Counter
from sqlalchemy import create_engine
import jieba
import jieba.analyse


# # 建立連線-----------------------
# host='104.199.214.113' # 主機位置
# user='test' # 使用者名稱
# port="3307" # 埠號
# password='PassWord_1' # 密碼
# url = f"mysql+pymysql://{user}:{password}@{host}:{port}/test2_db"
# engine = create_engine(url, echo=True, pool_pre_ping=True)

# # 用pandas讀取營位表
# with engine.connect() as connection:
#     df = pd.read_sql("SELECT * FROM articles", con=engine)

# # 讀取自訂的停用詞清單
# stopwords_path = Path(r"C:\TJR101_03_Project\.venv\MART\stopwords.txt")

# with open(stopwords_path, "r", encoding="utf-8") as f:
#     stopwords = set(line.strip() for line in f)

# def is_campsite_code(word):
#     return re.match(r"^[A-Za-z]\d{1,2}$", word) is not None

# def is_only_symbols(word):
#     # 只由標點或符號組成
#     return re.fullmatch(r"[\W_、，。！？：「」『』《》〈〉【】…—─·\-_.]+", word) is not None

# def remove_punctuations(text):
#     return re.sub(r"[，。、！？：；（）「」『』《》〈〉【】\[\]()~～…—\-\"\'`!@#$%^&*+=|\\/<>\n ]", "", text)


# # 同樣斷詞與關鍵字
# def clean_and_extract(text):
#     words = jieba.lcut(text)
#     words = [
#         w for w in words
#         if w.strip()
#         and w not in stopwords
#         and not re.match(r"^\d+(\.\d+)?$", w)       # 數字或浮點數
#         and not is_campsite_code(w)                 # 新增過濾營位編號
#         and not re.match(r"^\d+$", w)
#         and not re.match(r"^[\d\.]+[a-zA-Z%]+$", w)
#         and not (w.isascii() and w.isalpha() and len(w) <= 5)
#         and not is_only_symbols(w)
#     ]
#     clean_text = " ".join(words)
#     tfidf_kw = set(jieba.analyse.extract_tags(clean_text, topK = 30))
#     textrank_kw = set(jieba.analyse.textrank(clean_text, topK = 30))
#     combined_kw = sorted(set(tfidf_kw | textrank_kw))
#     return "｜".join(combined_kw)


# # Groupby 合併所有評論文字
# grouped_df = df.groupby("campground_ID")["content"].apply(
#                         lambda texts: remove_punctuations(" ".join(t for t in texts if isinstance(t, str)))
#                         ).reset_index(name="合併評論")

# grouped_df["關鍵字"] = grouped_df["合併評論"].apply(clean_and_extract)
# grouped_df = grouped_df[["campground_ID", "關鍵字"]].copy()

# ---------------------------------------

# 讀取jieba關鍵字
text = Path(r"C:\TJR101_03_Project\.venv\MART\result_csv\MART00_cut_keyword_groupby_campground.csv")
df = pd.read_csv(text, encoding="utf-8-sig")
df = df[["campground_ID", "關鍵字"]].copy()

# 讀取TAG檔案
tag_file = Path(r"C:\TJR101_03_Project\.venv\MART\result_csv\MART01_ai_add_campground_tag.csv")
df_tag = pd.read_csv(tag_file, encoding="utf-8-sig")

# 讀取露營場表
campground_list = Path(r"C:\TJR101_03_Project\.venv\MART\data_from_mysql\MySQL_MART_campground_infos.csv")
df_campground = pd.read_csv(campground_list, encoding="utf-8-sig")


# -------------------------------------------------
# 所有露營場用----------------------------------
df_merged = df_campground.merge(df, on="campground_ID", how="inner")
save_path = Path(".venv", "MART", "for_wordcloud")
save_file = save_path / "merged_all.csv"
df_merged.to_csv(save_file, index=False, encoding="utf-8-sig")
print("OK")


# 合併露營場表+TAG---------------------------------------
df_merged2 = df_campground.merge(df_tag, left_on="campground_ID", right_on="露營場ID", how="inner")
save_path = Path(".venv", "MART", "for_wordcloud")
save_file = save_path / "merged_tag.csv"
df_merged2.to_csv(save_file, index=False, encoding="utf-8-sig")
print("OK")

# ---------------------------------------

ref_dict = {
    "營主": ["營主", "營主們", "營主夫", "營主二爺", "營主燁哥", "營主爺" , "營主伯", 
           "營主們",  "營主羊哥","營主夫", "營主夫婦", "營主娘", "營主媽", "營主媽媽", 
           "營主小承", "營主小英姐", "營主淑玲", "主人"],
    "小孩": ["小孩", "小朋友", "孩子", "孩子們", "兒童", "女兒", "兒子"],
    "老闆": ["老闆", "老闆娘"],
    "豪華露營": ["豪華", "豪華露", "豪華露營", "豪華免", "Glamping", "奢華"],
    "戲水": ["戲水", "戲水區"],
    "服務態度佳": ["貼心", "營主熱心", "營主熱情", "服務"],
    "營主用心經營": ["用心", "營主用心經營", "營主用心經營(此TAG不存在，故不選取)", "營主盡力",],
    "老師": ["老師", "老師們"],
    "老闆": ["老板", "老板娘", "老版", "老闆", "老闆娘"],
    "草地": ["草皮", "草坪", "草地"],
    "帳棚": ["帳篷", "營帳", "帳棚區", "帳棚"],
    "蚊子": ["小黑蚊", "蚊蟲", ],
    "天氣": ["氣候", "天候"],
    "乾淨": ["清潔", "整潔", "衛生"],
    "漂亮": ["漂亮", "很漂亮"],
    "家庭": ["家人", "家庭", "一家人", "全家", "家族", "親朋"],
    "熱情": ["熱情", "好客", "熱心"],
    "舒適": ["舒適", "舒服"],
    "朋友": ["朋友", "好友", "大家"],
    "包場": ["包場", "包全場"],
    "開會": ["討會", "議室", "開檢討會"],
    "懶人": ["懶人", "懶人露", "懶人露營", "懶人帳"],
    "免搭帳": ["免搭帳", "免搭"],
    "房間": ["套房", "房間"],
    "維護": ["維護", "整理", "管理"]
}

black_list = ["住露", "送車", "三五", "態度", "二食", "感謝", "適合", "適合帶", 
              "戲區", "營", "主營位", "營主當",  "營主聯絡", "營主能", "營主花費", 
              "主親", "營主說", "營主貼", "營主超", "營主還", "營主開", "營主養", "營主在場",  
              "營主帥", "營主有", "上百人", "開著", "入住", "整間", "服務人員", "經營"]


# 匯入中文字形
zh_font_path = "C:/Windows/Fonts/msjh.ttc"
zh_font = fm.FontProperties(fname=zh_font_path)

# ---------------------------------------

word_to_main = {}  # 建立一個空的字典

for main, group in ref_dict.items():  
    # 對 ref_dict 裡的每一組「主詞彙 : 同義詞列表」做處理
    for word in group:    # 把這個 group 裡的每個同義詞
        word_to_main[word] = main   # 都存到新字典裡，指向它的主詞彙

cleaned_keywords = []

for i in df_merged.index:
    raw = df_merged.loc[i, "關鍵字"]
    if pd.isna(raw):
        cleaned_keywords.append("")
        continue

    words = raw.split("｜")
    result = []
    for word in words:
        if word in black_list:
                continue
        
        if word in word_to_main:
            result.append(word_to_main[word])
        else:
            result.append(word)

    cleaned_keywords.append("｜".join(result))


df_merged["cleaned_keywords"] = cleaned_keywords
output_path = Path(".venv", "MART", "for_wordcloud")
output_file = output_path / "output_file1.csv"
df_merged.to_csv(output_file, index=False, encoding="utf-8-sig")



# # 全台露營場文字雲-----------------------------------
# # 擷取關鍵字欄位並展開成詞列表
# keywords_series = df_merged["cleaned_keywords"].dropna().str.split("｜")  # 改為列表
# all_keywords = [kw.strip() for sublist in keywords_series for kw in sublist if kw.strip() != ""]

# # 統計詞頻
# word_freq = Counter(all_keywords)

# # 建立文字雲
# wc = WordCloud(
#     font_path="msjh.ttc",
#     width=1000,
#     height=800,
#     background_color="white",
#     prefer_horizontal=0.9,
#     colormap="cividis" ,
#     max_words = 20,
# ).generate_from_frequencies(word_freq)

# # 顯示圖形

# plt.figure(figsize=(10, 5))
# plt.imshow(wc, interpolation="bilinear")
# plt.axis("off")
# plt.title("全台露營場關鍵字", fontproperties=zh_font, fontsize=20)
# plt.show()


# # 好評露營場文字雲-----------------------------------

# good_campground = df_merged[df_merged["campground_category"] == "好評"]
# keywords_series = good_campground["cleaned_keywords"].dropna().str.split("｜")  # 改為列表
# all_keywords = [kw.strip() for sublist in keywords_series for kw in sublist if kw.strip() != ""]

# # 統計詞頻
# word_freq = Counter(all_keywords)

# # 建立文字雲
# wc = WordCloud(
#     font_path="msjh.ttc",
#     width=1000,
#     height=800,
#     background_color="white",
#     prefer_horizontal=1.0,
#     colormap="cividis" ,
#     max_words = 30,
# ).generate_from_frequencies(word_freq)

# # 顯示圖形

# plt.figure(figsize=(10, 5))
# plt.imshow(wc, interpolation="bilinear")
# plt.axis("off")
# plt.title("好評露營場關鍵字", fontproperties=zh_font, fontsize=20)
# plt.show()


# # 熱門露營場文字雲-----------------------------------

# hot_campground = df_merged[df_merged["hot"] == "Hot"]
# keywords_series = hot_campground["cleaned_keywords"].dropna().str.split("｜")  # 改為列表
# all_keywords = [kw.strip() for sublist in keywords_series for kw in sublist if kw.strip() != ""]

# # 統計詞頻
# word_freq = Counter(all_keywords)

# # 建立文字雲
# wc = WordCloud(
#     font_path="msjh.ttc",
#     width=1000,
#     height=800,
#     background_color="white",
#     prefer_horizontal=0.9,
#     colormap="cividis" ,
#     max_words = 30,
# ).generate_from_frequencies(word_freq)

# # 顯示圖形

# plt.figure(figsize=(10, 5))
# plt.imshow(wc, interpolation="bilinear")
# plt.axis("off")
# plt.title("Hot露營場關鍵字", fontproperties=zh_font, fontsize=20)
# plt.show()



# # 熱門又好評的露營場-------------------------------------------------
# # 篩選同時符合 Hot + 好評 的露營場

# filtered_campground = df_merged[
#     (df_merged["hot"] == "Hot") & 
#     (df_merged["campground_category"] == "好評")
# ]

# # 關鍵字處理
# keywords_series = filtered_campground["cleaned_keywords"].dropna().str.split("｜")  # 拆成關鍵字列表
# all_keywords = [kw.strip() for sublist in keywords_series for kw in sublist if kw.strip() != ""]

# # 統計詞頻
# word_freq = Counter(all_keywords)

# # 建立文字雲
# wc = WordCloud(
#     font_path="msjh.ttc",
#     width=1000,
#     height=800,
#     background_color="white",
#     prefer_horizontal=0.9,
#     colormap="cividis",
#     max_words=30,
# ).generate_from_frequencies(word_freq)

# # 顯示圖形
# plt.figure(figsize=(10, 5))
# plt.imshow(wc, interpolation="bilinear")
# plt.axis("off")
# plt.title("Hot + 好評露營場關鍵字", fontproperties=zh_font, fontsize=20)
# plt.show()

# # ---------------------------------------
# print("TAG文字雲")
# # ---------------------------------------

cleaned_keywords2 = []

for i in df_merged2.index:
    raw = df_merged2.loc[i, "對應的關鍵字"]
    if pd.isna(raw):
        cleaned_keywords2.append("")
        continue

    words = raw.split("｜")
    result = []
    for word in words:
        if word in black_list:
                continue
        
        if word in word_to_main:
            result.append(word_to_main[word])
        else:
            result.append(word)

    cleaned_keywords2.append("｜".join(result))


df_merged2["cleaned_keywords"] = cleaned_keywords2
output_path = Path(".venv", "MART", "for_wordcloud")
output_file2 = output_path / "output_file2.csv"
df_merged2.to_csv(output_file2, index=False, encoding="utf-8-sig")

# # 豪華露營文字雲-----------------------------------

# lux_campground = df_merged2[df_merged2["Tag"] == "豪華露營"]
# keywords_series = lux_campground["cleaned_keywords"].dropna().str.split("｜")  # 改為列表
# all_keywords = [kw.strip() for sublist in keywords_series for kw in sublist if kw.strip() != ""]

# # 統計詞頻
# word_freq = Counter(all_keywords)

# # 建立文字雲
# wc = WordCloud(
#     font_path="msjh.ttc",
#     width=1000,
#     height=800,
#     background_color="white",
#     prefer_horizontal=0.9,
#     colormap="cividis" ,
#     max_words = 30,
# ).generate_from_frequencies(word_freq)

# # 顯示圖形

# plt.figure(figsize=(10, 5))
# plt.imshow(wc, interpolation="bilinear")
# plt.axis("off")
# plt.title("豪華露營關鍵字", fontproperties=zh_font, fontsize=20)
# plt.show()

# 親子友善文字雲-----------------------------------
exclude = {"小孩", "親子"}

kid_campground = df_merged2[df_merged2["Tag"] == "親子友善"]
keywords_series = kid_campground["cleaned_keywords"].dropna().str.split("｜")  # 改為列表
all_keywords = [kw.strip() for sublist in keywords_series for kw in sublist if kw.strip() != ""]

all_keywords = [word for word in all_keywords if word not in exclude]

# 統計詞頻
word_freq = Counter(all_keywords)

# 建立文字雲
wc = WordCloud(
    font_path="msjh.ttc",
    width=1000,
    height=800,
    background_color="white",
    prefer_horizontal=0.9,
    colormap="cividis" ,
    max_words = 30,
).generate_from_frequencies(word_freq)

# 顯示圖形

plt.figure(figsize=(10, 5))
plt.imshow(wc, interpolation="bilinear")
plt.axis("off")
plt.title("親子友善關鍵字", fontproperties=zh_font, fontsize=20)
plt.show()


# # 適合團體或家族出遊文字雲-----------------------------------

# group_campground = df_merged2[df_merged2["Tag"] == "適合團體或家族出遊"]
# keywords_series = group_campground["cleaned_keywords"].dropna().str.split("｜")  # 改為列表
# all_keywords = [kw.strip() for sublist in keywords_series for kw in sublist if kw.strip() != ""]

# # 統計詞頻
# word_freq = Counter(all_keywords)

# # 建立文字雲
# wc = WordCloud(
#     font_path="msjh.ttc",
#     width=1000,
#     height=800,
#     background_color="white",
#     prefer_horizontal=0.9,
#     colormap="cividis" ,
#     max_words = 30,
# ).generate_from_frequencies(word_freq)

# # 顯示圖形

# plt.figure(figsize=(10, 5))
# plt.imshow(wc, interpolation="bilinear")
# plt.axis("off")
# plt.title("適合團體或家族出遊關鍵字", fontproperties=zh_font, fontsize=20)
# plt.show()


# # 懶人露營文字雲-----------------------------------

# exclude = {"不用", "搭帳", "裝備"}

# lazy_campground = df_merged2[df_merged2["Tag"] == "免搭帳篷(懶人露營)"]
# keywords_series = lazy_campground["cleaned_keywords"].dropna().str.split("｜")  # 改為列表
# all_keywords = [kw.strip() for sublist in keywords_series for kw in sublist if kw.strip() != ""]

# all_keywords = [word for word in all_keywords if word not in exclude]

# # 統計詞頻
# word_freq = Counter(all_keywords)

# # 建立文字雲
# wc = WordCloud(
#     font_path="msjh.ttc",
#     width=1000,
#     height=800,
#     background_color = "white",
#     prefer_horizontal=0.9,
#     colormap="cividis" ,
#     max_words = 30,
# ).generate_from_frequencies(word_freq)

# # 顯示圖形

# plt.figure(figsize=(10, 5))
# plt.imshow(wc, interpolation="bilinear")
# plt.axis("off")
# plt.title("免搭帳篷(懶人露營)關鍵字", fontproperties=zh_font, fontsize=20)
# plt.show()

