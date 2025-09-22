'''
获取恒生指数总市值
算法：求和每个成分股市值
获取恒生指数成分股市值：https://www.aastocks.com/tc/stocks/market/index/hk-index-con.aspx?index=HSI
'''
import pandas as pd

# 读取文件
file_path = "data/AASTOCKS_Export_2025-7-14.xlsx"
df = pd.read_excel(file_path)

# 清洗“市值”字段（如“1,838.42億” -> 1838.42）
df["市值（亿港元）"] = (
    df["市值"]
    .astype(str)
    .str.replace("億", "", regex=False)
    .str.replace(",", "", regex=False)
    .astype(float)
)

# 计算市值（单位：十亿港元 B HKD）
df["市值（B HKD）"] = df["市值（亿港元）"] / 10
df["市值（B HKD）"] = df["市值（B HKD）"].map(lambda x: f"{x:,.2f}B HKD")

total_b = df["市值（亿港元）"].sum() / 10
formatted_total = f"{total_b:,.2f}B HKD"

result_df = df[["名稱", "代號", "市值（B HKD）"]].sort_values(by="市值（B HKD）", ascending=False)

print(f"恒生指数总市值为：{formatted_total}")

# 保存结果
result_df.to_excel("output\恒生指数成分股市值明细.xlsx", index=False)