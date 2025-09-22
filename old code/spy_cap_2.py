# -*- coding: utf-8 -*-
"""
Created on Tue Jul 15 00:17:06 2025
@author: Lucius
"""

import pandas as pd
import akshare as ak

# 1. 下载标普500成分股列表
# url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
url = r"C:\Users\Lucius\Desktop\wealth-hunter\asset_mgnt_report\data\constituents.csv"
sp500_df = pd.read_csv(url)
symbols = sp500_df["Symbol"].tolist()
symbols_upper = [s.upper() for s in symbols]

# 2. 获取全量美股行情（加入异常处理）
try:
    spot_df = ak.stock_us_spot_em()
    spot_df.to_excel('output/us_stock_us_spot_em.xlsx', index = False)
except Exception as e:
    print("获取美股行情失败，可能是网络或代理问题。错误信息：", e)
    spot_df = pd.DataFrame()

if not spot_df.empty:
    # 标准化列名
    spot_df.columns = [col.lower() for col in spot_df.columns]

    # 提取真正的 symbol（如 AAPL）
    spot_df['symbol'] = spot_df['代码'].str.extract(r'\d+\.(.*)')[0].str.upper()

    # 3. 提取匹配的 symbol 数据
    filtered_df = spot_df[spot_df['symbol'].isin(symbols_upper)]

    # 4. 转换市值为数值
    def parse_market_cap(value):
        try:
            if isinstance(value, str):
                value = value.replace(",", "")
            return float(value)
        except:
            return None

    filtered_df.loc[:, "总市值"] = filtered_df["总市值"].apply(parse_market_cap)
    total_market_cap = filtered_df["总市值"].sum()

    # 5. 转换为十亿美元（Billion USD）
    total_market_cap_billion = total_market_cap / 1e9
    print(f"标普500总市值估算为: {total_market_cap_billion:,.2f} Billion USD")

    # 6. 记录未匹配的 symbol
    matched_symbols = set(filtered_df["symbol"])
    unmatched = [s for s in symbols_upper if s not in matched_symbols]
    if unmatched:
        with open("output/failed_symbols_2.txt", "w") as f:
            f.write("\n".join(unmatched))
        print(f"共 {len(unmatched)} 个 symbol 未匹配行情，已记录到 failed_symbols_2.txt")
else:
    print("无法获取行情数据，后续流程已跳过。")
