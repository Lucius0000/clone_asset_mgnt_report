# -*- coding: utf-8 -*-
"""
Created on Tue Jul 15 10:57:50 2025

@author: Lucius
"""

import pandas as pd
import os

# 1. 加载标普500公司代码列表（CSV 文件）
sp500_df = pd.read_csv("data/constituents.csv")
sp500_df.columns = sp500_df.columns.str.upper()
sp500_symbols = set(sp500_df["SYMBOL"].astype(str).str.upper().str.strip())

# 2. 加载全市场数据（Excel 文件）
market_df = pd.read_excel("output/us_stock_us_spot.xlsx")
market_df.columns = market_df.columns.str.upper()

# 3. 清洗市场数据中的代码（去掉前缀）
market_df["代码_CLEAN"] = market_df["SYMBOL"].astype(str).str.upper().str.replace(r"^\d+\.", "", regex=True)

# 4. 进行匹配
matched_df = market_df[market_df["代码_CLEAN"].isin(sp500_symbols)]

# 5. 导出结果
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)
matched_df.to_excel(os.path.join(output_dir, "sp500_matched_market_data.xlsx"), index=False)

print(f"成功匹配 {len(matched_df)} 家标普500公司行情数据，已保存至 {output_dir}/sp500_matched_market_data.xlsx")
