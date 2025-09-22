# -*- coding: utf-8 -*-
"""
美国国债总发行量获取：https://fiscaldata.treasury.gov/datasets/record-setting-auction-data/record-setting-auction
"""

import os
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime

os.environ['http_proxy'] = 'http://127.0.0.1:7890'
os.environ['https_proxy'] = 'http://127.0.0.1:7890'

symbol = "^TNX"
risk_free_rate = 0.042
output_folder = "output"
raw_data_folder = os.path.join(output_folder, "raw_data")

os.makedirs(raw_data_folder, exist_ok=True)

data = yf.download(symbol, period="3y", interval="1d")
data.dropna(inplace=True)

raw_data_path = os.path.join(raw_data_folder, f"{symbol.replace('^','')}_raw.csv")
data.to_csv(raw_data_path)

# 计算日收益率
data["daily_return"] = data["Close"].pct_change()
data.dropna(inplace=True)

# 当前收盘价
current_close = data["Close"].iloc[-1]

# 月收益率年化（按近30日平均计算）
monthly_return = data["daily_return"].iloc[-30:].mean()
monthly_return_pct = monthly_return * 100

# 年收益率（按年初至今）
start_of_year = datetime(datetime.now().year, 1, 1)
data_ytd = data[data.index >= pd.to_datetime(start_of_year)]
if not data_ytd.empty:
    annual_return = (data_ytd["Close"].iloc[-1] / data_ytd["Close"].iloc[0] - 1) * 100
else:
    annual_return = np.nan

# 年化波动率
annual_volatility = np.std(data["daily_return"]) * np.sqrt(252) * 100

# 夏普比率
sharpe_ratio = ((monthly_return - risk_free_rate) / (np.std(data["daily_return"]) * np.sqrt(252))) if np.std(data["daily_return"]) > 0 else np.nan

# 构造结果表格
results = pd.DataFrame([{
    "指标": "10年期美债收益率（^TNX）",
    "当前收盘价": round(current_close, 2),
    "月收益率年化 (%)": round(monthly_return_pct, 2),
    "年收益率 (%)": round(annual_return, 2),
    "年化波动率 (%)": round(annual_volatility, 2),
    "总市值 ($)": "",  # 留空
    "Sharpe Ratio": round(sharpe_ratio, 2)
}])

# 保存结果
result_path = os.path.join(output_folder, "TNX_summary_metrics.xlsx")
results.to_excel(result_path, index=False)

