# -*- coding: utf-8 -*-
"""
Created on Fri Aug  1 14:58:47 2025

@author: Lucius
"""

import pandas as pd
from datetime import datetime
import pandas_datareader.data as web
import os

# 设置时间区间
start_date = "2024-01-01"
end_date = datetime.today().strftime("%Y-%m-%d")

# FRED系列代码
fred_codes = {
    "2Y": "DGS2",
    "5Y": "DGS5",
    "10Y": "DGS10"
}

api_key = os.getenv("FRED_API_KEY")

data = {}
for label, code in fred_codes.items():
    df = web.DataReader(code, "fred", start_date, end_date, api_key=api_key)
    data[label] = df[code] / 100  # 百分比转小数

yield_df = pd.DataFrame(data)
yield_df.index.name = "Date"

output_path = "treasury_yields_fred.xlsx"
yield_df.to_excel(output_path)

print(f"成功保存至本地文件：{output_path}")
