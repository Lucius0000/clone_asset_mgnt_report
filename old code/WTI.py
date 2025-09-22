# -*- coding: utf-8 -*-
"""
Created on Wed Jul 23 15:06:12 2025

@author: Lucius
"""

import os
from datetime import datetime
from fredapi import Fred
import pandas as pd

# 从环境变量读取你的 API 密钥
fred = Fred(api_key=os.getenv("FRED_API_KEY"))

# 获取 FRED 中的 WTI 原油现货日度价格（美元/桶）
# 系列代码：DCOILWTICO
start_date = "2020-01-01"
end_date = datetime.today().strftime("%Y-%m-%d")

wti_series = fred.get_series("DCOILWTICO", observation_start=start_date, observation_end=end_date)

# 转换为 DataFrame
wti_df = wti_series.to_frame(name="WTI_Spot_Price")
wti_df.index.name = "Date"
wti_df = wti_df.dropna()

wti_df.to_excel('WTI.xlsx')


