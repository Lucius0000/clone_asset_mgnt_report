# -*- coding: utf-8 -*-
"""
Created on Mon Jul 14 14:35:54 2025
@author: Lucius
"""

import os
import pandas as pd
from fredapi import Fred
import akshare as ak

# ========== 设置文件路径 ==========
OUTPUT_DIR = "output"
RAW_DIR = os.path.join(OUTPUT_DIR, "raw_data")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ========== 公共函数 ==========
def convert_quarter_str_to_date(qtr_str):
    """将如 '2025第1季度' 转换为 2025-03-31"""
    year = qtr_str[:4]
    quarter = qtr_str[-3]
    month_map = {'1': '03', '2': '06', '3': '09', '4': '12'}
    month = month_map.get(quarter, '12')
    return pd.to_datetime(f"{year}-{month}-01") + pd.offsets.MonthEnd(0)

# ========== 1. 获取美国GDP（FRED） ==========
def fetch_us_gdp():
    fred_api_key = os.getenv("FRED_API_KEY")
    if not fred_api_key:
        raise EnvironmentError("环境变量 FRED_API_KEY 未设置")
    fred = Fred(api_key=fred_api_key)
    series = fred.get_series('GDP')  # 单位：十亿 USD
    df = series.reset_index()
    df.columns = ['date', 'value']
    df['region'] = 'US'
    df['unit'] = 'USD'
    df.to_excel(os.path.join(RAW_DIR, "GDP_value_US.xlsx"), index=False)
    return df[df['value'].notna()].sort_values('date')

# ========== 2. 获取中国GDP（AkShare） ==========
def fetch_china_gdp():
    raw = ak.macro_china_gdp()
    raw.to_excel(os.path.join(RAW_DIR, "GDP_value_CN.xlsx"), index=False)
    df = raw[raw['国内生产总值-绝对值'].notna()].copy()
    df['value'] = df['国内生产总值-绝对值'] / 10
    df['date'] = df['季度'].apply(convert_quarter_str_to_date)
    df['region'] = 'CN'
    df['unit'] = 'CNY'
    return df[['date', 'value', 'region', 'unit']]

# ========== 3. 获取香港GDP（AkShare） ==========
def fetch_hk_gdp():
    raw = ak.macro_china_hk_gbp()
    raw.to_excel(os.path.join(RAW_DIR, "GDP_value_HK.xlsx"), index=False)
    df = raw[raw['现值'].notna()].copy()
    df['value'] = raw['现值'] / 1000  # 百万 -> 十亿
    df['date'] = raw['时间'].apply(convert_quarter_str_to_date)
    df['region'] = 'HK'
    df['unit'] = 'HKD'
    return df[['date', 'value', 'region', 'unit']]

# ========== 4. 组合数据并格式化输出 ==========
def combine_and_save():
    us = fetch_us_gdp()
    cn = fetch_china_gdp()
    hk = fetch_hk_gdp()

    all_data = pd.concat([us, cn, hk], ignore_index=True)
    latest = all_data.sort_values('date').groupby('region', as_index=False).tail(1)

    # 格式化
    latest['当前值'] = latest.apply(lambda r: f"{r['value']:,.0f}B {r['unit']}", axis=1)
    latest['当前值日期'] = latest['date'].apply(lambda d: f"{d.year}Q{(d.month - 1) // 3 + 1}")
    latest['原始日期'] = latest['date'].dt.strftime('%Y-%m-%d')

    summary = latest[['region', '当前值', '当前值日期', '原始日期']].copy()
    region_map = {'US': '美国', 'CN': '中国', 'HK': '香港'}
    summary.loc[:, '区域'] = summary['region'].map(region_map)
    summary = summary[['区域', '当前值', '当前值日期', '原始日期']]

    output_path = os.path.join(OUTPUT_DIR, 'gdp_value_summary.xlsx')
    
    summary.to_excel(output_path, index=False)

# ========== 主程序入口 ==========
if __name__ == "__main__":
    combine_and_save()
