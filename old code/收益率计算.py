# -*- coding: utf-8 -*-
"""
Created on Tue Aug  5 14:39:08 2025

@author: Lucius
"""

import akshare as ak
import pandas as pd
import numpy as np
import os
from datetime import timedelta

# 确保输出文件夹存在
os.makedirs("output", exist_ok=True)

def compute_bond_metrics_fixed(df: pd.DataFrame, col_name: str):
    df = df[['日期', col_name]].dropna().copy()
    df['日期'] = pd.to_datetime(df['日期'])
    df.set_index('日期', inplace=True)
    df.sort_index(inplace=True)

    # 计算实际日收益率（从年化利率还原）
    df['r_daily'] = (1 + df[col_name] / 100) ** (1 / 252) - 1

    # 当前月（过去30个自然日）的年化收益率
    end_date = df.index.max()
    start_date_30d = end_date - timedelta(days=30)
    recent_30d = df.loc[start_date_30d:end_date]
    annual_return_30d = (1 + recent_30d['r_daily']).prod() ** (252 / len(recent_30d)) - 1

    # 过去一年（365个自然日）的年收益率
    start_date_365d = end_date - timedelta(days=365)
    recent_1y = df.loc[start_date_365d:end_date]
    annual_return_1y = (1 + recent_1y['r_daily']).prod() - 1

    # 年化波动率
    vol_window = df.iloc[-252:]
    annualized_vol = vol_window['r_daily'].std() * np.sqrt(252)

    # Sharpe Ratio
    risk_free_rate = 0.017
    sharpe = (annual_return_1y - risk_free_rate) / annualized_vol if annualized_vol != 0 else np.nan

    # 返回四个值（百分比形式，保留两位小数）
    return (
        round(annual_return_30d * 100, 2),     # 当前月年化收益率
        round(annual_return_1y * 100, 2),      # 过去一年年收益率
        round(annualized_vol * 100, 2),        # 年化波动率
        round(sharpe, 2)                       # Sharpe Ratio
    )


def main():
    df = ak.bond_zh_us_rate(start_date="20180101")

    r30_2y, r1y_2y, vol_2y, sharpe_2y = compute_bond_metrics_fixed(df, '中国国债收益率2年')

    print("📊 计算10年期国债指标...")
    r30_10y, r1y_10y, vol_10y, sharpe_10y = compute_bond_metrics_fixed(df, '中国国债收益率10年')

    print("📄 2Y:", r30_2y, r1y_2y, vol_2y, sharpe_2y)
    print("📄10Y:", r30_10y, r1y_10y, vol_10y, sharpe_10y)

    print("✅ 完成。")


if __name__ == "__main__":
    main()
