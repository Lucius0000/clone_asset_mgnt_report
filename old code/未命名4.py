# -*- coding: utf-8 -*-
"""
Created on Mon Jul 14 13:15:28 2025

@author: Lucius
"""

import akshare as ak
import pandas as pd

# 1. 沪深300指数数据获取（通过中证指数网最新数据）
all_index_df = ak.index_all_cni()  # 获取国证（含中证）指数最近交易日的概要数据
hs300_data = all_index_df[all_index_df["指数代码"] == "000300"]  # 查找沪深300
if hs300_data.empty:
    hs300_data = all_index_df[all_index_df["指数代码"] == "399300"]  # 有时沪深300可能登记为399300
hs300_mc = float(hs300_data["总市值"]) * 1e8  # 总市值单位是亿元，转换为元
hs300_pe = float(hs300_data["PE滚动"])       # 滚动市盈率（倍）

# 2. 恒生指数数据计算（通过成份股列表汇总）
hsi_components = ["00001.HK", "00002.HK", "..."]  # 恒生50成份列表（示例）
hk_spot = ak.stock_hk_main_board_spot_em()        # 获取港股主板实时行情
hsi_df = hk_spot[hk_spot["代码"].isin(hsi_components)]
# 将市值列（港股接口中一般以港元为单位）求和:
hsi_total_mv = hsi_df["总市值"].astype(float).sum()
# 计算市盈率: 调用恒指官方PE值或用调和平均法
hsi_pe = None
if "市盈率" in hsi_df.columns:
    # 若有个股市盈率列，可计算调和平均PE
    weights = hsi_df["总市值"].astype(float) / hsi_total_mv
    inv_pe = (weights / hsi_df["市盈率"].astype(float)).sum()
    hsi_pe = 1/inv_pe if inv_pe != 0 else None

# 3. SPY数据获取（通过yfinance作为校验）
import yfinance as yf
spy = yf.Ticker("SPY")
spy_info = spy.info  # 字典形式包含市值和PE等
spy_mc = spy_info.get("marketCap")
spy_pe = spy_info.get("trailingPE")

# 4. 整理结果并输出
data = [
    {"指数": "SPY (标普500 ETF)", "总市值": f"${spy_mc:.2f}", "市盈率": spy_pe},
    {"指数": "沪深300指数", "总市值": f"{hs300_mc/1e12:.2f} 万亿人民币", "市盈率": hs300_pe},
    {"指数": "恒生指数", "总市值": f"{hsi_total_mv/1e12:.2f} 万亿港元", "市盈率": f"{hsi_pe:.2f}" if hsi_pe else "N/A"}
]
df = pd.DataFrame(data)
df.to_excel("index_valuation_akshare.xlsx", index=False)
print("数据已保存至 index_valuation_akshare.xlsx")
