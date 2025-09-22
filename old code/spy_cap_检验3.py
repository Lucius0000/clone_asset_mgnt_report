# -*- coding: utf-8 -*-
"""
Created on Tue Jul 15 12:50:47 2025

@author: Lucius
"""

import pandas as pd

# 读取两个 Excel 文件
file_em = "output/sp500_matched_market_data_em.xlsx"
file_orig = "output/sp500_matched_market_data.xlsx"

# 读取数据
df_em = pd.read_excel(file_em)
df_orig = pd.read_excel(file_orig)

# 筛选出包含代码和市值的列，并统一列名
df_em_filtered = df_em[['代码_CLEAN', '总市值']].rename(columns={'总市值': 'EM_MKTCAP'})
df_orig_filtered = df_orig[['代码_CLEAN', 'MKTCAP']].rename(columns={'MKTCAP': 'ORIG_MKTCAP'})

# 合并两个表格，依据代码_CLEAN
merged_df = pd.merge(df_em_filtered, df_orig_filtered, on='代码_CLEAN', how='inner')

# 转换市值列为数值类型
merged_df['EM_MKTCAP'] = pd.to_numeric(merged_df['EM_MKTCAP'], errors='coerce')
merged_df['ORIG_MKTCAP'] = pd.to_numeric(merged_df['ORIG_MKTCAP'], errors='coerce')

# 计算差额
merged_df['ABS_DIFF'] = merged_df['EM_MKTCAP'] - merged_df['ORIG_MKTCAP']
merged_df['REL_DIFF'] = abs(merged_df['ABS_DIFF']) / merged_df[['EM_MKTCAP', 'ORIG_MKTCAP']].max(axis=1)
merged_df['IS_MATCH'] = merged_df['REL_DIFF'] < 0.01

# 输出不一致的股票
mismatch_df = merged_df[~merged_df['IS_MATCH']].copy()

# 统计正负方向总差额
positive_total = mismatch_df[mismatch_df['ABS_DIFF'] > 0]['ABS_DIFF'].sum()
negative_total = mismatch_df[mismatch_df['ABS_DIFF'] < 0]['ABS_DIFF'].sum()

# 保存结果到 Excel
mismatch_df.to_excel("output/mismatch_market_cap.xlsx", index=False)

# 打印简要信息
print("市值不一致的股票数量：", len(mismatch_df))
print(f"正向差额总和（EM > ORIG）：{positive_total:,.0f}")
print(f"负向差额总和（EM < ORIG）：{negative_total:,.0f}")
print(mismatch_df.head())
