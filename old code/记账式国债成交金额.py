# -*- coding: utf-8 -*-
"""
Created on Fri Aug  1 14:28:06 2025

@author: Lucius
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# 注意单位: 万元
# 设置日期范围：过去30个自然日
end_date = datetime.today()
start_date = end_date - timedelta(days=30)
date_range = pd.date_range(start=start_date, end=end_date)

# 存储每日详细数据和每日合计金额
all_data = []
summary_data = []

# 遍历日期获取数据
for date in date_range:
    date_str = date.strftime('%Y%m%d')
    try:
        df = ak.bond_deal_summary_sse(date=date_str)
        if df is not None and not df.empty:
            df["数据日期"] = date.strftime('%Y-%m-%d')  # 确保统一日期格式
            all_data.append(df)

            # 获取合计成交金额（可选）
            total_row = df[df['债券类型'] == '合计']
            if not total_row.empty:
                deal_amount = float(total_row['当日成交金额'].values[0])
                summary_data.append({'日期': date.strftime('%Y-%m-%d'), '成交金额（万元）': deal_amount})
    except Exception as e:
        continue  # 跳过异常日期

# 整合所有数据
full_df = pd.concat(all_data, ignore_index=True)
summary_df = pd.DataFrame(summary_data).sort_values(by='日期')

# 保存数据
output_path = Path("output")
output_path.mkdir(exist_ok=True)

full_df.to_excel(output_path / "all_bond_data.xlsx", index=False)
# summary_df.to_excel(output_path / "past_30_days_summary.xlsx", index=False)

# 每个债券种类求和（按“债券类型”分组）
group_sum_df = full_df.groupby("债券类型")["当日成交金额"].sum().reset_index()
group_sum_df.rename(columns={"当日成交金额": "30日总成交金额（万元）"}, inplace=True)
group_sum_df.to_excel(output_path / "bond_type_summary.xlsx", index=False)

# 获取“记账式国债”的总成交金额
guozhai_total = group_sum_df.loc[group_sum_df["债券类型"] == "记账式国债", "30日总成交金额（万元）"].values[0]

# 输出
print(f"记账式国债过去30个自然日总成交金额为：{guozhai_total:,.2f} 万元")
