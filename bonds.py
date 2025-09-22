"""
债券，输出bond.xlsx
"""
import pandas as pd
import akshare as ak
from datetime import datetime, timedelta
from pathlib import Path
import numpy as np
import os
from fredapi import Fred
import pandas_datareader.data as web
from openpyxl import Workbook
from openpyxl.styles import Alignment
from pathlib import Path

raw_path = Path('output/raw_data')
raw_path.mkdir(exist_ok=True)
output_path = Path("output")
output_path.mkdir(exist_ok=True)

''' 中国 '''
''' 获取国债总市值 '''
# 单位: 亿元； 注意时间容错
bond_cash_summary_sse_df = ak.bond_cash_summary_sse(date='20250731')
bond_cash_summary_sse_df.to_excel(raw_path / '国债总市值.xlsx', index=False)
china_cap = f"{float(bond_cash_summary_sse_df[bond_cash_summary_sse_df['债券现货'] == '国债']['托管面值']) / 10:,.0f} B CNY"

''' 获取国债交易量 '''
# 注意单位: 万元
# 设置日期范围：过去30个自然日
end_date = datetime.today()
start_date = end_date - timedelta(days=30)
date_range = pd.date_range(start=start_date, end=end_date)

all_data = []
summary_data = []

for date in date_range:
    date_str = date.strftime('%Y%m%d')
    try:
        df = ak.bond_deal_summary_sse(date=date_str)
        if df is not None and not df.empty:
            df["数据日期"] = date.strftime('%Y-%m-%d')
            all_data.append(df)

            total_row = df[df['债券类型'] == '合计']
            if not total_row.empty:
                deal_amount = float(total_row['当日成交金额'].values[0])
                summary_data.append({'日期': date.strftime('%Y-%m-%d'), '成交金额（万元）': deal_amount})
    except Exception as e:
        continue

full_df = pd.concat(all_data, ignore_index=True)
summary_df = pd.DataFrame(summary_data).sort_values(by='日期')
full_df.to_excel(raw_path / "all_bond_data.xlsx", index=False)

# 获取“记账式国债”的最新日期当日成交金额
latest_date = full_df["数据日期"].max()
latest_record = full_df[(full_df["数据日期"] == latest_date) & (full_df["债券类型"] == "记账式国债")]
if not latest_record.empty:
    china_volume_day = f'{float(latest_record["当日成交金额"].values[0]) / 100000:,.0f} B CNY'
else:
    china_volume_day = np.nan


# 每个债券种类求和
group_sum_df = full_df.groupby("债券类型")["当日成交金额"].sum().reset_index()
group_sum_df.rename(columns={"当日成交金额": "30日总成交金额（万元）"}, inplace=True)
group_sum_df.to_excel(raw_path / "bond_volumn_summary.xlsx", index=False)

# 获取“记账式国债”的当月总成交金额
china_volume_month = f'{float(group_sum_df.loc[group_sum_df["债券类型"] == "记账式国债", "30日总成交金额（万元）"].values[0]) / 100000:,.0f} B CNY'

''' 获取国债收益率 '''
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

df = ak.bond_zh_us_rate(start_date="20240101")
cn_r30_2y, cn_r1y_2y, cn_vol_2y, cn_sharpe_2y = compute_bond_metrics_fixed(df, '中国国债收益率2年')
cn_r30_10y, cn_r1y_10y, cn_vol_10y, cn_sharpe_10y = compute_bond_metrics_fixed(df, '中国国债收益率10年')

''' 美国 '''
''' 总市值 '''
fred = Fred(api_key=os.environ['FRED_API_KEY'])
series = fred.get_series('GFDEBTN')  # 单位：百万美元
latest_date = series.index.max().date()
latest_value = series.max()  # 最新值
us_cap = f"{latest_value/1e3:,.0f} B USD"

''' 收益率 '''
start_date = "2024-01-01"
end_date = datetime.today().strftime("%Y-%m-%d")

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
yield_df.to_excel(raw_path / '美债收益率.xlsx', index=False)

def compute_us_bond_metrics(df: pd.DataFrame, col_name: str):
    df = df[[col_name]].dropna().copy()
    df.index = pd.to_datetime(df.index)
    df.sort_index(inplace=True)

    # 计算日收益率（从年化利率估算）
    df['r_daily'] = (1 + df[col_name]) ** (1 / 252) - 1

    # 当前月（30个自然日）年化收益率
    end_date = df.index.max()
    start_date_30d = end_date - timedelta(days=30)
    recent_30d = df.loc[start_date_30d:end_date]
    annual_return_30d = (1 + recent_30d['r_daily']).prod() ** (252 / len(recent_30d)) - 1

    # 过去一年（365自然日）实际年收益率
    start_date_365d = end_date - timedelta(days=365)
    recent_1y = df.loc[start_date_365d:end_date]
    annual_return_1y = (1 + recent_1y['r_daily']).prod() - 1

    # 年化波动率
    vol_window = recent_1y
    annualized_vol = vol_window['r_daily'].std() * np.sqrt(252)

    # Sharpe Ratio
    risk_free_rate = 0.045
    sharpe = (annual_return_1y - risk_free_rate) / annualized_vol if annualized_vol != 0 else np.nan

    return (
        round(annual_return_30d * 100, 2),    # 当前月年化收益率
        round(annual_return_1y * 100, 2),     # 年收益率
        round(annualized_vol * 100, 2),       # 年化波动率
        round(sharpe, 2)                      # Sharpe Ratio
    )

us_r30_2y, us_r1y_2y, us_vol_2y, us_sharpe_2y = compute_us_bond_metrics(yield_df, "2Y")
us_r30_10y, us_r1y_10y, us_vol_10y, us_sharpe_10y = compute_us_bond_metrics(yield_df, "10Y")

''' 输出表格 '''
rows = [
    ['指标类别', '指标', '种类', '月收益率年化 (%)', '年收益率 (%)', '年化波动率（%）', '总市值 ($)', '当日交易量', '月交易量'],
    ['中国', '记账式国债', '2年期',
     f"{cn_r30_2y:.2f}%", f"{cn_r1y_2y:.2f}%", f"{cn_vol_2y:.2f}%",
     china_cap, china_volume_day, china_volume_month],
    [None, None, '10年期',
     f"{cn_r30_10y:.2f}%", f"{cn_r1y_10y:.2f}%", f"{cn_vol_10y:.2f}%",
     None, None, None],
    [None, '储蓄式国债', '3年期', None, None, None, None, None, None],
    [None, None, '5年期', None, None, None, None, None, None],
    ['美国', '记账式国债', '2年期',
     f"{us_r30_2y:.2f}%", f"{us_r1y_2y:.2f}%", f"{us_vol_2y:.2f}%",
     us_cap, None, None],
    [None, None, '10年期',
     f"{us_r30_10y:.2f}%", f"{us_r1y_10y:.2f}%", f"{us_vol_10y:.2f}%",
     None, None, None],
    [None,'储蓄式国债', 'EE bonds',None, None, None,None, None, None],
    [None, None,'I bonds',None, None,None, None, None],
]



# 填“-”
rows = [[cell if cell is not None else '-' for cell in row] for row in rows]

# 创建工作簿并写入
wb = Workbook()
ws = wb.active
ws.title = 'bond'

for row in rows:
    ws.append(row)

# 所有单元格居中
for row in ws.iter_rows():
    for cell in row:
        cell.alignment = Alignment(horizontal='center', vertical='center')

# 合并单元格
merge_ranges = ['A2:A5', 'A6:A9', 'B2:B3', 'B4:B5', 'B6:B7','B8:B9','D4:E4','D5:E5','G2:G3','G6:G9','H2:H3','I2:I3','D8:E8','D9:E9']
for merge_range in merge_ranges:
    ws.merge_cells(merge_range)

# 保存文件
output_path = Path("output")
output_path.mkdir(exist_ok=True)
final_path = output_path / "bond.xlsx"
wb.save(final_path)
