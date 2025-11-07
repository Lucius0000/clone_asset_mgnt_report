'''
货币汇率分析
输出汇率表：fx_metrics.xlsx
'''

import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import akshare as ak

# 配置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 汇率代号映射
SYMBOLS = {
    "USD_CNH": "USDCNH",
    "USD_HKD": "USDHKD",
}

# 保存路径
RAW_DATA_DIR = "output/raw_data"
os.makedirs(RAW_DATA_DIR, exist_ok=True)

def get_fx_data(symbol_code):
    try:
        df = ak.forex_hist_em(symbol=symbol_code)
        df['日期'] = pd.to_datetime(df['日期'])
        df = df[['日期', '最新价']].rename(columns={'最新价': '汇率'}).dropna()
        return df
    except Exception as e:
        logging.error(f"拉取 {symbol_code} 历史数据失败: {e}")
        return pd.DataFrame(columns=['日期', '汇率'])

def compute_cross_rate(base_df, quote_df):
    if base_df.empty or quote_df.empty:
        return pd.DataFrame(columns=['日期', '汇率'])
    df = pd.merge(base_df, quote_df, on='日期', suffixes=('_base', '_quote'))
    df['汇率'] = df['汇率_quote'] / df['汇率_base']
    return df[['日期', '汇率']]

def save_raw_data(data_dict):
    for name, df in data_dict.items():
        df.to_excel(f"{RAW_DATA_DIR}/{name}.xlsx", index=False)

def calculate_metrics(data_dict):
    results = []
    for name, df in data_dict.items():
        try:
            df = df.sort_values('日期', ascending=False).dropna()
            now = df.iloc[0]['日期']
            now_val = df.iloc[0]['汇率']

            # MoM
            one_month = now - pd.DateOffset(months=1)
            mom_df = df[(df['日期'] >= one_month - pd.Timedelta(days=10)) & (df['日期'] <= one_month + pd.Timedelta(days=10))].copy()
            if not mom_df.empty:
                mom_df['差值'] = (mom_df['日期'] - one_month).abs()
                closest_mom_row = mom_df.loc[mom_df['差值'].idxmin()]
                mom = (now_val - closest_mom_row['汇率']) / closest_mom_row['汇率'] * 100
                mom_date = closest_mom_row['日期'].strftime('%Y-%m-%d')
            else:
                mom = np.nan
                mom_date = None

            # YoY
            one_year = now - pd.DateOffset(years=1)
            yoy_df = df[(df['日期'] >= one_year - pd.Timedelta(days=10)) & (df['日期'] <= one_year + pd.Timedelta(days=10))].copy()
            if not yoy_df.empty:
                yoy_df['差值'] = (yoy_df['日期'] - one_year).abs()
                closest_yoy_row = yoy_df.loc[yoy_df['差值'].idxmin()]
                yoy = (now_val - closest_yoy_row['汇率']) / closest_yoy_row['汇率'] * 100
                yoy_date = closest_yoy_row['日期'].strftime('%Y-%m-%d')
            else:
                yoy = np.nan
                yoy_date = None

            # 5年均值和时间范围
            five_years_ago = now - pd.DateOffset(years=5)
            five_year_df = df[df['日期'] >= five_years_ago]
            avg5 = five_year_df['汇率'].mean()
            if not five_year_df.empty:
                start_date = five_year_df['日期'].min().strftime('%Y-%m')
                end_date = five_year_df['日期'].max().strftime('%Y-%m')
                avg5_range = f"{start_date} to {end_date}"
            else:
                avg5_range = None

            results.append({
                '货币汇率': name,
                '汇率值': now_val,
                '日期': now.strftime('%Y-%m-%d'),
                'MoM(%)': mom,
                'MoM参考日期': mom_date,
                'YoY(%)': yoy,
                'YoY参考日期': yoy_date,
                '5年均值': avg5,
                '5年均值周期': avg5_range
            })
        except Exception as e:
            logging.error(f"{name} 指标计算失败: {e}")
            results.append({
                '货币汇率': name,
                '汇率值': np.nan,
                '日期': None,
                'MoM(%)': np.nan,
                'MoM参考日期': None,
                'YoY(%)': np.nan,
                'YoY参考日期': None,
                '5年均值': np.nan,
                '5年均值周期': None
            })
    return pd.DataFrame(results)



def plot_trend(data_dict, years=2):
    plt.figure(figsize=(10, 6))
    now = pd.Timestamp.today()
    for name, df in data_dict.items():
        df = df[df['日期'] >= now - pd.DateOffset(years=years)].sort_values('日期')
        dfm = df.set_index('日期').resample('ME').last().dropna().reset_index()
        if not dfm.empty:
            plt.plot(dfm['日期'], dfm['汇率'], label=name)
    plt.title(f"近{years}年汇率走势")
    plt.xlabel("日期"); plt.ylabel("汇率")
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.xticks(rotation=45); plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.legend(); plt.tight_layout()
    plt.savefig("output/fx_trend_2y.png", dpi=300)
    plt.close()

def main(debug = False):
    print("=" * 40)
    print("货币汇率分析 USD/CNH、USD/HKD、CNH/HKD")
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 40)

    # 获取汇率数据
    fx_data = {}
    for name, code in SYMBOLS.items():
        fx_data[name] = get_fx_data(code)
    fx_data['CNH_HKD'] = compute_cross_rate(fx_data['USD_CNH'], fx_data['USD_HKD'])

    # 保存数据
    save_raw_data(fx_data)

    # 计算指标
    metrics_df = calculate_metrics(fx_data)
    # 删去 MoM参考日期 和 YoY参考日期 列（兼容不同列名编码情况）
    try:
        drop_cols = [
            c for c in metrics_df.columns
            if ("MoM" in str(c) and "参考" in str(c)) or ("YoY" in str(c) and "参考" in str(c))
        ]
        if drop_cols:
            metrics_df = metrics_df.drop(columns=drop_cols, errors='ignore')
    except Exception:
        pass
    metrics_df.to_excel("output/fx_metrics.xlsx", index=False)
    print(metrics_df)

    # 可视化
    # plot_trend(fx_data)


if __name__ == "__main__":
    main()
