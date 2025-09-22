'''
货币利差分析
目前只输出利差走势图: currency_spreads.png
其他数据暂不输出（已注释）
'''

import akshare as ak
import pandas as pd
import numpy as np
import os
from datetime import datetime
import logging
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import warnings
warnings.filterwarnings("ignore")


# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_currency_rate_data():
    """
    获取货币利率数据：CNH Hibor、USD Fed、HKD Hibor
    返回: dict，键为标识符，值为DataFrame
    """
    try:
        # 离岸人民币 HIBOR（香港市场）
        cnh_hibor_1m = ak.rate_interbank(
            market="香港银行同业拆借市场", symbol="Hibor人民币", indicator="1月")
        cnh_hibor_1m = cnh_hibor_1m.tail(2000)

        # 港币 HIBOR
        hkd_hibor_1m = ak.rate_interbank(
            market="香港银行同业拆借市场", symbol="Hibor港币", indicator="1月")
        hkd_hibor_1m = hkd_hibor_1m.tail(2000)

        # 美元联邦基金利率（政策利率替代 SOFR）
        usd_fed = ak.macro_bank_usa_interest_rate()
        usd_fed = usd_fed[['日期', '今值']].rename(columns={'日期': '报告日', '今值': '利率'})
        usd_fed = usd_fed.dropna(subset=["利率"])
        usd_fed = usd_fed.tail(200)

        return {
            'CNH_Hibor_1m': cnh_hibor_1m,
            'HKD_Hibor_1m': hkd_hibor_1m,
            'USD_FedFunds': usd_fed,
        }

    except Exception as e:
        logger.error(f"获取货币利率数据时出错: {str(e)}")
        return None


def save_raw_currency_data(data_dict, output_path='output/raw_data'):
    os.makedirs(output_path, exist_ok=True)
    for key, df in data_dict.items():
        file_path = os.path.join(output_path, f"{key}.xlsx")
        df.to_excel(file_path, index=False)


def calculate_currency_metrics(data_dict):
    """
    计算当前值、MoM、YoY、5年均值
    """
    results = []

    for key, df in data_dict.items():
        try:
            df['报告日'] = pd.to_datetime(df['报告日'])
            df = df.sort_values('报告日', ascending=False)
            df = df[df['利率'].notna()]
            latest = df.iloc[0]
            current_value = latest['利率']
            current_date = latest['报告日']

            # MoM
            mom_date = current_date - pd.DateOffset(days=30)
            mom_window = df[(df['报告日'] <= mom_date + pd.Timedelta(days=15)) &
                            (df['报告日'] >= mom_date - pd.Timedelta(days=15))]
            mom = np.nan
            if not mom_window.empty and mom_window.iloc[0]['利率'] != 0:
                mom = (current_value - mom_window.iloc[0]['利率']) / mom_window.iloc[0]['利率']

            # YoY
            yoy_date = current_date - pd.DateOffset(days=365)
            yoy_window = df[(df['报告日'] <= yoy_date + pd.Timedelta(days=10)) &
                            (df['报告日'] >= yoy_date - pd.Timedelta(days=10))]
            yoy = np.nan
            if not yoy_window.empty and yoy_window.iloc[0]['利率'] != 0:
                yoy = (current_value - yoy_window.iloc[0]['利率']) / yoy_window.iloc[0]['利率']

            # 5年均值
            five_years_ago = current_date - pd.DateOffset(years=5)
            long_window = df[df['报告日'] >= five_years_ago]
            five_year_avg = long_window['利率'].mean() if not long_window.empty else np.nan

            results.append({
                '名称': key,
                '当前值': current_value,
                '日期': current_date.strftime('%Y-%m-%d'),
                'MoM': mom,
                'YoY': yoy,
                '5年均值': five_year_avg
            })

        except Exception as e:
            logger.error(f"{key} 指标计算失败: {str(e)}")
            results.append({
                '名称': key,
                '当前值': np.nan,
                '日期': None,
                'MoM': np.nan,
                'YoY': np.nan,
                '5年均值': np.nan
            })

    return pd.DataFrame(results)


def plot_currency_rates(data_dict, output_path='output', years=2):
    """
    可视化近 N 年货币利率走势
    """
    plt.rcParams['font.family'] = 'SimHei'
    plt.rcParams['axes.unicode_minus'] = False

    now = pd.Timestamp.today()
    start_date = now - pd.DateOffset(years=years)

    plt.figure(figsize=(12, 6))
    for name, df in data_dict.items():
        if df.empty: continue
        df = df.copy()
        df['报告日'] = pd.to_datetime(df['报告日'])
        df = df[df['报告日'] >= start_date]
        df = df.sort_values('报告日')
        df_monthly = df.set_index('报告日').resample('M').last().dropna().reset_index()
        plt.plot(df_monthly['报告日'], df_monthly['利率'], label=name)

    plt.title(f"近{years}年货币利率走势")
    plt.xlabel("日期")
    plt.ylabel("利率（%）")
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.xticks(rotation=45)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.legend()
    plt.tight_layout()
    os.makedirs(output_path, exist_ok=True)
    plt.savefig(os.path.join(output_path, f"currency_rate_trend_{years}y.png"), dpi=300)
    plt.close()

def calculate_currency_spreads(data_dict):
    """
    计算货币之间的利差（CNH - USD、HKD - USD、CNH - HKD）
    """
    try:
        df_cnh = data_dict.get('CNH_Hibor_1m')
        df_hkd = data_dict.get('HKD_Hibor_1m')
        df_usd = data_dict.get('USD_FedFunds')

        # 格式统一
        for df in [df_cnh, df_hkd, df_usd]:
            df['报告日'] = pd.to_datetime(df['报告日'])
            df.set_index('报告日', inplace=True)

        df_all = pd.concat([
            df_cnh['利率'].rename('CNH'),
            df_hkd['利率'].rename('HKD'),
            df_usd['利率'].rename('USD')
        ], axis=1).dropna()

        df_all['CNH-USD'] = df_all['CNH'] - df_all['USD']
        df_all['HKD-USD'] = df_all['HKD'] - df_all['USD']
        df_all['CNH-HKD'] = df_all['CNH'] - df_all['HKD']

        df_all = df_all.reset_index()
        df_all = df_all.tail(365 * 2)  # 近两年
        return df_all

    except Exception as e:
        logger.error(f"计算货币利差出错: {str(e)}")
        return None

def plot_currency_spreads(spread_df, output_path='output'):
    """
    绘制货币利差时间序列图
    """
    plt.figure(figsize=(12, 6))
    for col in ['CNH-USD', 'HKD-USD', 'CNH-HKD']:
        if col in spread_df.columns:
            plt.plot(spread_df['报告日'], spread_df[col], label=col)

    plt.title("货币利差走势")
    plt.xlabel("日期")
    plt.ylabel("利差（%）")
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.legend()
    plt.tight_layout()
    os.makedirs(output_path, exist_ok=True)
    plt.savefig(os.path.join(output_path, "currency_spreads.png"), dpi=300)
    plt.close()


def main(debug=False):
    print("\n4. 利差分析")
    print("-"*30)

    rate_data = get_currency_rate_data()
    if not rate_data:
        print("未获取到数据")
        return

    save_raw_currency_data(rate_data)
    metrics = calculate_currency_metrics(rate_data)
    # print(metrics)

    os.makedirs('output', exist_ok=True)
    # metrics.to_excel("output/currency_rate_metrics.xlsx", index=False)
    
    # 画图：利率走势
    # plot_currency_rates(rate_data)
    
    # 利差分析
    spread_df = calculate_currency_spreads(rate_data)
    if spread_df is not None:
        # spread_df.to_excel("output/currency_spreads.xlsx", index=False)
        plot_currency_spreads(spread_df)

if __name__ == "__main__":
    main()
