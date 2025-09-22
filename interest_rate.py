'''
银行利率分析
输出银行利率表：interest_rate_metrics.xlsx
输出近两年银行利率走势图：interest_rate_trend_2y.png
'''

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.font_manager as fm

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_interest_rate_data():
    """
    获取美国、中国、香港的利率数据（自适应拉取行数）
    返回: 包含各国利率数据的字典
    """
    try:
        # 美国利率数据（Fed利率，约42天一次）
        us_rate_fed = ak.macro_bank_usa_interest_rate()
        us_rate_fed = us_rate_fed.tail(50)

        # 使用LPR1Y(一年期贷款市场报价利率)替代原来的中国基准利率，但是变量名依旧为cn_rate_fed
        cn_lpr_all = ak.macro_china_lpr()
        cn_rate_fed = cn_lpr_all[['TRADE_DATE', 'LPR1Y']].rename(
            columns={'TRADE_DATE': '日期', 'LPR1Y': '今值'}
        ).dropna()
        cn_rate_fed = cn_rate_fed.sort_values('日期').tail(80)

        # 中国Chibor利率（天频）
        cn_rate_interbank_1d = ak.rate_interbank(
            market="中国银行同业拆借市场", symbol="Chibor人民币", indicator="隔夜")
        cn_rate_interbank_1d = cn_rate_interbank_1d.tail(2000)

        cn_rate_interbank_1m = ak.rate_interbank(
            market="中国银行同业拆借市场", symbol="Chibor人民币", indicator="1月")
        cn_rate_interbank_1m = cn_rate_interbank_1m.tail(2000)

        # 香港Hibor利率（天频）
        hk_rate_interbank_1d = ak.rate_interbank(
            market="香港银行同业拆借市场", symbol="Hibor港币", indicator="隔夜")
        hk_rate_interbank_1d = hk_rate_interbank_1d.tail(2000)

        hk_rate_interbank_1m = ak.rate_interbank(
            market="香港银行同业拆借市场", symbol="Hibor港币", indicator="1月")
        hk_rate_interbank_1m = hk_rate_interbank_1m.tail(2000)

        hk_rate_interbank_1m_cny = ak.rate_interbank(
            market="香港银行同业拆借市场", symbol="Hibor人民币", indicator="1月")
        hk_rate_interbank_1m_cny = hk_rate_interbank_1m_cny.tail(2000)

        return {
            'US_fed': us_rate_fed,
            'CN_lpr_1m': cn_rate_fed,
            'CN_interbank_1d': cn_rate_interbank_1d,
            'CN_interbank_1m': cn_rate_interbank_1m,
            'HK_interbank_1d': hk_rate_interbank_1d,
            'HK_interbank_1m': hk_rate_interbank_1m,
            'HK_interbank_1m_cny': hk_rate_interbank_1m_cny,
        }

    except Exception as e:
        logger.error(f"获取利率数据时出错: {str(e)}")
        return None

    
def save_raw_interest_data(rate_data, output_path='output/raw_data'):
    """
    将原始利率数据分别保存为 Excel 文件
    Args:
        rate_data: 字典形式的原始利率数据
        output_path: 保存路径（默认是 output/raw_data 文件夹）
    """
    os.makedirs(output_path, exist_ok=True)
    for key, df in rate_data.items():
        file_path = os.path.join(output_path, f"interest_rate_{key}.xlsx")
        df.to_excel(file_path, index=False)
 

def calculate_interest_rate_metrics(rate_data, debug=False):
    """
    计算利率指标：当前值、日期、MoM、YoY、5年均值
    处理央行利率和同业拆借利率
    
    Args:
        rate_data: Dictionary containing interest rate data for different regions
        debug: Boolean flag to enable debug messages
    """
    if not rate_data:
        if debug:
            print("Error: No interest rate data provided")
        return None
    
    results = []
    
    for metric, data in rate_data.items():
        try:
            if debug:
                print(f"\n{'='*50}")
                print(f"Processing {metric} interest rate data")
                print(f"{'='*50}")
                print(f"Data shape: {data.shape}")
                print(f"Columns: {data.columns.tolist()}")
                print("\nFirst few records:")
                print(data.head())
            
            # Get value column name based on data type
            if '今值' in data.columns:  # Fed rates
                value_col = '今值'
                date_col = '日期'
                is_fed = True
            else:  # Interbank rates
                value_col = '利率'
                date_col = '报告日'
                is_fed = False
            
            if debug:
                print(f"\nUsing value column: {value_col}")
                print(f"Using date column: {date_col}")
            
            # Convert date column to datetime
            data[date_col] = pd.to_datetime(data[date_col])
            
            # Sort by date in descending order (newest first)
            data = data.sort_values(date_col, ascending=False)
            
            if debug:
                print("\nFirst few records after sorting:")
                print(data.head())
            
            # Get latest non-NaN value
            valid_data = data[data[value_col].notna()]
            if valid_data.empty:
                if debug:
                    print("\nNo valid data points found in the dataset")
                results.append({
                    'region': metric,
                    'current_value': np.nan,
                    'current_date': None,
                    'mom': np.nan,
                    'yoy': np.nan,
                    'five_year_avg': np.nan
                })
                continue
                
            latest_data = valid_data.iloc[0]
            current_value = latest_data[value_col]
            current_date = latest_data[date_col]
            
            if debug:
                print(f"\nLatest value: {current_value}% on {current_date}")
            
            # Initialize result dictionary with current values
            result = {
                'region': metric,
                'current_value': current_value,
                'current_date': current_date.strftime('%Y-%m-%d') if pd.notnull(current_date) else None,
                'mom': np.nan,
                'yoy': np.nan,
                'five_year_avg': np.nan
            }
            
            # Calculate MoM (Month over Month)
            if is_fed and metric == 'US_fed':
                # 特殊逻辑：美联储利率为非固定频率，找最近一次变动
                prev_diff = valid_data[valid_data[value_col] != current_value]
                if not prev_diff.empty and prev_diff.iloc[0][value_col] != 0:
                    result['mom'] = (current_value - prev_diff.iloc[0][value_col]) / prev_diff.iloc[0][value_col]
                    if debug:
                        print(f"\nUS Fed MoM calculation details:")
                        print(f"Current: {current_date} ({current_value}%)")
                        print(f"Previous different: {prev_diff.iloc[0][date_col]} ({prev_diff.iloc[0][value_col]}%)")
                        print(f"MoM change: {result['mom']:.2%}")
                else:
                    if debug:
                        print("\nInsufficient Fed MoM data (no previous different value)")

            elif metric == 'CN_lpr_1m':
                # LPR 是月度利率，发布时间大约在每月20日，需要 ±10 天容错窗口
                target_date = current_date - pd.DateOffset(days=30)
                window_start = target_date - pd.DateOffset(days=10)  # 40天前
                window_end = target_date + pd.DateOffset(days=10)    # 20天前
                mom_window = data[(data[date_col] >= window_start) &
                                  (data[date_col] <= window_end) &
                                  (data[value_col].notna())]
                mom_data = mom_window.iloc[0:1] if not mom_window.empty else pd.DataFrame()
            
                if not mom_data.empty and mom_data.iloc[0][value_col] != 0:
                    result['mom'] = (current_value - mom_data.iloc[0][value_col]) / mom_data.iloc[0][value_col]
                    if debug:
                        print(f"\nLPR MoM calculation details:")
                        print(f"Current: {current_date} ({current_value}%)")
                        print(f"Target date for comparison: {target_date}")
                        print(f"Search window: {window_start.date()} to {window_end.date()}")
                        print("\nAvailable data points around target date:")
                        print(mom_window[['日期', value_col]].to_string())
                        print(f"\nSelected previous point: {mom_data.iloc[0][date_col]} ({mom_data.iloc[0][value_col]}%)")
                        print(f"Time difference: {current_date - mom_data.iloc[0][date_col]}")
                        print(f"MoM change: {result['mom']:.2%}")
                else:
                    if debug:
                        print("\nInsufficient LPR data for MoM calculation")
                        print(f"Target date: {target_date}")
                        print(f"Search window: {window_start.date()} to {window_end.date()}")
                        print("Available data points in window:")
                        print(mom_window[['日期', value_col]].to_string())

                        
            else:
                # 通用逻辑
                target_date = current_date - pd.DateOffset(days=30)
                window_start = target_date - pd.DateOffset(days=10)
                window_end = target_date 
                mom_window = data[(data[date_col] >= window_start) & 
                                  (data[date_col] <= window_end) & 
                                  (data[value_col].notna())]
                mom_data = mom_window.iloc[0:1] if not mom_window.empty else pd.DataFrame()
            
                if not mom_data.empty and mom_data.iloc[0][value_col] != 0:
                    result['mom'] = (current_value - mom_data.iloc[0][value_col])/mom_data.iloc[0][value_col]
                    if debug:
                        print(f"\nMoM calculation details:")
                        print(f"Current: {current_date} ({current_value}%)")
                        print(f"Target date for comparison: {target_date}")
                        print("\nAvailable data points around target date:")
                        print(mom_window[['日期' if is_fed else '报告日', value_col]].to_string())
                        print(f"\nSelected previous point: {mom_data.iloc[0][date_col]} ({mom_data.iloc[0][value_col]}%)")
                        print(f"Time difference: {current_date - mom_data.iloc[0][date_col]}")
                        print(f"MoM change: {result['mom']:.2%}")
                else:
                    if debug:
                        print("\nInsufficient data for MoM calculation")
                        print(f"Target date: {target_date}")
                        print("Available data points in window:")
                        print(mom_window[['日期' if is_fed else '报告日', value_col]].to_string())

            
            # Calculate YoY (Year over Year)
            if is_fed:
                # For Fed rates, find data from 4 quarters ago
                target_date = current_date - pd.DateOffset(days=350)
                # Get all data points within 1 month of target date
                window_start = target_date - pd.DateOffset(months=1)
                window_end = target_date 
                yoy_window = data[(data[date_col] >= window_start) & 
                                (data[date_col] <= window_end) & 
                                (data[value_col].notna())]
                yoy_data = yoy_window.iloc[0:1] if not yoy_window.empty else pd.DataFrame()
            else:
                # For Interbank rates, find data from approximately 365 days ago
                target_date = current_date - pd.DateOffset(days=365)
                # Get all data points within 10 days of target date
                window_start = target_date - pd.DateOffset(days=10)
                window_end = target_date 
                yoy_window = data[(data[date_col] >= window_start) & 
                                (data[date_col] <= window_end) & 
                                (data[value_col].notna())]
                yoy_data = yoy_window.iloc[0:1] if not yoy_window.empty else pd.DataFrame()
            
            if not yoy_data.empty and yoy_data.iloc[0][value_col] != 0:  # Avoid division by zero
                result['yoy'] = (current_value - yoy_data.iloc[0][value_col])/yoy_data.iloc[0][value_col]
                if debug:
                    print(f"\nYoY calculation details:")
                    print(f"Current: {current_date} ({current_value}%)")
                    print(f"Target date for comparison: {target_date}")
                    print("\nAvailable data points around target date:")
                    print(yoy_window[['日期' if is_fed else '报告日', value_col]].to_string())
                    print(f"\nSelected previous year point: {yoy_data.iloc[0][date_col]} ({yoy_data.iloc[0][value_col]}%)")
                    print(f"Time difference: {current_date - yoy_data.iloc[0][date_col]}")
                    print(f"YoY change: {result['yoy']:.2%}")
            else:
                if debug:
                    print("\nInsufficient data for YoY calculation")
                    print(f"Target date: {target_date}")
                    print("Available data points in window:")
                    print(yoy_window[['日期' if is_fed else '报告日', value_col]].to_string())
            
            # Calculate 5-year average
            five_years_ago = current_date - pd.DateOffset(years=5)
            five_year_data = data[(data[date_col] >= five_years_ago) & (data[value_col].notna())]
            
            if is_fed:
                # For Fed rates, ensure dates are valid before resampling
                five_year_data = five_year_data[five_year_data[date_col].notna()].copy()
                if not five_year_data.empty:
                    # Convert to datetime if not already
                    five_year_data[date_col] = pd.to_datetime(five_year_data[date_col])
                    # Resample to quarterly data
                    five_year_data = five_year_data.set_index(date_col).resample('QE').last().reset_index()
            else:
                # For Interbank rates, resample to monthly data
                five_year_data = five_year_data.set_index(date_col).resample('ME').last().reset_index()
                
            # 新增时间跨度判断：必须覆盖至少 4.5 年，且有足够数据点
            if not five_year_data.empty and len(five_year_data) >= 8:
                time_span_days = (five_year_data[date_col].max() - five_year_data[date_col].min()).days
                if time_span_days >= 365 * 4.5:
                    result['five_year_avg'] = five_year_data[value_col].mean()
                    min_date = five_year_data[date_col].min()
                    max_date = five_year_data[date_col].max()
                    result['date_range'] = f"{min_date.strftime('%Y-%m')} to {max_date.strftime('%Y-%m')}"
                    if debug:
                        print(f"\n5-year average calculation details:")
                        print(f"Date range: {result['date_range']}")
                        print(f"Number of data points: {len(five_year_data)}")
                        print(f"Time span: {time_span_days} days (~{time_span_days/365:.1f} years)")
                        print(f"5-year average: {result['five_year_avg']}")
                else:
                    if debug:
                        print("\nInsufficient time coverage for 5-year average calculation")
                        print(f"Time span: {time_span_days} days (< 4.5 years)")
                    result['five_year_avg'] = np.nan
                    result['date_range'] = None
            else:
                if debug:
                    print("\nInsufficient data points for 5-year average calculation")
                    print(f"Found: {len(five_year_data)} data points")
                result['five_year_avg'] = np.nan
                result['date_range'] = None
            
            results.append(result)
            
        except Exception as e:
            if debug:
                print(f"Error processing {metric} interest rate data: {str(e)}")
            logger.error(f"计算{metric}利率指标时出错: {str(e)}")
            results.append({
                'region': metric,
                'current_value': np.nan,
                'current_date': None,
                'mom': np.nan,
                'yoy': np.nan,
                'five_year_avg': np.nan
            })
    
    return pd.DataFrame(results)

def map_interest_format(row):
    """
    将原始利率指标行格式化为标准输出格式行
    """
    mapping = {
        'US_fed': ('美国', 'USD', 'US Federal Fund Rate'),
        'CN_lpr_1m': ('中国', 'CNY', '中国央行LPR 1年'),
        'CN_interbank_1d': (None, 'CNY', 'Chibor 隔夜'),
        'CN_interbank_1m': (None, 'CNY', 'Chibor 1月'),
        'HK_interbank_1d': ('香港', 'HKD', 'HIBOR 隔夜'),
        'HK_interbank_1m': (None, 'HKD', 'HIBOR 1月'),
        'HK_interbank_1m_cny': (None, 'CNY', 'HIBOR人民币 1月'),
    }

    region, currency, label = mapping.get(row['region'], (None, '未知', row['region']))

    return pd.Series({
        '区域': region,
        '货币': currency,
        '利率久期': label,
        '当前值': round(row['current_value'], 2) if pd.notna(row['current_value']) else '-',
        '当前值日期': pd.to_datetime(row['current_date']).strftime('%Y-%m-%d') if pd.notna(row['current_date']) else '-',
        'MoM(%)': round(row['mom'] * 100, 1) if pd.notna(row['mom']) else '-',
        'YoY(%)': round(row['yoy'] * 100, 1) if pd.notna(row['yoy']) else '-',
        '5年均值': round(row['five_year_avg'], 2) if pd.notna(row['five_year_avg']) else '-',
        '5年均值日期': row['date_range'] if pd.notna(row.get('date_range')) else '-'
    })



def plot_interest_rate_trend(rate_data, output_path='output', years=2):
    """
    可视化近N年主要银行利率走势（月度）
    
    参数:
        rate_data: get_interest_rate_data() 返回的字典
        output_path: 图像保存路径
        years: 展示的年限（默认2年）
    """
    # 设置中文支持（宋体）和美观样式
    plt.rcParams['font.family'] = 'SimHei'
    plt.rcParams['axes.unicode_minus'] = False
    plt.style.use('seaborn-v0_8-muted')

    selected_metrics = {
        'US_fed': {'date_col': '日期', 'value_col': '今值', 'label': '美国联邦基金利率', 'step': True},
        'CN_lpr_1m': {'date_col': '日期', 'value_col': '今值', 'label': '中国LPR(1年)', 'step': False},
        'CN_interbank_1m': {'date_col': '报告日', 'value_col': '利率', 'label': '中国同业拆借(1月)', 'step': False},
        'HK_interbank_1m': {'date_col': '报告日', 'value_col': '利率', 'label': '香港Hibor(1月)', 'step': False},
        'HK_interbank_1m_cny': {'date_col': '报告日', 'value_col': '利率', 'label': '香港Hibor人民币(1月)', 'step': False},
    }

    plt.figure(figsize=(12, 6))
    now = pd.Timestamp.today()
    start_date = now - pd.DateOffset(years=years)

    for key, meta in selected_metrics.items():
        df = rate_data.get(key)
        if df is None or df.empty:
            continue
        df[meta['date_col']] = pd.to_datetime(df[meta['date_col']])
        df = df.sort_values(meta['date_col'])
        df = df[df[meta['date_col']] >= start_date]
        df_monthly = df.set_index(meta['date_col']).resample('M').last()
        if key == 'US_fed':
            df_monthly = df_monthly.ffill()
        df_monthly = df_monthly.reset_index()


        if meta.get('step'):
            plt.step(df_monthly[meta['date_col']], df_monthly[meta['value_col']], where='post', label=meta['label'])
        else:
            plt.plot(df_monthly[meta['date_col']], df_monthly[meta['value_col']], label=meta['label'])

    plt.title(f"近{years}年主要利率走势", fontsize=16)
    plt.xlabel("日期", fontsize=12)
    plt.ylabel("利率（%）", fontsize=12)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.xticks(rotation=45)
    plt.grid(True, linestyle='--', alpha=0.4)
    plt.legend(fontsize=10)
    plt.tight_layout()

    os.makedirs(output_path, exist_ok=True)
    plt.savefig(os.path.join(output_path, f'interest_rate_trend_{years}y.png'), dpi=300)
    plt.close()


def generate_report(debug=False):
    """
    生成宏观经济指标报告
    
    Args:
        debug: Boolean flag to enable debug messages
    """
    # Task 3: 利率分析
    print("\n3. 利率分析")
    print("-"*30)
    rate_data = get_interest_rate_data()
    save_raw_interest_data(rate_data)
    if (debug):
        print(rate_data)
    rate_metrics = calculate_interest_rate_metrics(rate_data, debug)
    print(rate_metrics)
    
    output_path = 'output'
    os.makedirs(output_path, exist_ok=True)
    
    formatted_rate_df = rate_metrics.apply(map_interest_format, axis=1)
    formatted_rate_df.to_excel(f"{output_path}/interest_rate_metrics.xlsx", index=False)
    
    plot_interest_rate_trend(rate_data)

def main(debug=False):
    try:
        generate_report(debug=debug)
    except Exception as e:
        logger.error(f"生成报告时出错: {str(e)}")

if __name__ == "__main__":
    main()
