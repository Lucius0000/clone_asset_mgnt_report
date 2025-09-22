# -*- coding: utf-8 -*-
"""
Created on Thu Jul 10 15:05:46 2025

@author: Lucius
"""

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates 


# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_gdp_data(time_range=10):
    """
    获取GDP数据
    处理年度数据，对于美国和中国数据，每个季度可能有多个修订版本, 使用时应只保留最新的版本
    
    Args:
        time_range: 获取数据的时间范围（年）
    """
    try:
        # 美国GDP数据
        us_gdp_yearly = ak.macro_usa_gdp_monthly()  # annual data, published quarterly, revisioned monthly..
        # 中国GDP数据
        cn_gdp_yearly = ak.macro_china_gdp_yearly() # annual data, published quartly, revisioned monthly..
        # 香港GDP数据
        hk_gdp_yearly = ak.macro_china_hk_gbp_ratio() # quarter data
    
        return {
            'US_yearly': us_gdp_yearly.tail(time_range),
            'CN_yearly': cn_gdp_yearly.tail(time_range),
            'HK_yearly': hk_gdp_yearly.tail(time_range)
        }
    
    except Exception as e:
        logger.error(f"获取GDP数据时出错: {str(e)}")
        return None
    
def calculate_gdp_metrics(gdp_data, debug=False):
    """
    计算GDP指标：最新值、年度变化、10年均值
    处理年度数据
    
    Args:
        gdp_data: Dictionary containing GDP data for different regions
        debug: Boolean flag to enable debug messages
    """
    if not gdp_data:
        if debug:
            print("Error: No GDP data provided")
        return None
    
    results = []
    
    # Process US data
    if 'US_yearly' in gdp_data:
        if debug:
            print("\n=== Processing US GDP data ===")
        us_yearly = gdp_data['US_yearly']
        
        if debug:
            print("Original data:")
            print(us_yearly.head())
        
        # Handle US GDP revisions - keep only latest revision for each quarter
        us_yearly['quarter'] = pd.to_datetime(us_yearly['日期']).dt.to_period('Q')
        # Sort by date in ascending order first to ensure we get the latest revision
        us_yearly = us_yearly.sort_values('日期', ascending=True)
        us_yearly = us_yearly.drop_duplicates(subset='quarter', keep='last')
        # Now sort by date in descending order for processing
        us_yearly = us_yearly.sort_values('日期', ascending=False)
        us_yearly = us_yearly.drop('quarter', axis=1)
        
        if debug:
            print("\nAfter handling revisions:")
            print(us_yearly.head())
            print("\nDate range:", us_yearly['日期'].min(), "to", us_yearly['日期'].max())
        
        # Get latest non-NaN values (most recent first)
        latest_yearly = us_yearly[us_yearly['今值'].notna()].iloc[0]
        
        if debug:
            print(f"Latest yearly: {latest_yearly['日期']} - {latest_yearly['今值']}%")
        
        # Calculate 10-year CAGR
        cagr_10y, date_range = calculate_gdp_10y_cagr(us_yearly, debug)
        
        results.append({
            'region': 'US',
            'yoy_value': latest_yearly['今值'],
            'yoy_date': latest_yearly['日期'],
            'cagr_10y': cagr_10y,
            'date_range': date_range,
            'current_value': latest_yearly['今值'],
            'current_date': pd.to_datetime(latest_yearly['日期']).to_period('Q').strftime('%YQ%q')

        })

    # Process CN data
    if 'CN_yearly' in gdp_data:
        if debug:
            print("\n=== Processing CN GDP data ===")
        cn_yearly = gdp_data['CN_yearly']
        
        if debug:
            print("Original data:")
            print(cn_yearly.head())
        
        # Handle CN GDP revisions - keep only latest revision for each quarter
        cn_yearly['quarter'] = pd.to_datetime(cn_yearly['日期']).dt.to_period('Q')
        # Sort by date in ascending order first to ensure we get the latest revision
        cn_yearly = cn_yearly.sort_values('日期', ascending=True)
        cn_yearly = cn_yearly.drop_duplicates(subset='quarter', keep='last')
        # Now sort by date in descending order for processing
        cn_yearly = cn_yearly.sort_values('日期', ascending=False)
        cn_yearly = cn_yearly.drop('quarter', axis=1)
        
        if debug:
            print("\nAfter handling revisions:")
            print(cn_yearly.head())
            print("\nDate range:", cn_yearly['日期'].min(), "to", cn_yearly['日期'].max())
        
        # Get latest non-NaN values (most recent first)
        latest_yearly = cn_yearly[cn_yearly['今值'].notna()].iloc[0]
        
        if debug:
            print(f"Latest yearly: {latest_yearly['日期']} - {latest_yearly['今值']}%")
        
        # Calculate 10-year CAGR
        cagr_10y, date_range = calculate_gdp_10y_cagr(cn_yearly, debug)
        
        results.append({
            'region': 'CN',
            'yoy_value': latest_yearly['今值'],
            'yoy_date': latest_yearly['日期'],
            'cagr_10y': cagr_10y,
            'date_range': date_range,
            'current_value': latest_yearly['今值'],
            'current_date': pd.to_datetime(latest_yearly['日期']).to_period('Q').strftime('%YQ%q')

        })
    
    # Process HK data
    if 'HK_yearly' in gdp_data:
        if debug:
            print("\n=== Processing HK GDP data ===")
        hk_yearly = gdp_data['HK_yearly']
        
        if debug:
            print("Original data:")
            print(hk_yearly.head())
        
        # Handle HK GDP data - convert quarter format to datetime
        def convert_hk_date(date_str):
            if isinstance(date_str, str) and '第' in date_str:
                year = date_str.split('第')[0]
                quarter = date_str.split('第')[1].split('季度')[0]
                # Map quarter to end month
                quarter_end_months = {'1': '03', '2': '06', '3': '09', '4': '12'}
                month = quarter_end_months.get(quarter, '12')
                # Use the last day of the month
                if month in ['04', '06', '09', '11']:
                    day = '30'
                elif month == '02':
                    # Handle February (28 or 29 depending on leap year)
                    year_int = int(year)
                    day = '29' if (year_int % 4 == 0 and year_int % 100 != 0) or (year_int % 400 == 0) else '28'
                else:
                    day = '31'
                return f"{year}-{month}-{day}"
            return date_str
        
        hk_yearly['quarter'] = hk_yearly['时间'].apply(convert_hk_date)
        hk_yearly['quarter'] = pd.to_datetime(hk_yearly['quarter'])
        
        # Sort by date in ascending order first to ensure we get the latest revision
        hk_yearly = hk_yearly.sort_values('quarter', ascending=True)
        hk_yearly = hk_yearly.drop_duplicates(subset='quarter', keep='last')
        # Now sort by date in descending order for processing
        hk_yearly = hk_yearly.sort_values('quarter', ascending=False)
        
        if debug:
            print("\nAfter handling revisions:")
            print(hk_yearly.head())
            print("\nDate range:", hk_yearly['quarter'].min(), "to", hk_yearly['quarter'].max())
        
        # Get latest non-NaN values (most recent first)
        latest_yearly = hk_yearly[hk_yearly['现值'].notna()].iloc[0]
        
        if debug:
            print(f"Latest yearly: {latest_yearly['时间']} - {latest_yearly['现值']}%")
        
        # Calculate 10-year CAGR
        cagr_10y, date_range = calculate_gdp_10y_cagr(hk_yearly, debug)
        
        results.append({
            'region': 'HK',
            'yoy_value': latest_yearly['现值'],
            'yoy_date': latest_yearly['quarter'].strftime('%Y-%m-%d'),
            'cagr_10y': cagr_10y,
            'date_range': date_range,
            'current_value': latest_yearly['现值'],
            'current_date': pd.to_datetime(latest_yearly['quarter']).to_period('Q').strftime('%YQ%q')

        })
    
    return pd.DataFrame(results)



def calculate_gdp_10y_cagr(yearly_data, debug=False):
    """
    计算10年复合增长率
    使用年度数据来计算，每年取1个数据点
    从最新的有效数据开始，取10个数据点
    使用复利计算：(1+gdp_year1) * (1+gdp_year2) * ... * (1+gdp_year10) 的10次方根
    
    Args:
        yearly_data: DataFrame containing yearly GDP data
        debug: Boolean flag to enable debug messages
    """
    try:
        if debug:
            print("\n=== Starting 10-year CAGR calculation for GDP ===")
            print(f"Input data shape: {yearly_data.shape}")
            print(f"Columns: {yearly_data.columns.tolist()}")
        
        # Get the correct date column (时间 or 日期)
        if '时间' in yearly_data.columns:
            date_col = '时间'
        else:
            date_col = '日期'
        
        if debug:
            print(f"Using date column: {date_col}")
        
        # Convert various date formats to YYYY-MM-DD
        def convert_date(date_str):
            if isinstance(date_str, str):
                if '年' in date_str and '月' in date_str:
                    # Convert "2021年07月" to "2021-07-01"
                    year = date_str.split('年')[0]
                    month = date_str.split('年')[1].split('月')[0].zfill(2)
                    return f"{year}-{month}-01"
                elif '第' in date_str and '季度' in date_str:
                    # Convert "2023第1季度" to "2023-03-31" (end of quarter)
                    year = date_str.split('第')[0]
                    quarter = date_str.split('第')[1].split('季度')[0]
                    # Map quarter to end month
                    quarter_end_months = {'1': '03', '2': '06', '3': '09', '4': '12'}
                    month = quarter_end_months.get(quarter, '12')
                    # Use the last day of the month
                    if month in ['04', '06', '09', '11']:
                        day = '30'
                    elif month == '02':
                        # Handle February (28 or 29 depending on leap year)
                        year_int = int(year)
                        day = '29' if (year_int % 4 == 0 and year_int % 100 != 0) or (year_int % 400 == 0) else '28'
                    else:
                        day = '31'
                    return f"{year}-{month}-{day}"
            return date_str
        
        yearly_data[date_col] = yearly_data[date_col].apply(convert_date)
        yearly_data[date_col] = pd.to_datetime(yearly_data[date_col])
        
        if debug:
            print("\nDate conversion example:")
            print(yearly_data[[date_col]].head())
        
        # Get value column name
        if '现值' in yearly_data.columns:
            value_col = '现值'
        else:
            value_col = '今值'
            
        if debug:
            print(f"Using value column: {value_col}")
        
        # Sort data by date in descending order (newest first)
        yearly_data = yearly_data.sort_values(date_col, ascending=False)
        
        if debug:
            print("\nFirst few records after sorting:")
            print(yearly_data.head())
        
        # Find the first valid (non-NaN) value
        valid_data = yearly_data[yearly_data[value_col].notna()]
        if len(valid_data) < 2:
            if debug:
                print("Error: Not enough valid data points found")
            return None, None
            
        # Get the position of the first valid record
        start_pos = yearly_data.index.get_loc(valid_data.index[0])
        
        if debug:
            print(f"\nStarting from position {start_pos}")
            print(f"First valid record: {yearly_data.iloc[start_pos]}")
        
        # Get 10 yearly records (10 years of data)
        yearly_records = []
        current_pos = start_pos
        
        for i in range(10):  # 10 years of data
            if current_pos >= len(yearly_data):
                if debug:
                    print(f"Reached end of data at position {current_pos}")
                break
                
            current_record = yearly_data.iloc[current_pos]
            if pd.notna(current_record[value_col]):
                yearly_records.append(current_record)
                if debug:
                    print(f"Year {i+1}: {current_record[date_col]} - {current_record[value_col]}%")
            
            # Move forward in the sorted data (which is descending, so this moves back in time)
            current_pos += 4  # Move one year at a time
        
        if debug:
            print("\nCollected records:")
            for i, record in enumerate(yearly_records):
                print(f"Year {i+1}: {record[date_col]} - {record[value_col]}%")
        
        if len(yearly_records) < 2:
            if debug:
                print("Error: Not enough yearly records collected")
            return None, None
        
        # Calculate compound growth rate
        # Convert percentage to decimal and add 1 for compounding
        growth_factors = [1 + (record[value_col] / 100) for record in yearly_records]
        compound_growth = np.prod(growth_factors)
        
        # Calculate the nth root (where n is the number of years)
        n = len(yearly_records)
        cagr = (compound_growth ** (1/n)) - 1
        
        # Get date range
        date_range = f"{yearly_records[-1][date_col].strftime('%Y-%m')} to {yearly_records[0][date_col].strftime('%Y-%m')}"
        
        if debug:
            print(f"\nCalculation details:")
            print(f"Number of years: {n}")
            print(f"Growth factors: {growth_factors}")
            print(f"Compound growth: {compound_growth}")
            print(f"Final CAGR: {cagr * 100:.2f}%")
            print(f"Date range: {date_range}")
        
        return cagr * 100, date_range  # Convert back to percentage
        
    except Exception as e:
        if debug:
            print(f"Error in CAGR calculation: {str(e)}")
        logger.error(f"计算10年复合增长率时出错: {str(e)}")
        return None, None

def convert_hk_date(date_str):
    if isinstance(date_str, str) and '第' in date_str:
        year = date_str.split('第')[0]
        quarter = date_str.split('第')[1].split('季度')[0]
        quarter_end_months = {'1': '03', '2': '06', '3': '09', '4': '12'}
        month = quarter_end_months.get(quarter, '12')
        if month in ['04', '06', '09', '11']:
            day = '30'
        elif month == '02':
            year_int = int(year)
            day = '29' if (year_int % 4 == 0 and year_int % 100 != 0) or (year_int % 400 == 0) else '28'
        else:
            day = '31'
        return f"{year}-{month}-{day}"
    return date_str

def plot_gdp_trend(gdp_data, gdp_metrics, output_path='output/gdp_trend_2y.png', debug=False):
    """
    绘制近两年中美港GDP同比增速走势图，并添加各国10年CAGR基准线
    """

    plt.rcParams['font.family'] = 'SimHei'
    plt.rcParams['axes.unicode_minus'] = False

    fig, ax = plt.subplots(figsize=(14, 6))
    cutoff_date = pd.Timestamp.today() - pd.DateOffset(years=2)

    def plot_series(df, date_col, value_col, label, color):
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        df = df[df[date_col] >= cutoff_date]
        df = df.drop_duplicates(subset=date_col, keep='last')
        df = df.sort_values(date_col)
        df = df[df[value_col].notna()]
        ax.plot(df[date_col], df[value_col], label=label, linewidth=2, color=color)

    def add_cagr_line(cagr_value, label, color):
        ax.axhline(y=cagr_value, linestyle='--', color=color, alpha=0.5, linewidth=1.5, label=f"{label} 10年CAGR ({cagr_value:.1f}%)")

    # 中国
    if 'CN_yearly' in gdp_data:
        df = gdp_data['CN_yearly']
        df['quarter'] = pd.to_datetime(df['日期']).dt.to_period('Q').dt.to_timestamp('Q')
        plot_series(df, 'quarter', '今值', '中国GDP同比增速', '#2ca02c')
        cagr = gdp_metrics[gdp_metrics['region'] == 'CN']['cagr_10y'].values
        if len(cagr) > 0:
            add_cagr_line(cagr[0], '中国', '#2ca02c')

    # 美国
    if 'US_yearly' in gdp_data:
        df = gdp_data['US_yearly']
        df['quarter'] = pd.to_datetime(df['日期']).dt.to_period('Q').dt.to_timestamp('Q')
        plot_series(df, 'quarter', '今值', '美国GDP同比增速', '#1f77b4')
        cagr = gdp_metrics[gdp_metrics['region'] == 'US']['cagr_10y'].values
        if len(cagr) > 0:
            add_cagr_line(cagr[0], '美国', '#1f77b4')

    # 香港
    if 'HK_yearly' in gdp_data:
        def convert_hk_date(date_str):
            if isinstance(date_str, str) and '第' in date_str:
                year = date_str.split('第')[0]
                quarter = date_str.split('第')[1].split('季度')[0]
                end_month = {'1': '03', '2': '06', '3': '09', '4': '12'}.get(quarter, '12')
                return f"{year}-{end_month}-30"
            return date_str
        df = gdp_data['HK_yearly'].copy()
        df['quarter'] = pd.to_datetime(df['时间'].apply(convert_hk_date))
        plot_series(df, 'quarter', '现值', '香港GDP同比增速', '#d62728')
        cagr = gdp_metrics[gdp_metrics['region'] == 'HK']['cagr_10y'].values
        if len(cagr) > 0:
            add_cagr_line(cagr[0], '香港', '#d62728')

    ax.set_title('近两年主要GDP同比增速走势（季度）', fontsize=16)
    ax.set_xlabel('季度')
    ax.set_ylabel('同比增速（%）')
    ax.legend(loc='best', fontsize=10)
    ax.grid(True, linestyle='--', alpha=0.5)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    fig.autofmt_xdate()

    plt.tight_layout()
    plt.savefig(output_path, dpi=300)
    plt.close()

    if debug:
        print(f"近两年GDP趋势图保存至: {output_path}")



def generate_report(debug=False):
    """
    生成宏观经济指标报告
    
    Args:
        debug: Boolean flag to enable debug messages
    """
    print("="*50)
    print("宏观经济指标统计")
    print("="*50)
    print(f"报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    time_range = 200 # getting 200 data points

    # Task 2: GDP分析
    print("\n2. GDP分析")
    print("-"*30)
    gdp_data = get_gdp_data(time_range)
    if (debug):
        print(gdp_data)
    gdp_metrics = calculate_gdp_metrics(gdp_data, debug)
    print(gdp_metrics)
    

    output_path = 'output'
    os.makedirs(output_path, exist_ok=True)
    
    # 保存 GDP 原始数据
    raw_data_path = os.path.join(output_path, 'raw_data')
    os.makedirs(raw_data_path, exist_ok=True)
    for name, df in gdp_data.items():
        df.to_excel(os.path.join(raw_data_path, f"gdp_{name}.xlsx"), index=False)

    # 格式化 GDP 输出
    def format_date(dt):
        if pd.isna(dt):
            return "-"
        return pd.to_datetime(dt).strftime("%Y-%m")

    def format_row(row):
        region_map = {'US': '美国', 'CN': '中国', 'HK': '香港'}
        return pd.Series({
            '区域': region_map.get(row['region'], row['region']),
            '指标': 'GDP',
            '当前值': row.get('current_value', '-'),
            '当前值日期': row.get('current_date', '-'),
            'YoY(%)': round(row['yoy_value'], 2) if pd.notna(row['yoy_value']) else '-',
            'YoY日期': format_date(row['yoy_date']),
            '年化增长10年均值（%）': round(row['cagr_10y'], 2) if pd.notna(row['cagr_10y']) else '-',
            '年化增长10年均值日期': row['date_range'] if pd.notna(row['date_range']) else '-'
        })

    formatted_df = gdp_metrics.apply(format_row, axis=1)
    formatted_df.to_excel(f"{output_path}/gdp_metrics.xlsx", index=False)

    
    plot_gdp_trend(gdp_data, gdp_metrics, output_path='output/gdp_trend.png', debug=False)


if __name__ == "__main__":
    try:
        generate_report(debug=False)
    except Exception as e:
        logger.error(f"生成报告时出错: {str(e)}")
        
        