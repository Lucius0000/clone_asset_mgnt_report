'''
CPI分析，输出CPI表：cpi_metrics.xlsx
输出各国 CPI YOY 走势图：cpi_trends.png
另有两幅图表，暂不输出
'''

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import glob

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_hk_composite_cpi(file_path):
    """
    读取香港综合消费物价指数（按年变动百分率）数据，清洗 φ 和 [φ3] 等字符
    :param file_path: str, Excel 路径
    :return: pd.DataFrame(columns=['日期', 'YoY', 'MoM'])
    """
    df = pd.read_excel(file_path, sheet_name=0, header=None, skiprows=5)

    df = df.rename(columns={
        0: "年",
        1: "月",
        2: "指数",
        3: "YoY",
        4: "MoM"
    })

    # 补全年份空值
    df["年"] = df["年"].fillna(method="ffill")

    # 只保留同时有“月”和“YoY”的行（排除年平均或空行）
    df = df[df["月"].notna() & df["YoY"].notna()]

    # 将“月”列转为两位数字符串
    df["月"] = df["月"].astype(int).astype(str).str.zfill(2)
    df["年"] = df["年"].astype(int).astype(str)

    # 构造“日期”
    df["日期"] = pd.to_datetime(df["年"] + df["月"], format="%Y%m")

    # 清洗 YoY 和 MoM 中的“φ”和“[φ3]”等特殊字符，保留数值
    df["YoY"] = df["YoY"].astype(str).str.replace(r"\[.*?\]", "", regex=True).str.replace("φ", "")
    df["MoM"] = df["MoM"].astype(str).str.replace(r"\[.*?\]", "", regex=True).str.replace("φ", "")

    df["YoY"] = pd.to_numeric(df["YoY"], errors="coerce")
    df["MoM"] = pd.to_numeric(df["MoM"], errors="coerce")

    return df[["日期", "YoY", "MoM"]].dropna()



def get_cpi_data(time_range=10):
    """
    获取美国、中国、香港的CPI数据
    返回: 包含各国CPI数据的字典
    """
    try:
        # 美国CPI数据
        us_cpi_monthly = ak.macro_usa_cpi_monthly()
        us_cpi_yearly = ak.macro_usa_cpi_yoy()
        # 美国PCE数据
        us_pce_yearly = ak.macro_usa_core_pce_price()
        # 中国CPI数据
        cn_cpi_monthly = ak.macro_china_cpi_monthly()
        cn_cpi_yearly = ak.macro_china_cpi_yearly()
        # 香港CPI数据
        # hk_cpi_monthly = none # not available
        files = glob.glob(os.path.join("data", "Table 510*.xlsx"))
        if not files:
            raise FileNotFoundError("未找到匹配的 Table 510*.xlsx 文件")
        latest_file = max(files, key=os.path.getmtime)
        hk_cpi_yearly = load_hk_composite_cpi(latest_file)

        return {
            'US_monthly': us_cpi_monthly.tail(time_range),
            'US_yearly': us_cpi_yearly.tail(time_range),
            'US_pce_yearly': us_pce_yearly.tail(time_range),
            'CN_monthly': cn_cpi_monthly.tail(time_range),
            'CN_yearly': cn_cpi_yearly.tail(time_range),
            'HK_yearly': hk_cpi_yearly.tail(time_range)
        }   
    
    except Exception as e:
        logger.error(f"获取CPI数据时出错: {str(e)}")
        return None
    
def calculate_cpi_metrics(cpi_data, debug=False):
    """
    计算CPI指标：MoM, YoY, 10年复合增长率
    处理月度数据和年度数据
    
    Args:
        cpi_data: Dictionary containing CPI data for different regions
        debug: Boolean flag to enable debug messages
    """
    if not cpi_data:
        if debug:
            print("Error: No CPI data provided")
        return None
    
    results = []
    
    # Process US data
    if 'US_monthly' in cpi_data and 'US_yearly' in cpi_data:
        if debug:
            print("\n=== Processing US CPI data ===")
        us_monthly = cpi_data['US_monthly']
        us_yearly = cpi_data['US_yearly']
        
        # Get latest non-NaN values (most recent first)
        latest_monthly = us_monthly[us_monthly['今值'].notna()].iloc[-1]
        latest_yearly = us_yearly[us_yearly['现值'].notna()].iloc[-1]
        
        if debug:
            print(f"Latest monthly: {latest_monthly['日期']} - {latest_monthly['今值']}%")
            print(f"Latest yearly: {latest_yearly['时间']} - {latest_yearly['现值']}%")
        
        # Calculate 10-year CAGR
        cagr_10y, date_range = calculate_cpi_10y_cagr(us_yearly, debug)
        
        results.append({
            'region': 'US',
            'mom_value': latest_monthly['今值'],
            'mom_date': latest_monthly['日期'],
            'yoy_value': latest_yearly['现值'],
            'yoy_date': latest_yearly['时间'],
            'cagr_10y': cagr_10y,
            'date_range': date_range
        })
    
    # Process US PCE data
    if 'US_pce_yearly' in cpi_data:
        if debug:
            print("\n=== Processing US PCE data ===")
        us_pce = cpi_data['US_pce_yearly']

        # 确保数据按日期升序排列
        us_pce = us_pce.sort_values('日期')

        # 取最后两个月的今值计算MoM
        valid_pce = us_pce[us_pce['今值'].notna()]
        latest = valid_pce.iloc[-1]
        prev = valid_pce.iloc[-2]

        mom_value = latest['今值'] - prev['今值']  # 简单环比近似
        mom_date = latest['日期']
        yoy_value = latest['今值']
        yoy_date = latest['日期']

        if debug:
            print(f"PCE MoM: {mom_value:.2f}% on {mom_date}")
            print(f"PCE YoY: {yoy_value:.2f}% on {yoy_date}")

        # 计算10年CAGR
        cagr_10y, date_range = calculate_cpi_10y_cagr(us_pce, debug)

        results.append({
            'region': 'US_PCE',
            'mom_value': mom_value,
            'mom_date': mom_date,
            'yoy_value': yoy_value,
            'yoy_date': yoy_date,
            'cagr_10y': cagr_10y,
            'date_range': date_range
        })

    
    # Process CN data
    if 'CN_monthly' in cpi_data and 'CN_yearly' in cpi_data:
        if debug:
            print("\n=== Processing CN CPI data ===")
        cn_monthly = cpi_data['CN_monthly']
        cn_yearly = cpi_data['CN_yearly']
        
        # Get latest non-NaN values (most recent first)
        latest_monthly = cn_monthly[cn_monthly['今值'].notna()].iloc[-1]
        latest_yearly = cn_yearly[cn_yearly['今值'].notna()].iloc[-1]
        
        if debug:
            print(f"Latest monthly: {latest_monthly['日期']} - {latest_monthly['今值']}%")
            print(f"Latest yearly: {latest_yearly['日期']} - {latest_yearly['今值']}%")
        
        # Calculate 10-year CAGR
        cagr_10y, date_range = calculate_cpi_10y_cagr(cn_yearly, debug)
        
        results.append({
            'region': 'CN',
            'mom_value': latest_monthly['今值'],
            'mom_date': latest_monthly['日期'],
            'yoy_value': latest_yearly['今值'],
            'yoy_date': latest_yearly['日期'],
            'cagr_10y': cagr_10y,
            'date_range': date_range
        })
    
    # Process HK data
    if 'HK_yearly' in cpi_data:
        if debug:
            print("\n=== Processing HK CPI data ===")
        hk_df = cpi_data['HK_yearly']
        
        latest = hk_df[hk_df['YoY'].notna()].iloc[-1]
        prev = hk_df[hk_df['YoY'].notna()].iloc[-2]
    
        mom_value = latest['MoM']
        mom_date = latest['日期']
        yoy_value = latest['YoY']
        yoy_date = latest['日期']
    
        if debug:
            print(f"Latest HK MoM: {mom_value}% on {mom_date}")
            print(f"Latest HK YoY: {yoy_value}% on {yoy_date}")
    
        cagr_10y, date_range = calculate_cpi_10y_cagr(
            hk_df.rename(columns={"YoY": "今值"}), debug
        )
    
        results.append({
            'region': 'HK',
            'mom_value': mom_value,
            'mom_date': mom_date,
            'yoy_value': yoy_value,
            'yoy_date': yoy_date,
            'cagr_10y': cagr_10y,
            'date_range': date_range
        })


    return pd.DataFrame(results)

def calculate_cpi_10y_cagr(yearly_data, debug=False):
    """
    计算10年复合增长率
    使用每年一个数据点（间隔12个月）来计算
    从最新的有效数据开始, 取10个有效数据点
    使用复利计算：(1+cpi_year1) * (1+cpi_year2) * ... * (1+cpi_year10) 的10次方根
    
    Args:
        yearly_data: DataFrame containing yearly CPI data
        debug: Boolean flag to enable debug messages
    """
    try:
        if debug:
            print("\n=== Starting 10-year CAGR calculation ===")
            print(f"Input data shape: {yearly_data.shape}")
            print(f"Columns: {yearly_data.columns.tolist()}")
        
        # Get the correct date column (时间 or 日期)
        if '时间' in yearly_data.columns:
            date_col = '时间'
        else:
            date_col = '日期'
        
        if debug:
            print(f"Using date column: {date_col}")
        
        # Convert Chinese date format to YYYY-MM-DD if needed
        def convert_date(date_str):
            if isinstance(date_str, str) and '年' in date_str:
                # Convert "2021年07月" to "2021-07-01"
                year = date_str.split('年')[0]
                month = date_str.split('年')[1].split('月')[0].zfill(2)
                return f"{year}-{month}-01"
            return date_str
        
        yearly_data[date_col] = yearly_data[date_col].apply(convert_date)
        yearly_data[date_col] = pd.to_datetime(yearly_data[date_col])
        
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
        
        # Get 10 yearly records (approximately 12 months apart)
        yearly_records = []
        current_pos = start_pos
        
        for i in range(10):
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
            current_pos += 12
        
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
    
def plot_cpi_trends(cpi_data):
    """
    绘制 CPI 各国 YoY 走势
    """
    plt.rcParams['font.family'] = 'SimHei'
    plt.rcParams['axes.unicode_minus'] = False
    
    plt.figure(figsize=(12, 6))
    
    # 美国 YoY
    us_yoy = cpi_data['US_yearly']
    plt.plot(us_yoy['时间'], us_yoy['现值'], label='US CPI YoY', linestyle='--')

    # 中国 YoY
    cn_yoy = cpi_data['CN_yearly']
    plt.plot(cn_yoy['日期'], cn_yoy['今值'], label='CN CPI YoY', linestyle='--')

    # 香港 YoY
    hk_df = cpi_data['HK_yearly']
    plt.plot(hk_df['日期'], hk_df['YoY'], label='HK CPI YoY', linestyle='--')
    
    plt.title('CPI同比（YoY）走势')
    plt.xlabel('时间')
    plt.ylabel('%')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("output/cpi_trends.png")
    plt.close()


def plot_cpi_trends_since_2024(cpi_data):
    """
    绘制 2024 年以后的 CPI 各国 YoY 走势
    """
    plt.rcParams['font.family'] = 'SimHei'
    plt.rcParams['axes.unicode_minus'] = False

    plt.figure(figsize=(12, 6))
    start_date = pd.Timestamp("2024-01-01")

    # 美国 YoY
    us_yoy = cpi_data['US_yearly']
    us_yoy['时间'] = pd.to_datetime(us_yoy['时间'])
    us_yoy = us_yoy[us_yoy['时间'] >= start_date]
    plt.plot(us_yoy['时间'], us_yoy['现值'], label='US CPI YoY', linestyle='--')

    # 中国 YoY
    cn_yoy = cpi_data['CN_yearly']
    cn_yoy['日期'] = pd.to_datetime(cn_yoy['日期'])
    cn_yoy = cn_yoy[cn_yoy['日期'] >= start_date]
    plt.plot(cn_yoy['日期'], cn_yoy['今值'], label='CN CPI YoY', linestyle='--')

    # 香港 YoY
    hk_df = cpi_data['HK_yearly']
    hk_df['日期'] = pd.to_datetime(hk_df['日期'])
    hk_df = hk_df[hk_df['日期'] >= start_date]
    plt.plot(hk_df['日期'], hk_df['YoY'], label='HK CPI YoY', linestyle='--')
    
    plt.title('2024年起 CPI同比（YoY）走势')
    plt.xlabel('时间')
    plt.ylabel('%')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("output/cpi_trends_since_2024.png")
    plt.close()



def plot_cpi_cagr_bar(metrics_df):
    """
    绘制各国 CPI 10年复合增长率比较条形图
    """
    
    plt.rcParams['font.family'] = 'SimHei'
    plt.rcParams['axes.unicode_minus'] = False
    
    df = metrics_df.copy().sort_values(by='cagr_10y', ascending=True)
    plt.figure(figsize=(8, 5))
    bars = plt.barh(df['region'], df['cagr_10y'])

    for bar, text in zip(bars, df['date_range']):
        width = bar.get_width()
        plt.text(width + 0.05, bar.get_y() + bar.get_height()/2,
                 f"{width:.2f}% ({text})", va='center')

    plt.title("CPI/PCE 十年复合增长率（CAGR）")
    plt.xlabel('%')
    plt.tight_layout()
    plt.grid(True, axis='x')
    plt.savefig("output/cpi_cagr_bar.png")
    plt.close()



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

    # Task 1: CPI/PCE分析
    print("\n1. CPI分析")
    print("-"*30)
    cpi_data = get_cpi_data(time_range)
    if (debug):
        print(cpi_data)
    cpi_metrics = calculate_cpi_metrics(cpi_data, debug)
    print(cpi_metrics)
    
    output_path = 'output'
    os.makedirs(output_path, exist_ok=True)
    
    # 保存原始数据
    raw_data_path = os.path.join(output_path, 'raw_data')
    os.makedirs(raw_data_path, exist_ok=True)
    
    for name, df in cpi_data.items():
        df.to_excel(os.path.join(raw_data_path, f"cpi_{name}.xlsx"), index=False)

    # 格式化输出表格
    def format_date(dt):
        if pd.isna(dt):
            return "-"
        try:
            return pd.to_datetime(dt).strftime("%Y-%m")
        except:
            return "-"

    def format_row(row):
        if row['region'] == 'US_PCE':
            indicator = 'Core PCE'
            region = '美国'
        elif row['region'] == 'US':
            indicator = 'CPI'
            region = '美国'
        elif row['region'] == 'CN':
            indicator = 'CPI'
            region = '中国'
        elif row['region'] == 'HK':
            indicator = 'CPI'
            region = '香港'
        else:
            indicator = row['region']
            region = row['region']

        return pd.Series({
            '区域': region,
            '指标': indicator,
            'MoM (%)': round(row['mom_value'], 2) if pd.notna(row['mom_value']) else '-',
            'MoM 日期': format_date(row['mom_date']),
            'YoY (%)': round(row['yoy_value'], 2) if pd.notna(row['yoy_value']) else '-',
            'YoY 日期': format_date(row['yoy_date']),
            '年化增长10年均值（%）': round(row['cagr_10y'], 2) if pd.notna(row['cagr_10y']) else '-',
            '年化增长10年均值日期': row['date_range'] if pd.notna(row['date_range']) else '-'
        })

    formatted_df = cpi_metrics.apply(format_row, axis=1)
    formatted_df.to_excel(f"{output_path}/cpi_metrics.xlsx", index=False)

    plot_cpi_trends(cpi_data)
    # plot_cpi_trends_since_2024(cpi_data)
    # plot_cpi_cagr_bar(cpi_metrics)

def main(debug = False):
    try:
        generate_report(debug=debug)
    except Exception as e:
        logger.error(f"生成报告时出错: {str(e)}")
        
if __name__ == '__main__':
    main()
    
    