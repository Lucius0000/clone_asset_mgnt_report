import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import traceback

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def calculate_stock_index_metrics(df, market, debug=False):
    """
    计算股票指数的各项指标
    
    Args:
        df: 标准化后的股票数据DataFrame
        market: 市场标识 ('US', 'CN', 'HK')
        debug: 是否启用调试模式
    
    Returns:
        dict: 包含所有计算指标的字典
    """
    if df is None or df.empty:
        logger.error(f"Empty data provided for {market} market")
        return None
        
    try:
        # 设置无风险利率
        risk_free_rates = {
            'US': 0.045,  # 4.5% for S&P 500
            'CN': 0.017,  # 1.7% for CSI 300
            'HK': 0.0062  # 0.62% for Hang Seng
        }
        risk_free_rate = risk_free_rates.get(market, 0.045)  # 默认使用美国利率
        
        # 确保数据按日期降序排序
        df = df.sort_values('date', ascending=False)
        
        # 计算每日收益率 - 需要按时间顺序计算，所以先排序为升序
        df_asc = df.sort_values('date', ascending=True)
        df_asc['daily_return'] = df_asc['close(point)'].pct_change()
        
        # 重新排序为降序，并保持daily_return列
        df = df_asc.sort_values('date', ascending=False)
        
        # 获取最新数据
        latest_data = df.iloc[0]
        current_date = latest_data['date']
        current_close = latest_data['close(point)']
        current_volume = latest_data['volume(share)']
        
        # 计算MoM变化
        one_month_ago = current_date - timedelta(days=30)  # Get last day of previous month
        # 在前后5个交易日的范围内寻找最接近的日期
        month_range_start = one_month_ago - timedelta(days=3)
        month_range_end = one_month_ago + timedelta(days=3)
        month_ago_data = df[(df['date'] >= month_range_start) & (df['date'] <= month_range_end)]
        
        if not month_ago_data.empty:
            # 找到最接近目标日期的数据点
            date_diffs = (month_ago_data['date'] - one_month_ago).abs()
            closest_idx = date_diffs.idxmin()
            month_ago_data = month_ago_data.loc[[closest_idx]]
            mom_change = ((current_close - month_ago_data['close(point)'].iloc[0]) / month_ago_data['close(point)'].iloc[0] * 100)
        else:
            month_ago_data = None
            mom_change = None
        
        if debug and month_ago_data is not None:
            logger.info(f"\nMoM Comparison:")
            logger.info(f"Target Date (Last day of previous month): {one_month_ago.strftime('%Y-%m-%d')}")
            logger.info(f"Current: {current_date.strftime('%Y-%m-%d')} - Close: {current_close:.2f}")
            logger.info(f"Found Date: {month_ago_data['date'].iloc[0].strftime('%Y-%m-%d')} - Close: {month_ago_data['close(point)'].iloc[0]:.2f}")
            logger.info(f"Days Difference: {(current_date - month_ago_data['date'].iloc[0]).days}")
            logger.info(f"MoM Change: {mom_change:.2f}%")
        
        # 计算YoY变化
        one_year_ago = current_date - timedelta(days=365)
        # 在前后10个交易日的范围内寻找最接近的日期
        year_range_start = one_year_ago - timedelta(days=10)
        year_range_end = one_year_ago + timedelta(days=10)
        year_ago_data = df[(df['date'] >= year_range_start) & (df['date'] <= year_range_end)]
        
        if not year_ago_data.empty:
            # 找到最接近目标日期的数据点
            date_diffs = (year_ago_data['date'] - one_year_ago).abs()
            closest_idx = date_diffs.idxmin()
            year_ago_data = year_ago_data.loc[[closest_idx]]
            yoy_change = ((current_close - year_ago_data['close(point)'].iloc[0]) / year_ago_data['close(point)'].iloc[0] * 100)
        else:
            year_ago_data = None
            yoy_change = None
        
        if debug and year_ago_data is not None:
            logger.info(f"\nYoY Comparison:")
            logger.info(f"Target Date: {one_year_ago.strftime('%Y-%m-%d')}")
            logger.info(f"Current: {current_date.strftime('%Y-%m-%d')} - Close: {current_close:.2f}")
            logger.info(f"Found Date: {year_ago_data['date'].iloc[0].strftime('%Y-%m-%d')} - Close: {year_ago_data['close(point)'].iloc[0]:.2f}")
            logger.info(f"Days Difference: {(current_date - year_ago_data['date'].iloc[0]).days}")
            logger.info(f"YoY Change: {yoy_change:.2f}%")
        
        # 计算5年平均
        five_years_ago = current_date - timedelta(days=5*365)
        five_year_data = df[df['date'] >= five_years_ago]
        five_year_avg = five_year_data['close(point)'].mean() if not five_year_data.empty else None
        five_year_start_date = five_year_data['date'].min() if not five_year_data.empty else None
        five_year_end_date = five_year_data['date'].max() if not five_year_data.empty else None
        
        if debug and not five_year_data.empty:
            logger.info(f"\n5-Year Average Calculation:")
            logger.info(f"Date Range: {five_year_start_date.strftime('%Y-%m-%d')} to {five_year_end_date.strftime('%Y-%m-%d')}")
            logger.info(f"Total Days: {len(five_year_data)}")
            logger.info(f"Average Close: {five_year_avg:.2f}")
            logger.info("\nSample Data (One row per year):")
            
            # Get sample rows per year
            for year in range(5):  # Changed back to 5 years
                year_start = current_date - timedelta(days=(year+1)*365)
                year_end = current_date - timedelta(days=year*365)
                year_data = five_year_data[(five_year_data['date'] >= year_start) & (five_year_data['date'] <= year_end)]
                if not year_data.empty:
                    # Get the middle row of the year's data
                    mid_idx = len(year_data) // 2
                    sample_row = year_data.iloc[mid_idx]
                    logger.info(f"Year {year+1} ({year_start.strftime('%Y')}): {sample_row['date'].strftime('%Y-%m-%d')} - Close: {sample_row['close(point)']:.2f}")
        
        # 计算波动率
        def calculate_volatility(data):
            return data['daily_return'].std() * np.sqrt(252)  # 始终使用√252进行年化
        
        # 波动率本质是收益率的标准差，其数值大小与时间窗口长度相关。金融领域通常使用年化波动率（Annualized Volatility）作为统一标准，便于不同周期的数据横向比较。
        # 1. 标准差的计算基准
        # 始终使用日收益率：无论计算哪个周期的波动率，都应基于日收益率计算标准差。
        # 原因：日收益率数据更细致，能捕捉市场高频波动，而月度 / 年度数据会丢失日内信息，导致波动率低估。

        # 2. 年化公式
        # 年化波动率 = 日收益率标准差 × √(年化因子)
        # 年化因子：取决于一年的交易日数量（通常按 252 天计算）。
        # 月度波动率 → 日标准差 × √252
        # 半年度波动率 → 日标准差 × √252
        # 年度波动率 → 日标准差 × √252
        # 5 年波动率 → 日标准差 × √252
        # 关键逻辑：无论时间窗口多长，年化因子始终是√252，因为我们要将波动率统一到 "年化" 维度。

        monthly_data  = df[df['date'] >= month_ago_data['date'].iloc[0]]
        monthly_vol = calculate_volatility(monthly_data)
        semi_annual_vol = calculate_volatility(df.head(126))
        annual_vol = calculate_volatility(df.head(252))
        five_year_vol = calculate_volatility(five_year_data)

        if debug:
            logger.info(f"\nVolatility Calculations:")
            # Monthly volatility
            logger.info(f"\nMonthly Volatility (Annualized):")
            logger.info(f"Date Range: {monthly_data['date'].min().strftime('%Y-%m-%d')} to {monthly_data['date'].max().strftime('%Y-%m-%d')}")
            logger.info(f"Number of Days: {len(monthly_data)}")
            logger.info(f"Daily Returns: Mean={monthly_data['daily_return'].mean():.4f}, Std={monthly_data['daily_return'].std():.4f}")
            logger.info(f"Monthly Volatility (Annualized): {monthly_vol:.2%}")
            logger.info("\nSample Data (5 rows):")
            sample_indices = np.linspace(0, len(monthly_data)-1, 5, dtype=int)
            for idx in sample_indices:
                row = monthly_data.iloc[idx]
                logger.info(f"{row['date'].strftime('%Y-%m-%d')}: Close={row['close(point)']:.2f}, Return={row['daily_return']:.4f}")
            
            # Semi-annual volatility
            semi_annual_data = df.head(126)
            logger.info(f"\nSemi-Annual Volatility (Annualized):")
            logger.info(f"Date Range: {semi_annual_data['date'].min().strftime('%Y-%m-%d')} to {semi_annual_data['date'].max().strftime('%Y-%m-%d')}")
            logger.info(f"Number of Days: {len(semi_annual_data)}")
            logger.info(f"Daily Returns: Mean={semi_annual_data['daily_return'].mean():.4f}, Std={semi_annual_data['daily_return'].std():.4f}")
            logger.info(f"Semi-Annual Volatility (Annualized): {semi_annual_vol:.2%}")
            logger.info("\nSample Data (5 rows):")
            sample_indices = np.linspace(0, len(semi_annual_data)-1, 5, dtype=int)
            for idx in sample_indices:
                row = semi_annual_data.iloc[idx]
                logger.info(f"{row['date'].strftime('%Y-%m-%d')}: Close={row['close(point)']:.2f}, Return={row['daily_return']:.4f}")
            
            # Annual volatility
            annual_data = df.head(252)
            logger.info(f"\nAnnual Volatility (Annualized):")
            logger.info(f"Date Range: {annual_data['date'].min().strftime('%Y-%m-%d')} to {annual_data['date'].max().strftime('%Y-%m-%d')}")
            logger.info(f"Number of Days: {len(annual_data)}")
            logger.info(f"Daily Returns: Mean={annual_data['daily_return'].mean():.4f}, Std={annual_data['daily_return'].std():.4f}")
            logger.info(f"Annual Volatility (Annualized): {annual_vol:.2%}")
            logger.info("\nSample Data (5 rows):")
            sample_indices = np.linspace(0, len(annual_data)-1, 5, dtype=int)
            for idx in sample_indices:
                row = annual_data.iloc[idx]
                logger.info(f"{row['date'].strftime('%Y-%m-%d')}: Close={row['close(point)']:.2f}, Return={row['daily_return']:.4f}")
            
            # 5-year volatilityse) alwintang@Alwins-MacBook-Air html %       
            logger.info(f"\n5-Year Volatility (Annualized):")
            logger.info(f"Date Range: {five_year_data['date'].min().strftime('%Y-%m-%d')} to {five_year_data['date'].max().strftime('%Y-%m-%d')}")
            logger.info(f"Number of Days: {len(five_year_data)}")
            logger.info(f"Daily Returns: Mean={five_year_data['daily_return'].mean():.4f}, Std={five_year_data['daily_return'].std():.4f}")
            logger.info(f"5-Year Volatility (Annualized): {five_year_vol:.2%}")
            logger.info("\nSample Data (5 rows):")
            sample_indices = np.linspace(0, len(five_year_data)-1, 5, dtype=int)
            for idx in sample_indices:
                row = five_year_data.iloc[idx]
                logger.info(f"{row['date'].strftime('%Y-%m-%d')}: Close={row['close(point)']:.2f}, Return={row['daily_return']:.4f}")

         # 计算年化收益率
        def calculate_annualized_return(data, period_name, debug=False):
            """计算年化收益率
            长期业绩（1年以上）：使用几何平均（尾减头）
            短期业绩（1个月以内）：使用算术平均
            """
            if data.empty or len(data) < 2:
                return None
            
            # 判断是否为长期业绩（超过1年）
            date_range = (data['date'].max() - data['date'].min()).days
            is_long_term = date_range >= 360
            
            if is_long_term:
                # 长期业绩：使用几何平均（尾减头）
                start_price = data['close(point)'].iloc[-1]  # 最早的价格
                end_price = data['close(point)'].iloc[0]    # 最新的价格
                total_return = (end_price - start_price) / start_price
                years = date_range / 365.25
                annualized_return = (1 + total_return) ** (1 / years) - 1
                
                if debug:
                    logger.info(f"{period_name} Geometric Annualized Return:")
                    logger.info(f"Start Date: {data['date'].min().strftime('%Y-%m-%d')}, Start Price: {start_price:.2f}")
                    logger.info(f"End Date: {data['date'].max().strftime('%Y-%m-%d')}, End Price: {end_price:.2f}")
                    logger.info(f"Total Return: {total_return:.2%}, Years: {years:.2f}")
                    logger.info(f"Geometric Annualized Return: {annualized_return:.2%}")
            else:
                # 短期业绩：使用算术平均
                daily_return_mean = data['daily_return'].mean()
                annualized_return = daily_return_mean * 252
                
                if debug:
                    logger.info(f"{period_name} Arithmetic Annualized Return (Note: Based on arithmetic mean, does not represent actual compound return):")
                    logger.info(f"Date Range: {data['date'].min().strftime('%Y-%m-%d')} to {data['date'].max().strftime('%Y-%m-%d')}")
                    logger.info(f"Days: {len(data)}, Daily Return Mean: {daily_return_mean:.4f}")
                    logger.info(f"Arithmetic Annualized Return: {annualized_return:.2%}")
            
            return annualized_return
        
        # 计算各时间段的年化收益率
        monthly_annualized_return = calculate_annualized_return(monthly_data, "Monthly", debug)
        semi_annual_annualized_return = calculate_annualized_return(df.head(126), "Semi-Annual", debug)
        annual_annualized_return = calculate_annualized_return(df.head(252), "Annual", debug)
        five_year_annualized_return = calculate_annualized_return(five_year_data, "5-Year", debug)
        
        # 计算夏普比率
        def calculate_sharpe_ratio(data, annualized_return, risk_free_rate):
            # 年化波动率 = 日标准差 × √252
            annualized_volatility = data['daily_return'].std() * np.sqrt(252)
            if annualized_volatility == 0:
                return None
            # 使用年化的无风险利率（已经是年化值，无需调整）
            return (annualized_return - risk_free_rate) / annualized_volatility
        
        monthly_sharpe = calculate_sharpe_ratio(monthly_data, monthly_annualized_return, risk_free_rate)
        semi_annual_sharpe = calculate_sharpe_ratio(df.head(126), semi_annual_annualized_return, risk_free_rate)
        annual_sharpe = calculate_sharpe_ratio(df.head(252), annual_annualized_return, risk_free_rate)
        five_year_sharpe = calculate_sharpe_ratio(five_year_data, five_year_annualized_return, risk_free_rate)
        

        if debug:
            logger.info(f"\nSharpe Ratio Calculations (Annualized Risk-free rate: {risk_free_rate:.2%}):")
            # Monthly Sharpe
            logger.info(f"\nMonthly Sharpe Ratio:")
            logger.info(f"Date Range: {monthly_data['date'].min().strftime('%Y-%m-%d')} to {monthly_data['date'].max().strftime('%Y-%m-%d')}")
            logger.info(f"Number of Days: {len(monthly_data)}")
            logger.info(f"Daily Returns: Mean={monthly_data['daily_return'].mean():.4f}, Std={monthly_data['daily_return'].std():.4f}")
            logger.info(f"Annualized Return: {monthly_annualized_return:.2%}")
            logger.info(f"Annualized Volatility: {monthly_vol:.2%}")
            logger.info(f"Monthly Sharpe Ratio: {monthly_sharpe:.2f}")
            sample_indices = np.linspace(0, len(monthly_data)-1, 5, dtype=int)
            for idx in sample_indices:
                row = monthly_data.iloc[idx]
                logger.info(f"{row['date'].strftime('%Y-%m-%d')}: Close={row['close(point)']:.2f}, Return={row['daily_return']:.4f}")
            

            # Semi-annual Sharpe
            logger.info(f"\nSemi-Annual Sharpe Ratio:")
            logger.info(f"Date Range: {semi_annual_data['date'].min().strftime('%Y-%m-%d')} to {semi_annual_data['date'].max().strftime('%Y-%m-%d')}")
            logger.info(f"Number of Days: {len(semi_annual_data)}")
            logger.info(f"Daily Returns: Mean={semi_annual_data['daily_return'].mean():.4f}, Std={semi_annual_data['daily_return'].std():.4f}")
            logger.info(f"Annualized Return: {semi_annual_annualized_return:.2%}")
            logger.info(f"Annualized Volatility: {semi_annual_vol:.2%}")
            logger.info(f"Semi-Annual Sharpe Ratio: {semi_annual_sharpe:.2f}")
            sample_indices = np.linspace(0, len(semi_annual_data)-1, 5, dtype=int)
            for idx in sample_indices:
                row = semi_annual_data.iloc[idx]
                logger.info(f"{row['date'].strftime('%Y-%m-%d')}: Close={row['close(point)']:.2f}, Return={row['daily_return']:.4f}")
            


            # Annual Sharpe
            logger.info(f"\nAnnual Sharpe Ratio:")
            logger.info(f"Date Range: {annual_data['date'].min().strftime('%Y-%m-%d')} to {annual_data['date'].max().strftime('%Y-%m-%d')}")
            logger.info(f"Number of Days: {len(annual_data)}")
            logger.info(f"Daily Returns: Mean={annual_data['daily_return'].mean():.4f}, Std={annual_data['daily_return'].std():.4f}")
            logger.info(f"Annualized Return: {annual_annualized_return:.2%}")
            logger.info(f"Annualized Volatility: {annual_vol:.2%}")
            logger.info(f"Annual Sharpe Ratio: {annual_sharpe:.2f}")
            sample_indices = np.linspace(0, len(annual_data)-1, 5, dtype=int)
            for idx in sample_indices:
                row = annual_data.iloc[idx]
                logger.info(f"{row['date'].strftime('%Y-%m-%d')}: Close={row['close(point)']:.2f}, Return={row['daily_return']:.4f}")
            
            
            # 5-year Sharpe
            logger.info(f"\n5-Year Sharpe Ratio:")
            logger.info(f"Date Range: {five_year_data['date'].min().strftime('%Y-%m-%d')} to {five_year_data['date'].max().strftime('%Y-%m-%d')}")
            logger.info(f"Number of Days: {len(five_year_data)}")
            logger.info(f"Daily Returns: Mean={five_year_data['daily_return'].mean():.4f}, Std={five_year_data['daily_return'].std():.4f}")
            logger.info(f"Annualized Return: {five_year_annualized_return:.2%}")
            logger.info(f"Annualized Volatility: {five_year_vol:.2%}")
            logger.info(f"5-Year Sharpe Ratio: {five_year_sharpe:.2f}")
            sample_indices = np.linspace(0, len(five_year_data)-1, 5, dtype=int)
            for idx in sample_indices:
                row = five_year_data.iloc[idx]
                logger.info(f"{row['date'].strftime('%Y-%m-%d')}: Close={row['close(point)']:.2f}, Return={row['daily_return']:.4f}")
        

        metrics = {
            'current_data': {
                'current_date': current_date.strftime('%Y-%m-%d'),
                'close': current_close,
                'volume': current_volume
            },
            'change_overtime': {
                'MoM (%)': mom_change,
                'YoY (%)': yoy_change,
                '5_year_average': five_year_avg,
                '5_year_average_dates': f"{five_year_start_date.strftime('%Y-%m-%d')} to {five_year_end_date.strftime('%Y-%m-%d')}" if five_year_start_date and five_year_end_date else None
            },
            'volatility': {
                'monthly_volatility': monthly_vol,
                'semi_annual_volatility': semi_annual_vol,
                'annual_volatility': annual_vol,
                '5_year_volatility': five_year_vol
            },
            'annualized_return': {
                'monthly_annualized_return': monthly_annualized_return,
                'semi_annual_annualized_return': semi_annual_annualized_return,
                'annual_annualized_return': annual_annualized_return,
                '5_year_annualized_return': five_year_annualized_return
            },
            'sharpe_ratio': {
                'monthly_sharpe_ratio': monthly_sharpe,
                'semi_annual_sharpe_ratio': semi_annual_sharpe,
                'annual_sharpe_ratio': annual_sharpe,
                '5_year_sharpe_ratio': five_year_sharpe
            }
        }
        
        if debug:
            logger.info(f"Calculated metrics for {market} market:")
            logger.info(f"Current data: {metrics['current_data']}")
            logger.info(f"Change overtime: {metrics['change_overtime']}")
            logger.info(f"Volatility: {metrics['volatility']}")
            logger.info(f"Annualized return: {metrics['annualized_return']}")
            logger.info(f"Sharpe ratio: {metrics['sharpe_ratio']}")
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error calculating metrics for {market} market: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def normalize_stock_data(df, debug=False):
    """
    标准化股票数据格式，确保输出包含以下字段：
    date, open(point), high(point), low(point), close(point), volume(share)
    
    Args:
        df: 原始数据DataFrame
        debug: 是否启用调试模式
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=['date', 'open(point)', 'high(point)', 'low(point)', 'close(point)', 'volume(share)'])
    
    try:
        # 创建标准列名映射
        column_mappings = {
            # 日期列映射
            'date': ['date', '日期', '报告日'],
            # 开盘价列映射
            'open(point)': ['open(point)', 'open (point)', 'open', '开盘', '开盘价'],
            # 最高价列映射
            'high(point)': ['high(point)', 'high (point)', 'high', '最高', '最高价'],
            # 最低价列映射
            'low(point)': ['low(point)', 'low (point)', 'low', '最低', '最低价'],
            # 收盘价列映射
            'close(point)': ['close(point)', 'close (point)', 'close', '收盘', '收盘价'],
            # 成交量列映射
            'volume(share)': ['volume(share)', 'volume (share)', 'volume', '成交量', '成交股数']
        }
        
        # 创建新的标准化DataFrame
        normalized_df = pd.DataFrame()
        
        # 对每个标准列名进行处理
        for std_col, possible_names in column_mappings.items():
            # 查找匹配的列名
            found_col = None
            for col in possible_names:
                if col in df.columns:
                    found_col = col
                    break
            
            if found_col:
                normalized_df[std_col] = df[found_col]
            else:
                # 如果找不到对应的列，填充0
                normalized_df[std_col] = 0
                if debug:
                    logger.warning(f"Column {std_col} not found in data, filled with 0")
        
        # 确保日期列是datetime类型
        normalized_df['date'] = pd.to_datetime(normalized_df['date'])
        
        # 按日期排序（降序）
        normalized_df = normalized_df.sort_values('date', ascending=False)
        
        if debug:
            logger.info(f"Normalized data shape: {normalized_df.shape}")
            logger.info(f"Normalized columns: {normalized_df.columns.tolist()}")
            logger.info(f"Sample of normalized data:\n{normalized_df.head()}")
        
        return normalized_df
        
    except Exception as e:
        logger.error(f"Error normalizing stock data: {str(e)}")
        logger.error(traceback.format_exc())
        return pd.DataFrame(columns=['date', 'open(point)', 'high(point)', 'low(point)', 'close(point)', 'volume(share)'])

def get_stock_index_data(time_range=2920, debug=False):  # 2920 days ≈ 8 years
    """
    获取美国、中国、香港的股票指数数据
    返回: 包含各国股票指数数据的字典
    
    Args:
        time_range: 获取数据的时间范围（天），默认8年
        debug: 是否启用调试模式
    """
    try:
        if debug:
            logger.info(f"开始获取股票指数数据，时间范围：{time_range}天")
        
        # 计算起始日期
        end_date = datetime.now()
        start_date = end_date - timedelta(days=time_range)
        
        if debug:
            logger.info(f"数据获取时间范围：{start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")
        
        # 美国标普500指数数据
        try:
            if debug:
                logger.info("开始获取美国标普500指数数据...")
            us_sp500 = ak.index_us_stock_sina(symbol=".inx")
            if us_sp500 is None or us_sp500.empty:
                logger.error("US SP500 data is empty or None")
                us_sp500 = pd.DataFrame()
            else:
                logger.info(f"Successfully fetched US SP500 data, shape: {us_sp500.shape}")
                if debug:
                    logger.info(f"US SP500 data columns: {us_sp500.columns.tolist()}")
                    logger.info(f"US SP500 data sample:\n{us_sp500.head()}")
                ## Sample fetched data
                #    date   open (point)   high (point)   low (point)   close (point)  volume (share)   amount (?)
                # 0  2004-01-02  1111.92  1118.85  1105.0800  1108.48  1153200000  1281300000000
                # 1  2004-01-05  1108.48  1122.22  1108.4800  1122.22  1578200064  1760250000000
                # 2  2004-01-06  1122.22  1124.46  1118.4399  1123.67  1494499968  1677120000000
                # 3  2004-01-07  1123.67  1126.33  1116.4500  1126.33  1704899968  1914940000000
                # 4  2004-01-08  1126.33  1131.92  1124.9100  1131.92  1868400000  2108990000000
                # ...
                # 5391  2025-06-03  5938.5601  5981.3501  5929.0000  5970.3701  2959610307       0
                # 5392  2025-06-04  5978.9399  5990.4800  5966.1099  5970.8101  2680331774       0
                # 5393  2025-06-05  5985.6699  5999.7002  5921.2002  5939.2998  3128216678       0
                # 5394  2025-06-06  5987.0601  6016.8701  5978.6299  6000.3599  2557851418       0
                # 5395  2025-06-09  6004.6299  6021.3101  5994.1802  6005.8799  2882903740       0
                # 5396  2025-06-10  6009.9102  6043.0098  6000.2798  6038.8101  2976909395       0
                # 5397  2025-06-11  6049.3799  6059.3999  6002.3198  6022.2402  2978585361       0
                # 5398  2025-06-12  6009.8999  6045.4302  6003.8799  6045.2598  2614508550       0
                # 5399  2025-06-13  6000.5601  6026.1602  5963.2100  5976.9702  3001801788       0
                # 5400  2025-06-16  6004.0000  6050.8301  6004.0000  6033.1099  2870307250       0
                # 标准化数据
                us_sp500 = normalize_stock_data(us_sp500, debug)
                # 过滤日期范围
                us_sp500 = us_sp500[us_sp500['date'] >= start_date]
                if debug:
                    logger.info(f"US SP500 data after date filtering: {us_sp500.shape[0]} rows")
        except Exception as e:
            logger.error(f"Error fetching US SP500 data: {str(e)}")
            logger.error(traceback.format_exc())
            us_sp500 = pd.DataFrame()
        
        # 中国沪深300指数数据
        try:
            if debug:
                logger.info("开始获取中国沪深300指数数据...")
            cn_hs300 = ak.stock_zh_index_daily(symbol="sh000300")
            if cn_hs300 is None or cn_hs300.empty:
                logger.error("China HS300 data is empty or None")
                cn_hs300 = pd.DataFrame()
            else:
                logger.info(f"Successfully fetched China HS300 data, shape: {cn_hs300.shape}")
                if debug:
                    logger.info(f"China HS300 data columns: {cn_hs300.columns.tolist()}")
                    logger.info(f"China HS300 data sample:\n{cn_hs300.head()}")
                ## Sample fetched data
                #    date   open(point)   high(point)  low(point)  close(point)  volume(share)
                # 0  2002-01-04  1316.455  1316.455  1316.455  1316.455       0
                # 1  2002-01-07  1302.084  1302.084  1302.084  1302.084       0
                # 2  2002-01-08  1292.714  1292.714  1292.714  1292.714       0
                # 3  2002-01-09  1272.645  1272.645  1272.645  1272.645       0
                # 4  2002-01-10  1281.261  1281.261  1281.261  1281.261       0
                # ...
                # 5676  2025-06-03  3833.458  3863.295  3832.720  3852.013  13140309000
                # 5677  2025-06-04  3855.488  3875.857  3855.488  3868.743  11680387300
                # 5678  2025-06-05  3872.637  3883.135  3861.272  3877.556  12287818400
                # 5679  2025-06-06  3878.103  3889.456  3869.387  3873.984  11098797100
                # 5680  2025-06-09  3877.798  3894.652  3871.992  3885.246  13630629300
                # 5681  2025-06-10  3887.551  3897.457  3850.466  3865.465  14556385900
                # 5682  2025-06-11  3870.672  3911.611  3870.672  3894.625  14241477200
                # 5683  2025-06-12  3885.520  3900.041  3870.384  3892.199  13399869700
                # 5684  2025-06-13  3881.492  3889.160  3853.819  3864.182  17253953600
                # 5685  2025-06-16  3853.619  3876.170  3853.619  3873.795  13734183500
                # 标准化数据
                cn_hs300 = normalize_stock_data(cn_hs300, debug)
                # 过滤日期范围
                cn_hs300 = cn_hs300[cn_hs300['date'] >= start_date]
                if debug:
                    logger.info(f"China HS300 data after date filtering: {cn_hs300.shape[0]} rows")
        except Exception as e:
            logger.error(f"Error fetching China HS300 data: {str(e)}")
            logger.error(traceback.format_exc())
            cn_hs300 = pd.DataFrame()
        
        # 香港恒生指数数据
        try:
            if debug:
                logger.info("开始获取香港恒生指数数据...")
            hk_hsi = ak.stock_hk_index_daily_sina(symbol="HSI")
            if hk_hsi is None or hk_hsi.empty:
                logger.error("Hong Kong HSI data is empty or None")
                hk_hsi = pd.DataFrame()
            else:
                logger.info(f"Successfully fetched Hong Kong HSI data, shape: {hk_hsi.shape}")
                if debug:
                    logger.info(f"Hong Kong HSI data columns: {hk_hsi.columns.tolist()}")
                    logger.info(f"Hong Kong HSI data sample:\n{hk_hsi.head()}")
                ## Sample fetched data
                #    date   open(point)   high(point)  low(point)  close(point)  volume(share)
                # 0  2013-08-20  22396.289  22481.740  21907.211  21964.051  63521943552
                # 1  2013-08-21  21964.689  21970.250  21618.600  21817.730   7237451920
                # 2  2013-08-22  21538.189  21944.471  21538.189  21895.400   8229202926
                # 3  2013-08-23  22006.410  22103.900  21769.039  21863.510   7544465315
                # 4  2013-08-26  21943.400  22116.000  21898.150  22005.320   7672641627
                # ...
                # 2898  2025-06-03  23281.10  23535.37  23281.10  23512.490  15837935351
                # 2899  2025-06-04  23499.78  23716.93  23481.46  23654.031  16869939569
                # 2900  2025-06-05  23828.69  23911.14  23732.31  23906.969  16860179017
                # 2901  2025-06-06  23941.57  23951.14  23773.36  23792.541  17148833440
                # 2902  2025-06-09  23977.54  24181.43  23957.96  24181.430  19478192065
                # 2903  2025-06-10  24231.31  24296.47  24003.38  24162.871  20758220406
                # 2904  2025-06-11  24191.32  24439.35  24179.35  24366.941  19662842424
                # 2905  2025-06-12  24223.12  24288.76  24002.42  24035.379  21483102413
                # 2906  2025-06-13  23959.81  24100.32  23774.92  23892.561  26328661263
                # 2907  2025-06-16  23791.79  24125.05  23718.72  24060.990  22854730447
                hk_hsi = normalize_stock_data(hk_hsi, debug)
                # 过滤日期范围
                hk_hsi = hk_hsi[hk_hsi['date'] >= start_date]
                if debug:
                    logger.info(f"Hong Kong HSI data after date filtering: {hk_hsi.shape[0]} rows")
        except Exception as e:
            logger.error(f"Error fetching Hong Kong HSI data: {str(e)}")
            logger.error(traceback.format_exc())
            hk_hsi = pd.DataFrame()
        
        # 检查数据可用性
        data_availability = {
            'US_SP500': not us_sp500.empty,
            'CN_HS300': not cn_hs300.empty,
            'HK_HSI': not hk_hsi.empty
        }
        
        if debug:
            logger.info("数据可用性统计：")
            for index, available in data_availability.items():
                logger.info(f"{index}: {'可用' if available else '不可用'}")
        
        # 如果没有有效数据，返回None
        if not any(data_availability.values()):
            logger.error("No valid data was fetched for any index")
            return None
            
        return {
            'US_SP500': us_sp500,
            'CN_HS300': cn_hs300,
            'HK_HSI': hk_hsi
        }
        
    except Exception as e:
        logger.error(f"获取股票指数数据时出错: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def main(time_range=2920, debug=False):  # 2920 days ≈ 8 years
    """
    主函数：获取并打印股票指数数据
    
    Args:
        time_range: 获取数据的时间范围（天），默认8年
        debug: 是否启用调试模式
    """
    try:
        logger.info(f"开始获取股票指数数据... (time_range={time_range}天, debug={debug})")
        stock_data = get_stock_index_data(time_range=time_range, debug=debug)
        
        if stock_data:
            if debug:
                print("\n=== 股票指数数据 ===")
                print(f"数据获取时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"数据范围: 最近 {time_range} 天")
            
            # 计算并显示美国标普500指数的指标
            if not stock_data['US_SP500'].empty:
                if debug:
                    print("\n1. 美国标普500指数 (SPX)")
                    print("-"*30)
                    print(f"数据范围: {stock_data['US_SP500']['date'].min().strftime('%Y-%m-%d')} 至 {stock_data['US_SP500']['date'].max().strftime('%Y-%m-%d')}")
                    print(f"数据条数: {len(stock_data['US_SP500'])}")
                us_metrics = calculate_stock_index_metrics(stock_data['US_SP500'], 'US', debug)
                if us_metrics:
                    if debug:
                        print("\n当前数据:")
                        print(f"日期: {us_metrics['current_data']['current_date']}")
                        print(f"收盘价: {us_metrics['current_data']['close']:.2f}")
                        print(f"成交量: {us_metrics['current_data']['volume']:,.0f}")
                        
                        print("\n变化情况:")
                        print(f"月环比: {us_metrics['change_overtime']['MoM (%)']:.2f}%" if us_metrics['change_overtime']['MoM (%)'] is not None else "月环比: 数据不足")
                        print(f"年同比: {us_metrics['change_overtime']['YoY (%)']:.2f}%" if us_metrics['change_overtime']['YoY (%)'] is not None else "年同比: 数据不足")
                        print(f"5年平均: {us_metrics['change_overtime']['5_year_average']:.2f}" if us_metrics['change_overtime']['5_year_average'] is not None else "5年平均: 数据不足")
                        print(f"5年平均区间: {us_metrics['change_overtime']['5_year_average_dates']}" if us_metrics['change_overtime']['5_year_average_dates'] is not None else "5年平均区间: 数据不足")
                        
                        print("\n波动率:")
                        print(f"月度波动率: {us_metrics['volatility']['monthly_volatility']:.2%}" if us_metrics['volatility']['monthly_volatility'] is not None else "月度波动率: 数据不足")
                        print(f"半年波动率: {us_metrics['volatility']['semi_annual_volatility']:.2%}" if us_metrics['volatility']['semi_annual_volatility'] is not None else "半年波动率: 数据不足")
                        print(f"年度波动率: {us_metrics['volatility']['annual_volatility']:.2%}" if us_metrics['volatility']['annual_volatility'] is not None else "年度波动率: 数据不足")
                        print(f"5年波动率: {us_metrics['volatility']['5_year_volatility']:.2%}" if us_metrics['volatility']['5_year_volatility'] is not None else "5年波动率: 数据不足")
                        
                        print("\n年化收益率:")
                        print(f"月度年化收益率: {us_metrics['annualized_return']['monthly_annualized_return']:.2%}" if us_metrics['annualized_return']['monthly_annualized_return'] is not None else "月度年化收益率: 数据不足")
                        print(f"半年度年化收益率: {us_metrics['annualized_return']['semi_annual_annualized_return']:.2%}" if us_metrics['annualized_return']['semi_annual_annualized_return'] is not None else "半年度年化收益率: 数据不足")
                        print(f"年度年化收益率: {us_metrics['annualized_return']['annual_annualized_return']:.2%}" if us_metrics['annualized_return']['annual_annualized_return'] is not None else "年度年化收益率: 数据不足")
                        print(f"5年年化收益率: {us_metrics['annualized_return']['5_year_annualized_return']:.2%}" if us_metrics['annualized_return']['5_year_annualized_return'] is not None else "5年年化收益率: 数据不足")
                        
                        print("\n夏普比率:")
                        print(f"月度夏普比率: {us_metrics['sharpe_ratio']['monthly_sharpe_ratio']:.2f}" if us_metrics['sharpe_ratio']['monthly_sharpe_ratio'] is not None else "月度夏普比率: 数据不足")
                        print(f"半年夏普比率: {us_metrics['sharpe_ratio']['semi_annual_sharpe_ratio']:.2f}" if us_metrics['sharpe_ratio']['semi_annual_sharpe_ratio'] is not None else "半年夏普比率: 数据不足")
                        print(f"年度夏普比率: {us_metrics['sharpe_ratio']['annual_sharpe_ratio']:.2f}" if us_metrics['sharpe_ratio']['annual_sharpe_ratio'] is not None else "年度夏普比率: 数据不足")
                        print(f"5年夏普比率: {us_metrics['sharpe_ratio']['5_year_sharpe_ratio']:.2f}" if us_metrics['sharpe_ratio']['5_year_sharpe_ratio'] is not None else "5年夏普比率: 数据不足")
            else:
                if debug:
                    print("\n1. 美国标普500指数 (SPX) - 数据获取失败")
            
            # 计算并显示中国沪深300指数的指标
            if not stock_data['CN_HS300'].empty:
                if debug:
                    print("\n2. 中国沪深300指数 (sh000300)")
                    print("-"*30)
                    print(f"数据范围: {stock_data['CN_HS300']['date'].min().strftime('%Y-%m-%d')} 至 {stock_data['CN_HS300']['date'].max().strftime('%Y-%m-%d')}")
                    print(f"数据条数: {len(stock_data['CN_HS300'])}")
                cn_metrics = calculate_stock_index_metrics(stock_data['CN_HS300'], 'CN', debug)
                if cn_metrics:
                    if debug:
                        print("\n当前数据:")
                        print(f"日期: {cn_metrics['current_data']['current_date']}")
                        print(f"收盘价: {cn_metrics['current_data']['close']:.2f}")
                        print(f"成交量: {cn_metrics['current_data']['volume']:,.0f}")
                        
                        print("\n变化情况:")
                        print(f"月环比: {cn_metrics['change_overtime']['MoM (%)']:.2f}%" if cn_metrics['change_overtime']['MoM (%)'] is not None else "月环比: 数据不足")
                        print(f"年同比: {cn_metrics['change_overtime']['YoY (%)']:.2f}%" if cn_metrics['change_overtime']['YoY (%)'] is not None else "年同比: 数据不足")
                        print(f"5年平均: {cn_metrics['change_overtime']['5_year_average']:.2f}" if cn_metrics['change_overtime']['5_year_average'] is not None else "5年平均: 数据不足")
                        print(f"5年平均区间: {cn_metrics['change_overtime']['5_year_average_dates']}" if cn_metrics['change_overtime']['5_year_average_dates'] is not None else "5年平均区间: 数据不足")
                        
                        print("\n波动率:")
                        print(f"月度波动率: {cn_metrics['volatility']['monthly_volatility']:.2%}" if cn_metrics['volatility']['monthly_volatility'] is not None else "月度波动率: 数据不足")
                        print(f"半年波动率: {cn_metrics['volatility']['semi_annual_volatility']:.2%}" if cn_metrics['volatility']['semi_annual_volatility'] is not None else "半年波动率: 数据不足")
                        print(f"年度波动率: {cn_metrics['volatility']['annual_volatility']:.2%}" if cn_metrics['volatility']['annual_volatility'] is not None else "年度波动率: 数据不足")
                        print(f"5年波动率: {cn_metrics['volatility']['5_year_volatility']:.2%}" if cn_metrics['volatility']['5_year_volatility'] is not None else "5年波动率: 数据不足")
                        
                        print("\n年化收益率:")
                        print(f"月度年化收益率: {cn_metrics['annualized_return']['monthly_annualized_return']:.2%}" if cn_metrics['annualized_return']['monthly_annualized_return'] is not None else "月度年化收益率: 数据不足")
                        print(f"半年度年化收益率: {cn_metrics['annualized_return']['semi_annual_annualized_return']:.2%}" if cn_metrics['annualized_return']['semi_annual_annualized_return'] is not None else "半年度年化收益率: 数据不足")
                        print(f"年度年化收益率: {cn_metrics['annualized_return']['annual_annualized_return']:.2%}" if cn_metrics['annualized_return']['annual_annualized_return'] is not None else "年度年化收益率: 数据不足")
                        print(f"5年年化收益率: {cn_metrics['annualized_return']['5_year_annualized_return']:.2%}" if cn_metrics['annualized_return']['5_year_annualized_return'] is not None else "5年年化收益率: 数据不足")
                        
                        print("\n夏普比率:")
                        print(f"月度夏普比率: {cn_metrics['sharpe_ratio']['monthly_sharpe_ratio']:.2f}" if cn_metrics['sharpe_ratio']['monthly_sharpe_ratio'] is not None else "月度夏普比率: 数据不足")
                        print(f"半年夏普比率: {cn_metrics['sharpe_ratio']['semi_annual_sharpe_ratio']:.2f}" if cn_metrics['sharpe_ratio']['semi_annual_sharpe_ratio'] is not None else "半年夏普比率: 数据不足")
                        print(f"年度夏普比率: {cn_metrics['sharpe_ratio']['annual_sharpe_ratio']:.2f}" if cn_metrics['sharpe_ratio']['annual_sharpe_ratio'] is not None else "年度夏普比率: 数据不足")
                        print(f"5年夏普比率: {cn_metrics['sharpe_ratio']['5_year_sharpe_ratio']:.2f}" if cn_metrics['sharpe_ratio']['5_year_sharpe_ratio'] is not None else "5年夏普比率: 数据不足")
            else:
                if debug:
                    print("\n2. 中国沪深300指数 (sh000300) - 数据获取失败")
            
            # 计算并显示香港恒生指数的指标
            if not stock_data['HK_HSI'].empty:
                if debug:
                    print("\n3. 香港恒生指数 (HSI)")
                    print("-"*30)
                    print(f"数据范围: {stock_data['HK_HSI']['date'].min().strftime('%Y-%m-%d')} 至 {stock_data['HK_HSI']['date'].max().strftime('%Y-%m-%d')}")
                    print(f"数据条数: {len(stock_data['HK_HSI'])}")
                hk_metrics = calculate_stock_index_metrics(stock_data['HK_HSI'], 'HK', debug)
                if hk_metrics:
                    if debug:
                        print("\n当前数据:")
                        print(f"日期: {hk_metrics['current_data']['current_date']}")
                        print(f"收盘价: {hk_metrics['current_data']['close']:.2f}")
                        print(f"成交量: {hk_metrics['current_data']['volume']:,.0f}")
                        
                        print("\n变化情况:")
                        print(f"月环比: {hk_metrics['change_overtime']['MoM (%)']:.2f}%" if hk_metrics['change_overtime']['MoM (%)'] is not None else "月环比: 数据不足")
                        print(f"年同比: {hk_metrics['change_overtime']['YoY (%)']:.2f}%" if hk_metrics['change_overtime']['YoY (%)'] is not None else "年同比: 数据不足")
                        print(f"5年平均: {hk_metrics['change_overtime']['5_year_average']:.2f}" if hk_metrics['change_overtime']['5_year_average'] is not None else "5年平均: 数据不足")
                        print(f"5年平均区间: {hk_metrics['change_overtime']['5_year_average_dates']}" if hk_metrics['change_overtime']['5_year_average_dates'] is not None else "5年平均区间: 数据不足")
                        
                        print("\n波动率:")
                        print(f"月度波动率: {hk_metrics['volatility']['monthly_volatility']:.2%}" if hk_metrics['volatility']['monthly_volatility'] is not None else "月度波动率: 数据不足")
                        print(f"半年波动率: {hk_metrics['volatility']['semi_annual_volatility']:.2%}" if hk_metrics['volatility']['semi_annual_volatility'] is not None else "半年波动率: 数据不足")
                        print(f"年度波动率: {hk_metrics['volatility']['annual_volatility']:.2%}" if hk_metrics['volatility']['annual_volatility'] is not None else "年度波动率: 数据不足")
                        print(f"5年波动率: {hk_metrics['volatility']['5_year_volatility']:.2%}" if hk_metrics['volatility']['5_year_volatility'] is not None else "5年波动率: 数据不足")
                        
                        print("\n年化收益率:")
                        print(f"月度年化收益率: {hk_metrics['annualized_return']['monthly_annualized_return']:.2%}" if hk_metrics['annualized_return']['monthly_annualized_return'] is not None else "月度年化收益率: 数据不足")
                        print(f"半年度年化收益率: {hk_metrics['annualized_return']['semi_annual_annualized_return']:.2%}" if hk_metrics['annualized_return']['semi_annual_annualized_return'] is not None else "半年度年化收益率: 数据不足")
                        print(f"年度年化收益率: {hk_metrics['annualized_return']['annual_annualized_return']:.2%}" if hk_metrics['annualized_return']['annual_annualized_return'] is not None else "年度年化收益率: 数据不足")
                        print(f"5年年化收益率: {hk_metrics['annualized_return']['5_year_annualized_return']:.2%}" if hk_metrics['annualized_return']['5_year_annualized_return'] is not None else "5年年化收益率: 数据不足")
                        
                        print("\n夏普比率:")
                        print(f"月度夏普比率: {hk_metrics['sharpe_ratio']['monthly_sharpe_ratio']:.2f}" if hk_metrics['sharpe_ratio']['monthly_sharpe_ratio'] is not None else "月度夏普比率: 数据不足")
                        print(f"半年夏普比率: {hk_metrics['sharpe_ratio']['semi_annual_sharpe_ratio']:.2f}" if hk_metrics['sharpe_ratio']['semi_annual_sharpe_ratio'] is not None else "半年夏普比率: 数据不足")
                        print(f"年度夏普比率: {hk_metrics['sharpe_ratio']['annual_sharpe_ratio']:.2f}" if hk_metrics['sharpe_ratio']['annual_sharpe_ratio'] is not None else "年度夏普比率: 数据不足")
                        print(f"5年夏普比率: {hk_metrics['sharpe_ratio']['5_year_sharpe_ratio']:.2f}" if hk_metrics['sharpe_ratio']['5_year_sharpe_ratio'] is not None else "5年夏普比率: 数据不足")
            else:
                if debug:
                    print("\n3. 香港恒生指数 (HSI) - 数据获取失败")
            

            ## print the metrics in a table
            print("\n=== Stock Index Data Summary ===")
            print(f"Data Retrieval Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Data Range: Last {time_range} days")
            
            # 定义表格头部
            headers = [
                "Index", "Latest Date", "Close", "Volume", "MoM (%)", "YoY (%)", 
                "5Y Avg", "5Y Avg Range", "Monthly Vol (Ann.)", "Semi-Annual Vol (Ann.)", 
                "Annual Vol (Ann.)", "5Y Vol (Ann.)", "Monthly Annualized Return", "Semi-Annual Annualized Return", 
                "Annual Annualized Return", "5Y Annualized Return", "Monthly Sharpe", "Semi-Annual Sharpe", 
                "Annual Sharpe", "5Y Sharpe"
            ]
            
            # 打印表头
            header_format = "{:<10} {:<12} {:<10} {:<12} {:<10} {:<10} {:<10} {:<20} {:<12} {:<12} {:<12} {:<12} {:<12} {:<12} {:<12} {:<12} {:<12} {:<12} {:<12} {:<12}"
            print(header_format.format(*headers))
            print("-" * 240)
            print("Note: Monthly returns use arithmetic mean; Annual and 5Y returns use geometric mean for accurate compound effect")
            print()
            
            # 辅助函数：格式化数值
            def format_value(value, is_percent=False, is_ratio=False):
                if value is None:
                    return "N/A"
                if is_percent:
                    return f"{value:.2f}%"
                if is_ratio:
                    return f"{value:.2f}"
                return f"{value:.2f}"
            
            # 打印每个指数的数据
            indices = [
                ("S&P 500", stock_data['US_SP500'], us_metrics),
                ("CSI 300", stock_data['CN_HS300'], cn_metrics),
                ("Hang Seng", stock_data['HK_HSI'], hk_metrics)
            ]
            
            for name, data, metrics in indices:
                if not data.empty and metrics:
                    row = [
                        name,
                        metrics['current_data']['current_date'],
                        format_value(metrics['current_data']['close']),
                        f"{metrics['current_data']['volume']:,.0f}",
                        format_value(metrics['change_overtime']['MoM (%)'], is_percent=True),
                        format_value(metrics['change_overtime']['YoY (%)'], is_percent=True),
                        format_value(metrics['change_overtime']['5_year_average']),
                        metrics['change_overtime']['5_year_average_dates'] or "N/A",
                        format_value(metrics['volatility']['monthly_volatility'] * 100, is_percent=True),
                        format_value(metrics['volatility']['semi_annual_volatility'] * 100, is_percent=True),
                        format_value(metrics['volatility']['annual_volatility'] * 100, is_percent=True),
                        format_value(metrics['volatility']['5_year_volatility'] * 100, is_percent=True),
                        format_value(metrics['annualized_return']['monthly_annualized_return'], is_percent=True),
                        format_value(metrics['annualized_return']['semi_annual_annualized_return'], is_percent=True),
                        format_value(metrics['annualized_return']['annual_annualized_return'], is_percent=True),
                        format_value(metrics['annualized_return']['5_year_annualized_return'], is_percent=True),
                        format_value(metrics['sharpe_ratio']['monthly_sharpe_ratio'], is_ratio=True),
                        format_value(metrics['sharpe_ratio']['semi_annual_sharpe_ratio'], is_ratio=True),
                        format_value(metrics['sharpe_ratio']['annual_sharpe_ratio'], is_ratio=True),
                        format_value(metrics['sharpe_ratio']['5_year_sharpe_ratio'], is_ratio=True)
                    ]
                    print(header_format.format(*row))
        else:
            logger.error("未能获取股票指数数据")
            
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    # 可以通过修改这里的参数来调整数据范围和调试模式
    main(time_range=2920, debug=True)  # 2920 days ≈ 8 years 