import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import os

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
        hk_cpi_yearly = ak.macro_china_hk_cpi_ratio()
        
        return {
            'US_monthly': us_cpi_monthly.tail(time_range),
            'US_yearly': us_cpi_yearly.tail(time_range),
            'US_pce_yearly': us_pce_yearly.tail(time_range),
            'CN_monthly': cn_cpi_monthly.tail(time_range),
            'CN_yearly': cn_cpi_yearly.tail(time_range),
            'HK_yearly': hk_cpi_yearly.tail(time_range)
        }   
        # return value example:
        #{'US_monthly':           商品          日期   今值  预测值   前值
            # 656  美国CPI月率  2024-09-11  0.2  0.2  0.2
            # 657  美国CPI月率  2024-10-10  0.2  0.1  0.2
            # 658  美国CPI月率  2024-11-13  0.2  0.2  0.2
            # 659  美国CPI月率  2024-12-11  0.3  0.3  0.2
            # 660  美国CPI月率  2025-01-15  0.4  0.4  0.3
            # 661  美国CPI月率  2025-02-12  0.5  0.3  0.4
            # 662  美国CPI月率  2025-03-12  0.2  0.3  0.5
            # 663  美国CPI月率  2025-04-10 -0.1  0.1  0.2
            # 664  美国CPI月率  2025-05-13  0.2  0.3 -0.1
            # 665  美国CPI月率  2025-06-11  NaN  NaN  0.2, 
        # 'US_yearly':              时间        发布日期   现值   前值
            # 199  2024-08-01  2024-09-11  2.5  2.9
            # 200  2024-09-01  2024-10-10  2.4  2.5
            # 201  2024-10-01  2024-11-13  2.6  2.4
            # 202  2024-11-01  2024-12-11  2.7  2.6
            # 203  2024-12-01  2025-01-15  2.9  2.7
            # 204  2025-01-01  2025-02-12  3.0  2.9
            # 205  2025-02-01  2025-03-12  2.8  3.0
            # 206  2025-03-01  2025-04-10  2.4  2.8
            # 207  2025-04-01  2025-05-13  2.3  2.4
            # 208  2025-05-01  2025-06-11  NaN  2.3, 
        # 'US_pce_yearly':                 商品          日期   今值  预测值   前值
            # 469  美国核心PCE物价指数年率  2009-02-01  1.2  NaN  1.4
            # 470  美国核心PCE物价指数年率  2009-03-01  1.2  NaN  1.2
            # 471  美国核心PCE物价指数年率  2009-04-01  1.1  NaN  1.2
            # 472  美国核心PCE物价指数年率  2009-05-01  1.2  NaN  1.1
            # 473  美国核心PCE物价指数年率  2009-06-01  1.2  NaN  1.2
            # ..             ...         ...  ...  ...  ...
            # 664  美国核心PCE物价指数年率  2025-02-28  2.6  2.6  2.9
            # 665  美国核心PCE物价指数年率  2025-03-28  2.8  2.7  2.7
            # 666  美国核心PCE物价指数年率  2025-04-30  2.6  2.6  3.0
            # 667  美国核心PCE物价指数年率  2025-05-30  2.5  2.5  2.7
            # 668  美国核心PCE物价指数年率  2025-06-27  NaN  NaN  2.5
        # 'CN_monthly':             商品          日期   今值  预测值   前值
            # 344  中国CPI月率报告  2024-10-13  0.0  0.4  0.4
            # 345  中国CPI月率报告  2024-11-09 -0.3  NaN  0.0
            # 346  中国CPI月率报告  2024-12-09 -0.6 -0.4 -0.3
            # 347  中国CPI月率报告  2025-01-09  0.0  0.0 -0.6
            # 348  中国CPI月率报告  2025-01-12  NaN  NaN -0.6
            # 349  中国CPI月率报告  2025-02-09  0.7  0.8  0.0
            # 350  中国CPI月率报告  2025-03-09 -0.2 -0.1  0.7
            # 351  中国CPI月率报告  2025-04-10 -0.4 -0.2 -0.2
            # 352  中国CPI月率报告  2025-05-10  0.1  NaN -0.4
            # 353  中国CPI月率报告  2025-06-09  NaN  NaN  0.1, 
        # 'CN_yearly':             商品          日期   今值  预测值   前值
            # 464  中国CPI年率报告  2024-10-13  0.4  0.6  0.6
            # 465  中国CPI年率报告  2024-11-09  0.3  0.3  0.4
            # 466  中国CPI年率报告  2024-12-09  0.2  0.5  0.3
            # 467  中国CPI年率报告  2025-01-09  0.1  0.1  0.2
            # 468  中国CPI年率报告  2025-01-12  NaN  NaN  0.2
            # 469  中国CPI年率报告  2025-02-09  0.5  0.4  0.1
            # 470  中国CPI年率报告  2025-03-09 -0.7 -0.4  0.5
            # 471  中国CPI年率报告  2025-04-10 -0.1  0.0 -0.7
            # 472  中国CPI年率报告  2025-05-10 -0.1 -0.1 -0.1
            # 473  中国CPI年率报告  2025-06-09  NaN  NaN -0.1, 
        # 'HK_yearly':            时间   前值   现值        发布日期
            # 162  2021年07月  0.7  3.7  2021-08-19
            # 163  2021年08月  3.7  1.6  2021-09-20
            # 164  2021年09月  1.6  1.4  2021-10-22
            # 165  2021年10月  1.4  1.7  2021-11-22
            # 166  2021年11月  1.7  1.8  2021-12-21
            # 167  2021年12月  1.8  2.4  2022-01-20
            # 168  2022年01月  2.4  1.2  2022-02-22
            # 169  2022年02月  1.2  1.6  2022-03-21
            # 170  2022年03月  1.6  1.7  2022-04-22
            # 171  2022年04月  1.7  1.3  2022-05-23}

    except Exception as e:
        logger.error(f"获取CPI数据时出错: {str(e)}")
        return None


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
        # return value example:
        # {'US_quarterly':                 商品          日期   今值  预测值   前值
            # 196  美国国内生产总值(GDP)  2024-08-29  3.0  2.8  1.4
            # 197  美国国内生产总值(GDP)  2024-09-26  3.0  3.0  1.6
            # 198  美国国内生产总值(GDP)  2024-10-30  2.8  3.0  3.0
            # 199  美国国内生产总值(GDP)  2024-11-27  2.8  2.8  3.0
            # 200  美国国内生产总值(GDP)  2024-12-19  3.1  2.8  3.0
            # 201  美国国内生产总值(GDP)  2025-01-30  2.3  2.7  3.1
            # 202  美国国内生产总值(GDP)  2025-02-27  2.3  2.3  3.1
            # 203  美国国内生产总值(GDP)  2025-03-27  2.4  2.3  3.1
            # 204  美国国内生产总值(GDP)  2025-04-30 -0.3  0.2  2.4
            # 205  美国国内生产总值(GDP)  2025-05-29 -0.2 -0.3  2.4, 
        # 'CN_quarterly':            商品          日期   今值  预测值   前值
            # 50  中国GDP年率报告  2023-01-17  2.9  1.8  3.9
            # 51  中国GDP年率报告  2023-04-18  4.5  4.0  2.9
            # 52  中国GDP年率报告  2023-07-17  6.3  7.3  4.5
            # 53  中国GDP年率报告  2023-10-18  4.9  4.4  6.3
            # 54  中国GDP年率报告  2024-01-17  5.2  5.3  4.9
            # 55  中国GDP年率报告  2024-04-16  5.3  4.8  5.2
            # 56  中国GDP年率报告  2024-07-15  4.7  5.1  5.3
            # 57  中国GDP年率报告  2024-10-18  4.6  4.6  4.7
            # 58  中国GDP年率报告  2025-01-17  5.4  5.0  4.6
            # 59  中国GDP年率报告  2025-04-16  5.4  5.2  5.4, 
        # 'HK_quarterly':           时间   前值   现值        发布日期
            # 60  2023第1季度 -2.9  5.0  2023-05-12
            # 61  2023第2季度  5.0  4.4  2023-08-11
            # 62  2023第3季度  4.4  6.8  2023-11-10
            # 63  2023第4季度  6.8  8.5  2024-02-28
            # 64  2024第1季度  8.5  6.7  2024-05-17
            # 65  2024第2季度  6.7  7.6  2024-08-16
            # 66  2024第3季度  7.6  6.3  2024-11-15
            # 67  2024第4季度  6.3  5.3  2025-02-26
            # 68  2025第1季度  5.3  4.3  2025-05-16
            # 69  2025第2季度  4.3  NaN  2025-08-15}
    except Exception as e:
        logger.error(f"获取GDP数据时出错: {str(e)}")
        return None

def get_interest_rate_data(time_range=10):
    """
    获取美国、中国、香港的利率数据
    返回: 包含各国利率数据的字典
    """
    try:
        # 美国利率数据
        us_rate_fed = ak.macro_bank_usa_interest_rate() # 8 times a year.
        # us_rate_interbank = ak.rate_interbank(market="香港银行同业拆借市场", symbol="Hibor美元", indicator="隔夜") # not available
        
        # # 中国利率数据
        cn_rate_fed = ak.macro_bank_china_interest_rate() # published every quarter.
        cn_rate_interbank_1d = ak.rate_interbank(market="中国银行同业拆借市场", symbol="Chibor人民币", indicator="隔夜") ## 2025.6.11 API 的 Chibor 和 Shibor 都只反馈Chibor..
        cn_rate_interbank_1m = ak.rate_interbank(market="中国银行同业拆借市场", symbol="Chibor人民币", indicator="1月") ## 2025.6.11 API 的 Chibor 和 Shibor 都只反馈Chibor..

        # # 香港利率数据
        # hk_rate_fed = none # not available
        hk_rate_interbank_1d = ak.rate_interbank(market="香港银行同业拆借市场", symbol="Hibor港币", indicator="隔夜") 
        hk_rate_interbank_1m = ak.rate_interbank(market="香港银行同业拆借市场", symbol="Hibor港币", indicator="1月") 
        hk_rate_interbank_1m_cny = ak.rate_interbank(market="香港银行同业拆借市场", symbol="Hibor人民币", indicator="1月") 
 
        # 返回字典，键是标的，值是DataFrame，包括后{time_range}期的数据
        return {
            'US_fed': us_rate_fed.tail(time_range),
            'CN_fed': cn_rate_fed.tail(time_range),
            'CN_interbank_1d': cn_rate_interbank_1d.tail(time_range),
            'CN_interbank_1m': cn_rate_interbank_1m.tail(time_range),
            'HK_interbank_1d': hk_rate_interbank_1d.tail(time_range),
            'HK_interbank_1m': hk_rate_interbank_1m.tail(time_range),
            'HK_interbank_1m_cny': hk_rate_interbank_1m_cny.tail(time_range)
        }
        # Return value example:
        # {'US_fed':             商品          日期    今值  预测值    前值                                                                   
        # 92   美联储利率决议报告  1992-07-03  3.25  NaN  3.75
        # 93   美联储利率决议报告  1992-09-05  3.00  NaN  3.25
        # 94   美联储利率决议报告  1994-02-05  3.25  NaN  3.00
        # 95   美联储利率决议报告  1994-03-23  3.50  NaN  3.25
        # 96   美联储利率决议报告  1994-04-19  3.75  NaN  3.50
        # ..         ...         ...   ...  ...   ...
        # 287  美联储利率决议报告  2025-01-30  4.50  4.5  4.50
        # 288  美联储利率决议报告  2025-03-20  4.50  4.5  4.50
        # 289  美联储利率决议报告  2025-05-08  4.50  4.5  4.50
        # 290  美联储利率决议报告  2025-06-19   NaN  NaN  4.50
        # 291  美联储利率决议报告  2025-07-31   NaN  NaN   NaN

        # [200 rows x 5 columns], 'CN_fed':            商品          日期    今值  预测值    前值
        # 18   中国央行决议报告  1992-11-01  8.64  NaN  8.64
        # 19   中国央行决议报告  1992-12-01  8.64  NaN  8.64
        # 20   中国央行决议报告  1993-01-01  8.64  NaN  8.64
        # 21   中国央行决议报告  1993-02-01  8.64  NaN  8.64
        # 22   中国央行决议报告  1993-03-01  8.64  NaN  8.64
        # ..        ...         ...   ...  ...   ...
        # 213  中国央行决议报告  2015-08-25  4.60  NaN  4.85
        # 214  中国央行决议报告  2015-10-23  4.35  NaN  4.60
        # 215  中国央行决议报告  2019-09-20  4.20  NaN  4.25
        # 216  中国央行决议报告  2019-10-21  4.20  NaN  4.20
        # 217  中国央行决议报告  2019-11-20  4.15  4.2  4.20

        # [200 rows x 5 columns], 'CN_interbank_1d':              报告日      利率    涨跌
        # 5045  2024-08-15  1.8118 -8.51
        # 5046  2024-08-16  1.7519 -5.99
        # 5047  2024-08-19  1.7411 -1.08
        # 5048  2024-08-20  1.7766  3.55
        # 5049  2024-08-21  1.7847  0.81
        # ...          ...     ...   ...
        # 5240  2025-06-04  1.4473 -0.22
        # 5241  2025-06-05  1.4430 -0.43
        # 5242  2025-06-06  1.4427 -0.03
        # 5243  2025-06-09  1.4097 -3.30
        # 5244  2025-06-10  1.3964 -1.33

        # [200 rows x 3 columns], 'CN_interbank_1m':              报告日      利率     涨跌
        # 4453  2024-08-15  1.8827   0.38
        # 4454  2024-08-16  2.0834  20.07
        # 4455  2024-08-19  2.1285   4.51
        # 4456  2024-08-20  2.0994  -2.91
        # 4457  2024-08-21  2.0541  -4.53
        # ...          ...     ...    ...
        # 4648  2025-06-04  1.7441 -11.74
        # 4649  2025-06-05  1.7401  -0.40
        # 4650  2025-06-06  1.7390  -0.11
        # 4651  2025-06-09  1.9150  17.60
        # 4652  2025-06-10  1.7009 -21.41

        # [200 rows x 3 columns], 'HK_interbank_1d':              报告日       利率      涨跌
        # 4582  2024-08-14  3.76560   2.655
        # 4583  2024-08-15  3.60298 -16.262
        # 4584  2024-08-16  3.64333   4.035
        # 4585  2024-08-19  3.59119  -5.214
        # 4586  2024-08-20  3.50893  -8.226
        # ...          ...      ...     ...
        # 4777  2025-06-04  0.01289  -0.738
        # 4778  2025-06-05  0.01949   0.660
        # 4779  2025-06-06  0.02182   0.233
        # 4780  2025-06-09  0.02000  -0.182
        # 4781  2025-06-10  0.02000   0.000

        # [200 rows x 3 columns], 'HK_interbank_1m':              报告日       利率      涨跌
        # 6929  2024-08-14  4.07833   2.833
        # 6930  2024-08-15  4.10369   2.536
        # 6931  2024-08-16  4.10000  -0.369
        # 6932  2024-08-19  4.08458  -1.542
        # 6933  2024-08-20  4.07452  -1.006
        # ...          ...      ...     ...
        # 7124  2025-06-04  0.76952 -10.471
        # 7125  2025-06-05  0.70000  -6.952
        # 7126  2025-06-06  0.63952  -6.048
        # 7127  2025-06-09  0.55982  -7.970
        # 7128  2025-06-10  0.54524  -1.458

        # [200 rows x 3 columns], 'HK_interbank_1m_cny':              报告日       利率      涨跌
        # 2798  2024-08-13  2.42955   5.076
        # 2799  2024-08-14  2.03242 -39.713
        # 2800  2024-08-15  1.98242  -5.000
        # 2801  2024-08-16  1.98333   0.091
        # 2802  2024-08-19  1.80348 -17.985
        # ...          ...      ...     ...
        # 2993  2025-06-03  1.73758  -0.090
        # 2994  2025-06-04  1.71000  -2.758
        # 2995  2025-06-05  1.78091   7.091
        # 2996  2025-06-06  1.76394  -1.697
        # 2997  2025-06-09  1.68515  -7.879
    except Exception as e:
        logger.error(f"获取利率数据时出错: {str(e)}")
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
        us_pce_yearly = cpi_data['US_pce_yearly']
        
        if debug:
            print("Original data:")
            print(us_pce_yearly.head())
        
        # Sort by date in descending order to get the latest record first
        us_pce_yearly = us_pce_yearly.sort_values('日期', ascending=False)
        
        # Get latest non-NaN values (most recent first)
        latest_pce = us_pce_yearly[us_pce_yearly['今值'].notna()].iloc[0]
        
        if debug:
            print(f"Latest PCE: {latest_pce['日期']} - {latest_pce['今值']}%")
        
        # Calculate 10-year CAGR
        cagr_10y, date_range = calculate_cpi_10y_cagr(us_pce_yearly, debug)
        
        results.append({
            'region': 'US_PCE',
            'yoy_value': latest_pce['今值'],
            'yoy_date': latest_pce['日期'],
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
        hk_yearly = cpi_data['HK_yearly']
        
        # Get latest non-NaN values (most recent first)
        latest_yearly = hk_yearly[hk_yearly['现值'].notna()].iloc[-1]
        
        if debug:
            print(f"Latest yearly: {latest_yearly['时间']} - {latest_yearly['现值']}%")
        
        # Calculate 10-year CAGR
        cagr_10y, date_range = calculate_cpi_10y_cagr(hk_yearly, debug)
        
        results.append({
            'region': 'HK',
            'mom_value': None,  # No monthly data available
            'mom_date': None,
            'yoy_value': latest_yearly['现值'],
            'yoy_date': latest_yearly['时间'],
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
            'date_range': date_range
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
            'date_range': date_range
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
            'date_range': date_range
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
            if is_fed:
                # For Fed rates, find the previous quarter's data
                target_date = current_date - pd.DateOffset(days=25)
                # Get all data points within 1 month of target date
                window_start = target_date - pd.DateOffset(months=1)
                window_end = target_date 
                mom_window = data[(data[date_col] >= window_start) & 
                                (data[date_col] <= window_end) & 
                                (data[value_col].notna())]
                mom_data = mom_window.iloc[0:1] if not mom_window.empty else pd.DataFrame()
            else:
                # For Interbank rates, find data from approximately 30 days ago
                target_date = current_date - pd.DateOffset(days=30)
                # Get all data points within 10 days of target date
                window_start = target_date - pd.DateOffset(days=10)
                window_end = target_date 
                mom_window = data[(data[date_col] >= window_start) & 
                                (data[date_col] <= window_end) & 
                                (data[value_col].notna())]
                mom_data = mom_window.iloc[0:1] if not mom_window.empty else pd.DataFrame()
            
            if not mom_data.empty and mom_data.iloc[0][value_col] != 0:  # Avoid division by zero
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
            
            if not five_year_data.empty and len(five_year_data) >= 8:  # Require at least 12 data points
                result['five_year_avg'] = five_year_data[value_col].mean()
                min_date = five_year_data[date_col].min()
                max_date = five_year_data[date_col].max()
                result['date_range'] = f"{min_date.strftime('%Y-%m')} to {max_date.strftime('%Y-%m')}"
                if debug:
                    print(f"\n5-year average calculation details:")
                    print(f"Date range: {result['date_range']}")
                    print(f"Number of data points: {len(five_year_data)}")
                    print("\nData points used:")
                    for _, row in five_year_data.iterrows():
                        print(f"{row[date_col]}: {row[value_col]}")
                    print(f"\n5-year average: {result['five_year_avg']}")
            else:
                if debug:
                    print("\nInsufficient data for 5-year average calculation")
                    print(f"Required: at least 8 data points")
                    print(f"Found: {len(five_year_data) if not five_year_data.empty else 0} data points")
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
    
    # Task 2: GDP分析
    print("\n2. GDP分析")
    print("-"*30)
    gdp_data = get_gdp_data(time_range)
    if (debug):
        print(gdp_data)
    gdp_metrics = calculate_gdp_metrics(gdp_data, debug)
    print(gdp_metrics)
    
    # Task 3: 利率分析
    print("\n3. 利率分析")
    print("-"*30)
    rate_data = get_interest_rate_data(time_range)
    if (debug):
        print(rate_data)
    rate_metrics = calculate_interest_rate_metrics(rate_data, debug)
    print(rate_metrics)
    
    output_path = 'output'
    os.makedirs(output_path, exist_ok=True)
    
    cpi_metrics.to_excel(f"{output_path}/cpi_metrics.xlsx", index=False)
    gdp_metrics.to_excel(f"{output_path}/gdp_metrics.xlsx", index=False)
    rate_metrics.to_excel(f"{output_path}/interest_rate_metrics.xlsx", index=False)

if __name__ == "__main__":
    try:
        generate_report(debug=False)
    except Exception as e:
        logger.error(f"生成报告时出错: {str(e)}")