import yfinance as yf
import tushare as ts
import pandas as pd

# 获取 SPY ETF 信息
spy = yf.Ticker("SPY")
spy_info = spy.info
spy_mc = spy_info.get("marketCap")
spy_pe = spy_info.get("trailingPE")

# 沪深300估值（来自Tushare）
pro = ts.pro_api("你的TUSHARE_TOKEN")
hs300 = pro.index_dailybasic(ts_code="399300.SZ", fields="trade_date,total_mv,pe_ttm", 
                             start_date="20250714", end_date="20250714")
if not hs300.empty:
    hs300_mc = hs300.iloc[0]["total_mv"] * 1e8  # 转为元
    hs300_pe = hs300.iloc[0]["pe_ttm"]
else:
    hs300_mc = None
    hs300_pe = None

# 恒生指数估值（暂以假设值填充）
hsi_mc = 'HK$ ~30 trillion'
hsi_pe = 11.5

# 整理输出
data = [
    {"指数": "SPY (标普500 ETF)", "总市值": spy_mc, "市盈率": spy_pe},
    {"指数": "沪深300", "总市值": f"{hs300_mc:.0f} 元" if hs300_mc else None, "市盈率": hs300_pe},
    {"指数": "恒生指数", "总市值": hsi_mc, "市盈率": hsi_pe}
]

df = pd.DataFrame(data)
df.to_excel("index_valuation_fixed.xlsx", index=False)
print("数据已保存至 index_valuation_fixed.xlsx")
