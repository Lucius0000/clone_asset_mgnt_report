import akshare as ak
import pandas as pd
from datetime import datetime

# 映射名称到 symbol
us_name_to_symbol = {
    "SPY": "SPY", "QQQ": "QQQ", "AMZN": "AMZN", "GOOG": "GOOG", "AAPL": "AAPL", "MSFT": "MSFT", "META": "META", "TSLA": "TSLA", "NVDA": "NVDA",
    "TSM": "TSM", "AVGO": "AVGO", "AMD": "AMD", "INTC": "INTC", "QCOM": "QCOM",
    "SNOW": "SNOW", "ORCL": "ORCL", "MDB": "MDB", "PLTR": "PLTR", "DDOG": "DDOG",
    "CRM": "CRM", "APP": "APP", "ADBE": "ADBE", "NOW": "NOW", "WDAY": "WDAY"
}

hk_name_to_symbol = {
    "腾讯控股": "00700", "阿里巴巴": "09988", "美团": "03690", "盈富基金": "02800", "中海油": "00883",
    "恒生科技ETF": "03033", "汇丰控股": "00005", "友邦保险": "01299", "新鸿基地产": "00016",
    "领展房产基金": "00823", "中电控股": "00002", "香港中华煤气": "00003",
    "中广核电力": "01816", "比亚迪H": "01211", "中芯国际H": "00981"
}

cn_name_to_symbol = {
    "贵州茅台": "600519", "工商银行": "601398", "比亚迪A": "002594", "中芯国际A": "688981"
}

# 通用映射函数
def map_names_to_symbols(names, mapping):
    return [mapping[name] for name in names if name in mapping]

def reverse_mapping(d):
    return {v: k for k, v in d.items()}

# 名称与 symbol 映射
names_us = list(us_name_to_symbol.keys())
names_hk = list(hk_name_to_symbol.keys())
names_cn = list(cn_name_to_symbol.keys())

symbols_us = map_names_to_symbols(names_us, us_name_to_symbol)
symbols_hk = map_names_to_symbols(names_hk, hk_name_to_symbol)
symbols_cn = map_names_to_symbols(names_cn, cn_name_to_symbol)

us_symbol_to_name = reverse_mapping(us_name_to_symbol)
hk_symbol_to_name = reverse_mapping(hk_name_to_symbol)
cn_symbol_to_name = reverse_mapping(cn_name_to_symbol)

# 获取美股最新数据
def get_latest_us_data(symbols):
    data = []
    for symbol in symbols:
        try:
            df = ak.stock_us_daily(symbol=symbol)
            latest = df.iloc[-1:].copy()
            latest["symbol"] = symbol
            latest["name"] = us_symbol_to_name[symbol]
            latest["date"] = pd.to_datetime(latest["date"])
            data.append(latest)
        except Exception as e:
            print(f"美股 {symbol} 获取失败: {e}")
    return pd.concat(data) if data else pd.DataFrame()

# 获取港股最新数据
def get_latest_hk_data(symbols):
    data = []
    for symbol in symbols:
        try:
            df = ak.stock_hk_daily(symbol=symbol)
            df.rename(columns={"close": "收盘价", "date": "日期"}, inplace=True)
            latest = df.iloc[-1:].copy()
            latest["symbol"] = symbol
            latest["name"] = hk_symbol_to_name[symbol]
            latest["日期"] = pd.to_datetime(latest["日期"])
            data.append(latest)
        except Exception as e:
            print(f"港股 {symbol} 获取失败: {e}")
    return pd.concat(data) if data else pd.DataFrame()

# 获取A股最新数据
def get_latest_cn_data(symbols):
    end_date = datetime.today()
    start_date = end_date - pd.Timedelta(days=7)
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")

    data = []
    for symbol in symbols:
        try:
            df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_str, end_date=end_str)
            df["symbol"] = symbol
            df["name"] = cn_symbol_to_name[symbol]
            df["日期"] = pd.to_datetime(df["日期"])
            df = df[df["日期"] >= start_date]
            data.append(df)
        except Exception as e:
            print(f"A股 {symbol} 获取失败: {e}")
    return pd.concat(data) if data else pd.DataFrame()


latest_us = get_latest_us_data(symbols_us)
latest_hk = get_latest_hk_data(symbols_hk)
latest_cn = get_latest_cn_data(symbols_cn)

with pd.ExcelWriter("output/stock_close_validation.xlsx") as writer:
    if not latest_us.empty:
        latest_us.to_excel(writer, sheet_name="US", index=False)
    if not latest_hk.empty:
        latest_hk.to_excel(writer, sheet_name="HK", index=False)
    if not latest_cn.empty:
        latest_cn.to_excel(writer, sheet_name="CN", index=False)

