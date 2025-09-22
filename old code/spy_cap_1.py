import pandas as pd
import akshare as ak
import os
import requests
from io import StringIO

# 1. 下载标普500成分股列表（带异常处理 + 本地缓存）
url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
local_backup = "data/constituents.csv"
os.makedirs("data", exist_ok=True)

def get_sp500_df(url, backup_path):
    try:
        print("尝试从网络读取 S&P500 成分股列表...")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        with open(backup_path, "w", encoding="utf-8") as f:
            f.write(response.text)
        print("网络读取成功，已保存本地备份。")
        return pd.read_csv(StringIO(response.text))
    except Exception as e:
        print(f"网络读取失败：{e}")
        if os.path.exists(backup_path):
            print("尝试使用本地备份文件。")
            return pd.read_csv(backup_path)
        else:
            raise RuntimeError("无法获取 S&P500 成分股列表，也未找到本地备份。")

sp500_df = get_sp500_df(url, local_backup)
symbols = sp500_df["Symbol"].tolist()

# 2. 获取全量美股行情
spot_df = ak.stock_us_spot()
spot_df.to_excel('output/us_stock_us_spot.xlsx', index=False)

# 标准化列名，防止大小写影响
spot_df.columns = [col.lower() for col in spot_df.columns]
spot_df.loc[:,'symbol'] = spot_df['symbol'].str.upper()

# 3. 提取匹配的symbol数据
filtered_df = spot_df[spot_df['symbol'].isin(symbols)]

# 4. 转换市值为数值
def parse_market_cap(mktcap):
    try:
        if isinstance(mktcap, str):
            mktcap = mktcap.replace(",", "")
        return float(mktcap)
    except:
        return None

filtered_df["mktcap"] = filtered_df["mktcap"].apply(parse_market_cap)
total_market_cap = filtered_df["mktcap"].sum()

# 5. 转换为十亿美元（Billion USD）
total_market_cap_billion = total_market_cap / 1e9
print(f"标普500总市值估算为: {total_market_cap_billion:,.2f} Billion USD")

# 6. 记录未匹配 symbol
matched_symbols = set(filtered_df["symbol"])
unmatched = [s for s in symbols if s not in matched_symbols]
if unmatched:
    with open("output/failed_symbols.txt", "w") as f:
        f.write("\n".join(unmatched))
    print(f"共 {len(unmatched)} 个 symbol 未匹配行情，已记录到 failed_symbols.txt")
