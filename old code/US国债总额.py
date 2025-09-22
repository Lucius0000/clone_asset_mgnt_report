from fredapi import Fred
import os

fred = Fred(api_key=os.environ['FRED_API_KEY'])
series = fred.get_series('GFDEBTN')  # 单位：百万美元
latest_date = series.index.max().date()
latest_value = series.max()  # 最新值
print(f"截至 {latest_date}（季度末），美国联邦政府总债务为 {latest_value/1e3:.2f} 十亿美元")
