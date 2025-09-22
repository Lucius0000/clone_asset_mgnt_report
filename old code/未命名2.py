import tushare as ts
pro = ts.pro_api('你的Tushare Token')
df = pro.index_dailybasic(ts_code='399300.SZ', fields='trade_date, total_mv, pe_ttm', start_date='20250701', end_date='20250714')
