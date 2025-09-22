import akshare as ak
import pandas as pd

'''
获取沪深300成分股列表
       品种代码  品种名称        纳入日期        代码
0    302132  中航成飞  2025-06-16  sz302132
1    001391   国货航  2025-06-16  sz001391
2    688047  龙芯中科  2025-06-16  sh688047
3    002600  领益智造  2025-06-16  sz002600
4    601077  渝农商行  2025-06-16  sh601077
'''
hs300_df = ak.index_stock_cons(symbol="000300")

''' 获取行情， Index(['序号', '代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额', '振幅', '最高', '最低',
'今开', '昨收', '量比', '换手率', '市盈率-动态', '市净率', '总市值', '流通市值', '涨速', '5分钟涨跌',
'60日涨跌幅', '年初至今涨跌幅']
'''
spot_df = ak.stock_zh_a_spot_em()

merged = pd.merge(hs300_df, spot_df, left_on="品种代码", right_on="代码")
merged["总市值"] = pd.to_numeric(merged["总市值"], errors="coerce")

# 适配输出格式
total_market_cap_billion = merged["总市值"].sum() / 1e9  # 元 -> 十亿元
formatted_cap = f"{total_market_cap_billion:,.0f}B CNY"

result_df = merged[["品种名称", "代码", "总市值"]].copy()
result_df["总市值（B CNY）"] = result_df["总市值"] / 1e9
result_df["总市值（B CNY）"] = result_df["总市值（B CNY）"].map(lambda x: f"{x:,.2f}B CNY")
result_df = result_df.sort_values(by="总市值", ascending=False)

# print(result_df.head(10))
# print(f"沪深300总市值为：{formatted_cap}")
result_df.to_excel("output/沪深300_成分股市值明细.xlsx", index=False)

