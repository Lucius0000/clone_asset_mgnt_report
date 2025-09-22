# -*- coding: utf-8 -*-
"""
获取标普500市值信息，分别通过两个接口尝试并记录匹配情况与原始数据。
@author: Lucius
"""

import pandas as pd
import akshare as ak
import os
import requests
from io import StringIO

# 初始化路径
sp500_url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
local_backup = "data/constituents.csv"
raw_data_dir = "output/raw_data"
os.makedirs("data", exist_ok=True)
os.makedirs(raw_data_dir, exist_ok=True)

# 获取标普500成分股列表
def get_sp500_df(url, backup_path):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        with open(backup_path, "w", encoding="utf-8") as f:
            f.write(response.text)
        return pd.read_csv(StringIO(response.text))
    except Exception:
        if os.path.exists(backup_path):
            return pd.read_csv(backup_path)
        else:
            raise RuntimeError("无法获取 S&P500 成分股列表")

# 加载并格式化 symbol 列表
sp500_df = get_sp500_df(sp500_url, local_backup)
sp500_df.columns = sp500_df.columns.str.upper()
symbols = sp500_df["SYMBOL"].astype(str).str.upper().str.strip().tolist()
symbols_upper = set(symbols)

# 初始化数据和差异列表
final_df = pd.DataFrame()
unmatched_em = []
unmatched_final = []
null_stats = {} 
mismatch_stats = {} 


# 市值字段处理函数
def parse_market_cap(val):
    try:
        return float(str(val).replace(",", ""))
    except:
        return None

# 第一接口：stock_us_spot_em
try:
    spot_em_df = ak.stock_us_spot_em()
    spot_em_df.to_excel(os.path.join(raw_data_dir, "us_stock_us_spot_em.xlsx"), index=False)

    spot_em_df.columns = spot_em_df.columns.str.upper()
    spot_em_df["代码_CLEAN"] = spot_em_df["代码"].astype(str).str.upper().str.replace(r"^\d+\.", "", regex=True)
    spot_em_df["SYMBOL"] = spot_em_df["代码_CLEAN"]

    matched_em_df = spot_em_df[spot_em_df["代码_CLEAN"].isin(symbols_upper)].copy()
    matched_em_df["MKT_CAP_PARSED"] = matched_em_df["总市值"].apply(parse_market_cap)
    matched_em_df.to_excel("output/sp500_matched_market_data_em.xlsx", index=False)

    print(f"成功匹配 {len(matched_em_df)} 家标普500公司（spot_em）")
    final_df = matched_em_df.copy()
    matched_symbols_em = set(matched_em_df["代码_CLEAN"])
    unmatched_em = [s for s in symbols_upper if s not in matched_symbols_em]
except Exception as e:
    print("stock_us_spot_em 接口失败：", e)
    unmatched_em = list(symbols_upper)

# 第二接口：stock_us_spot
if unmatched_em:
    spot_df = ak.stock_us_spot()
    spot_df.to_excel(os.path.join(raw_data_dir, "us_stock_us_spot.xlsx"), index=False)

    spot_df.columns = spot_df.columns.str.upper()
    spot_df["代码_CLEAN"] = spot_df["SYMBOL"].astype(str).str.upper().str.replace(r"^\d+\.", "", regex=True)

    matched_df = spot_df[spot_df["代码_CLEAN"].isin(unmatched_em)].copy()
    spot_df.columns = spot_df.columns.str.upper()
    matched_df["MKT_CAP_PARSED"] = matched_df["MKTCAP"].apply(parse_market_cap)
    matched_df.to_excel("output/sp500_matched_market_data.xlsx", index=False)

    print(f"补充匹配 {len(matched_df)} 家标普500公司（spot）")
    final_df = pd.concat([final_df, matched_df], ignore_index=True)
    matched_all = set(final_df["代码_CLEAN"])
    unmatched_final = [s for s in symbols_upper if s not in matched_all]

# 校验：市值为 0 或空值
def check_mktcap_issues(df, col_name, label):
    try:
        df = df.copy()
        df.columns = df.columns.str.upper()
        col_name_upper = col_name.upper()

        if col_name_upper not in df.columns:
            print(f"\n{label} 中未找到市值列 {col_name_upper}，跳过检查。")
            null_stats[label] = -1
            return

        df[col_name_upper] = pd.to_numeric(df[col_name_upper], errors='coerce')
        invalid_df = df[(df[col_name_upper].isna()) | (df[col_name_upper] == 0)]
        count = len(invalid_df)

        null_stats[label] = count

        if count > 0:
            print(f"\n{label} 中发现 {count} 条市值为 0 或缺失的记录")
            display_cols = ['SYMBOL'] if 'SYMBOL' in df.columns else df.columns[:2].tolist()
            print(invalid_df[[col_name_upper] + display_cols].head())
            invalid_df.to_excel(f"output/raw_data/invalid_mktcap_{label}.xlsx", index=False)
        else:
            print(f"\n{label} 中市值字段无 0 或缺失")

    except Exception as e:
        print(f"检查 {label} 市值时出错：{e}")
        null_stats[label] = -1

check_mktcap_issues(matched_em_df, "总市值", "EM接口")
check_mktcap_issues(matched_df, "MKTCAP", "SPOT接口")


# 检查两个接口的匹配数据在市值上的差异
def compare_market_cap_between_interfaces(df_em, df_spot):
    try:
        # 标准化字段名
        df_em.columns = df_em.columns.str.upper()
        df_spot.columns = df_spot.columns.str.upper()

        # 提取代码_CLEAN 字段
        df_em["代码_CLEAN"] = df_em["代码"].astype(str).str.upper().str.replace(r"^\d+\.", "", regex=True)
        df_spot["代码_CLEAN"] = df_spot["SYMBOL"].astype(str).str.upper().str.replace(r"^\d+\.", "", regex=True)

        # 限定为在标普500中的公司
        em_set = set(df_em["代码_CLEAN"]) & symbols_upper
        spot_set = set(df_spot["代码_CLEAN"]) & symbols_upper
        common_symbols = em_set & spot_set

        if not common_symbols:
            print("两个接口中无共同的标普500公司，跳过对比")
            mismatch_stats["count"] = 0
            return

        # 只保留交集
        df_em = df_em[df_em["代码_CLEAN"].isin(common_symbols)][["代码_CLEAN", "总市值"]].copy()
        df_spot = df_spot[df_spot["代码_CLEAN"].isin(common_symbols)][["代码_CLEAN", "MKTCAP"]].copy()

        df_em.columns = ['代码_CLEAN', 'EM_MKTCAP']
        df_spot.columns = ['代码_CLEAN', 'SPOT_MKTCAP']

        merged_df = pd.merge(df_em, df_spot, on='代码_CLEAN', how='inner')
        merged_df['EM_MKTCAP'] = pd.to_numeric(merged_df['EM_MKTCAP'], errors='coerce')
        merged_df['SPOT_MKTCAP'] = pd.to_numeric(merged_df['SPOT_MKTCAP'], errors='coerce')

        # 差异计算
        merged_df['ABS_DIFF'] = merged_df['EM_MKTCAP'] - merged_df['SPOT_MKTCAP']
        merged_df['REL_DIFF'] = abs(merged_df['ABS_DIFF']) / merged_df[['EM_MKTCAP', 'SPOT_MKTCAP']].max(axis=1)
        merged_df['IS_MATCH'] = merged_df['REL_DIFF'] < 0.01

        mismatch_df = merged_df[~merged_df['IS_MATCH']].copy()

        # 统计
        mismatch_stats["count"] = len(mismatch_df)
        mismatch_stats["positive"] = mismatch_df[mismatch_df['ABS_DIFF'] > 0]['ABS_DIFF'].sum()
        mismatch_stats["negative"] = mismatch_df[mismatch_df['ABS_DIFF'] < 0]['ABS_DIFF'].sum()

        if not mismatch_df.empty:
            mismatch_df.to_excel("output/mismatch_market_cap.xlsx", index=False)
            print(f"市值不一致的股票数量：{mismatch_stats['count']}")
            print(f"正向差额总和（EM > SPOT）：{mismatch_stats['positive']:,.0f}")
            print(f"负向差额总和（EM < SPOT）：{mismatch_stats['negative']:,.0f}")
        else:
            print("两个接口中标普500公司市值一致（误差 < 1%）")

    except Exception as e:
        print(f"比较接口市值差异时出错：{e}")
        mismatch_stats["count"] = -1
        
compare_market_cap_between_interfaces(spot_em_df, spot_df)


print(f"\n首次未匹配（spot_em）：{len(unmatched_em)}，最终未匹配：{len(unmatched_final)}")

# 总市值估算
total_market_cap = final_df["MKT_CAP_PARSED"].sum()
print(f"\n标普500市值估算：{total_market_cap / 1e9:,.2f} B USD")

final_df.to_excel("output/merged_us_sp500_market_cap.xlsx", index=False)
print("合并结果已保存至：output/merged_us_sp500_market_cap.xlsx")

summary_path = "output/summary.txt"
with open(summary_path, "w", encoding="utf-8") as f:
    f.write(f"标普500市值估算汇总\n")
    f.write(f"--------------------------\n")
    f.write(f"接口一（spot_em）匹配数：{len(matched_em_df) if 'matched_em_df' in locals() else 0}\n")
    f.write(f"接口二（spot）补充匹配数：{len(matched_df) if 'matched_df' in locals() else 0}\n")
    f.write(f"合并后总数（去重可能有误差）：{len(final_df)}\n")
    f.write(f"总市值估算：{total_market_cap / 1e9:,.2f} B USD\n")
    f.write(f"首次未匹配数量：{len(unmatched_em)}\n")
    f.write(f"最终仍未匹配数量：{len(unmatched_final)}\n")

    # 市值为 0 或缺失统计
    f.write("\n市值为 0 或缺失统计：\n")
    for label, count in null_stats.items():
        if count > 0:
            f.write(f" - {label}：{count} 条\n")
        elif count == 0:
            f.write(f" - {label}：无缺失\n")

    # 市值差异统计
    if "count" in mismatch_stats:
        if mismatch_stats["count"] > 0:
            f.write(f"\n市值不一致记录数：{mismatch_stats['count']}\n")
            f.write(f"   - 正向差额：{mismatch_stats['positive']:,.0f}\n")
            f.write(f"   - 负向差额：{mismatch_stats['negative']:,.0f}\n")
        elif mismatch_stats["count"] == 0:
            f.write("\n接口市值完全一致\n")


print(f"\n市值汇总已写入 {summary_path}")
