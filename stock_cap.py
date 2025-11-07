'''
计算股票指数的总市值，由 asset_stock_index.py 调用
'''

import pandas as pd
import akshare as ak
import os
import requests
from io import StringIO
import time
import glob
import random

def get_hs300_cap():
    """
    获取沪深300总市值（将 hs300 与实时行情按 6 位证券代码合并）
    """
    def normalize_code(series: pd.Series) -> pd.Series:
        # 转字符串 → 去非数字 → 取末6位 → 不足补零
        return (
            series.astype(str)
                  .str.replace(r"\D", "", regex=True)  # 去掉非数字，如 ".SZ"、空格
                  .str[-6:]                             # 末6位
                  .str.zfill(6)                         # 补零到6位
        )

    # 读取本地表格
    file_path = "data/000300cons.xls"
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"未找到文件 {file_path}，请检查文件路径。")

    # 读取并确保成分代码为字符串
    hs300_df = pd.read_excel(file_path, dtype={"成份券代码Constituent Code": str})

    # 列校验
    if "成份券代码Constituent Code" not in hs300_df.columns:
        raise RuntimeError("Excel文件中未找到 '成份券代码Constituent Code'")

    # 标准化成分代码
    hs300_df["成份券代码Constituent Code"] = normalize_code(hs300_df["成份券代码Constituent Code"])

    # 获取A股实时行情
    spot_df = ak.stock_zh_a_spot_em()

    # 标准化行情代码列
    if "代码" not in spot_df.columns:
        raise RuntimeError("实时行情数据中未找到 '代码' 列")
    spot_df["代码"] = normalize_code(spot_df["代码"])

    # 合并
    merged = pd.merge(
        hs300_df,
        spot_df,
        left_on="成份券代码Constituent Code",
        right_on="代码",
        how="inner"
    )

    # 数值化总市值
    merged["总市值"] = pd.to_numeric(merged.get("总市值"), errors="coerce")

    # 汇总并格式化
    total_market_cap_billion = merged["总市值"].sum() / 1e9  # 元 -> 十亿元
    formatted_cap = f"{total_market_cap_billion:,.0f} B CNY"

    # 明细输出
    cols_exist = [c for c in ["品种名称", "代码", "总市值"] if c in merged.columns]
    result_df = merged[cols_exist].copy()
    if "总市值" in result_df.columns:
        result_df["总市值（B CNY）"] = (result_df["总市值"] / 1e9).map(lambda x: f"{x:,.2f} B CNY")
        result_df = result_df.sort_values(by="总市值", ascending=False)

    # 保存
    os.makedirs("output/raw_data", exist_ok=True)
    result_df.to_excel("output/raw_data/沪深300_成分股市值明细.xlsx", index=False)

    return formatted_cap

def get_hsi_cap():
    '''
    获取恒生指数总市值
    算法：求和每个成分股市值
    获取恒生指数成分股市值：https://www.aastocks.com/tc/stocks/market/index/hk-index-con.aspx?index=HSI
    需下载excel格式，放置在data文件夹下
    '''
    # 读取文件
    files = glob.glob(os.path.join("data", "AASTOCKS_Export*.xlsx"))
    if not files:
        raise FileNotFoundError("未找到匹配的 AASTOCKS_Export*.xlsx 文件")
    file_path = max(files, key=os.path.getmtime)
    df = pd.read_excel(file_path)

    # 清洗“市值”字段（如“1,838.42億” -> 1838.42）
    df["市值（亿港元）"] = (
        df["市值"]
        .astype(str)
        .str.replace("億", "", regex=False)
        .str.replace(",", "", regex=False)
        .astype(float)
    )

    # 计算市值（单位：十亿港元 B HKD）
    df["市值（B HKD）"] = df["市值（亿港元）"] / 10
    df["市值（B HKD）"] = df["市值（B HKD）"].map(lambda x: f"{x:,.2f} B HKD")

    total_b = df["市值（亿港元）"].sum() / 10
    formatted_total = f"{total_b:,.0f} B HKD"

    result_df = df[["名稱", "代號", "市值（B HKD）"]].sort_values(by="市值（B HKD）", ascending=False)

    # print(f"恒生指数总市值为：{formatted_total}")

    # 保存结果
    result_df.to_excel("output/raw_data/恒生指数成分股市值明细.xlsx", index=False)
    
    return formatted_total

def _validate_spot_df(df):
    """校验 ak.stock_us_spot() 的返回是否可用：非空且包含关键列"""
    try:
        cols = set(c.upper() for c in getattr(df, "columns", []))
        return (hasattr(df, "empty") and (not df.empty) and {"SYMBOL", "MKTCAP"}.issubset(cols))
    except Exception:
        return False

def _call_with_backoff(func, *args, max_retries=4, base_delay=2.0, jitter=(0.3, 1.2), **kwargs):
    """
    指数退避 + 抖动：对 func 进行最多 max_retries 次调用；
    校验失败或抛异常都会重试。等待时间：base_delay**attempt + random[jitter]
    """
    attempt = 0
    while True:
        try:
            res = func(*args, **kwargs)
            if _validate_spot_df(res):
                return res
            # 返回内容不符合预期，也当作一次失败以触发退避
            raise RuntimeError("Validation failed: empty result or missing columns.")
        except Exception as e:
            if attempt >= max_retries:
                # 重试耗尽，抛出最后一次异常
                raise
            sleep_s = (base_delay ** attempt) + random.uniform(*jitter)
            time.sleep(sleep_s)
            attempt += 1

def get_spy_cap(debug = False):
    """
    获取标普500市值信息，分别通过两个接口尝试并记录匹配情况与原始数据。
    @author: Lucius
    """

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
        matched_em_df.to_excel("output/raw_data/sp500_matched_market_data_em.xlsx", index=False)
        
        if debug:
            print(f"成功匹配 {len(matched_em_df)} 家标普500公司（spot_em）")
        final_df = matched_em_df.copy()
        matched_symbols_em = set(matched_em_df["代码_CLEAN"])
        unmatched_em = [s for s in symbols_upper if s not in matched_symbols_em]
    except Exception as e:
        print("stock_us_spot_em 接口失败：", e)
        unmatched_em = list(symbols_upper)


    # 第二接口：stock_us_spot
    matched_df = pd.DataFrame()
    try:
        if unmatched_em:

            time.sleep(20)
    
            # —— 关键改动：用带退避与校验的安全调用替换原来的直呼 ——
            spot_df = _call_with_backoff(
                ak.stock_us_spot,
                max_retries=4,         # 可按需要调大
                base_delay=2.0,        # 指数退避底数：2.0^0, 2.0^1, ...
                jitter=(0.3, 1.2)      # 抖动，避免“羊群效应”
            )
    
            spot_df.to_excel(os.path.join(raw_data_dir, "us_stock_us_spot.xlsx"), index=False)
    
            spot_df.columns = spot_df.columns.str.upper()
            spot_df["代码_CLEAN"] = spot_df["SYMBOL"].astype(str).str.upper().str.replace(r"^\d+\.", "", regex=True)
    
            matched_df = spot_df[spot_df["代码_CLEAN"].isin(unmatched_em)].copy()
            matched_df["MKT_CAP_PARSED"] = matched_df["MKTCAP"].apply(parse_market_cap)
            matched_df.to_excel("output/raw_data/sp500_matched_market_data.xlsx", index=False)
    
            if debug:
                print(f"补充匹配 {len(matched_df)} 家标普500公司（spot）")
    
            final_df = pd.concat([final_df, matched_df], ignore_index=True)
    except Exception as e:
        # 不中断主流程，落盘失败信息
        err_path = os.path.join(raw_data_dir, "sina_fallback_error.txt")
        with open(err_path, "w", encoding="utf-8") as f:
            f.write(f"调用 ak.stock_us_spot() 失败（已含退避重试）：{repr(e)}\n")
        if debug:
            print("stock_us_spot 接口失败，已跳过：", e)

    
    # 统一计算未匹配剩余
    matched_all = set(final_df["代码_CLEAN"]) if "代码_CLEAN" in final_df.columns else set()
    unmatched_final = [s for s in symbols_upper if s not in matched_all]
    
    # 校验：市值为 0 或空值
    def check_mktcap_issues(df, col_name, label):
        try:
            df = df.copy()
            df.columns = df.columns.str.upper()
            col_name_upper = col_name.upper()

            if col_name_upper not in df.columns:
                if debug:
                    print(f"\n{label} 中未找到市值列 {col_name_upper}，跳过检查。")
                null_stats[label] = -1
                return

            df[col_name_upper] = pd.to_numeric(df[col_name_upper], errors='coerce')
            invalid_df = df[(df[col_name_upper].isna()) | (df[col_name_upper] == 0)]
            count = len(invalid_df)

            null_stats[label] = count

            if count > 0:
                if debug:
                    print(f"\n{label} 中发现 {count} 条市值为 0 或缺失的记录")
                display_cols = ['SYMBOL'] if 'SYMBOL' in df.columns else df.columns[:2].tolist()
                if debug:
                    print(invalid_df[[col_name_upper] + display_cols].head())
                invalid_df.to_excel(f"output/raw_data/invalid_mktcap_{label}.xlsx", index=False)
            else:
                if debug:
                    print(f"\n{label} 中市值字段无 0 或缺失")

        except Exception as e:
            if debug:
                print(f"检查 {label} 市值时出错：{e}")
            null_stats[label] = -1

    if 'matched_em_df' in locals() and not matched_em_df.empty:
        check_mktcap_issues(matched_em_df, "总市值", "EM接口")
    if 'matched_df' in locals() and not matched_df.empty:
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
                if debug:
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
                mismatch_df.to_excel("output/raw_data/mismatch_market_cap.xlsx", index=False)
                if debug:
                    print(f"市值不一致的股票数量：{mismatch_stats['count']}")
                    print(f"正向差额总和（EM > SPOT）：{mismatch_stats['positive']:,.0f}")
                    print(f"负向差额总和（EM < SPOT）：{mismatch_stats['negative']:,.0f}")
            else:
                if debug:
                    print("两个接口中标普500公司市值一致（误差 < 1%）")

        except Exception as e:
            if debug:
                print(f"比较接口市值差异时出错：{e}")
            mismatch_stats["count"] = -1
            
    if debug and 'spot_em_df' in locals() and 'spot_df' in locals():
        compare_market_cap_between_interfaces(spot_em_df, spot_df)

    # 总市值估算
    total_market_cap = final_df["MKT_CAP_PARSED"].sum()
    if debug:
        print(f"\n标普500市值估算：{total_market_cap / 1e9:,.2f} B USD")

    final_df.to_excel("output/raw_data/merged_us_sp500_market_cap.xlsx", index=False)
    if debug:
        print("合并结果已保存至：output/raw_data/merged_us_sp500_market_cap.xlsx")

    summary_path = "output/raw_data/summary.txt"
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
    
    spy_cap = f'{total_market_cap / 1e9:,.0f} B USD'
    
    return spy_cap


def get_all_index_caps():
    """
    这是用于 asset_stock_index.py 调用的接口，不用于本代码输出，注意维护
    """
    cn = get_hs300_cap()
    us = get_spy_cap()
    hk = get_hsi_cap()
    return {"CN": cn, "HK": hk, "US": us}


def main():
    hs300_cap = get_hs300_cap()
    print(f'沪深300总市值:{hs300_cap}')
    hsi_cap = get_hsi_cap()
    print(f'恒生指数总市值：{hsi_cap}')
    spy_cap = get_spy_cap()
    print(f'SPY总市值:{spy_cap}')
    
if __name__ == '__main__':
    main()



