'''
计算股票指数的总市值，由 asset_stock_index.py 调用
'''

import pandas as pd
import os
import requests
from io import StringIO
import time
import glob
import random
import yfinance as yf
from tqdm import tqdm
import logging
from datetime import datetime

os.environ['http_proxy'] = 'http://127.0.0.1:7890'
os.environ['https_proxy'] = 'http://127.0.0.1:7890'


# 日志初始化
os.makedirs("output/raw_data", exist_ok=True)
LOG_FILE = "output/raw_data/yf_marketcap_log.txt"
logging.basicConfig(
    filename=LOG_FILE,
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


def _safe_float(x, default=None):
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return default
        return float(x)
    except Exception:
        return default


def _retry(func, *args, max_retries=4, base_delay=1.5, jitter=(0.2, 0.8), **kwargs):
    """
    指数退避 + 抖动：对 func 进行最多 max_retries 次调用；
    抛异常或返回 None 都会重试。等待时间：base_delay**attempt + random[jitter]
    """
    attempt = 0
    while True:
        try:
            res = func(*args, **kwargs)
            if res is not None:
                return res
            raise RuntimeError("Empty result")
        except Exception:
            if attempt >= max_retries:
                raise
            sleep_s = (base_delay ** attempt) + random.uniform(*jitter)
            time.sleep(sleep_s)
            attempt += 1


def _yf_fetch_market_cap_with_method(ticker_symbol: str):
    """
    只用两种方法：
    1) get_info()['marketCap']
    2) price × shares（price=最近收盘；shares=优先 get_shares_full()，否则 get_info()['sharesOutstanding']）
    返回 (market_cap: float|None, method: str in {"get_info","calc","fail"})
    """
    t = yf.Ticker(ticker_symbol)

    # 1) 直接从 get_info 取 marketCap
    info = None
    try:
        info = t.get_info()  # 新版/旧版兼容
        mc = _safe_float((info or {}).get("marketCap"))
        if mc and mc > 0:
            return mc, "get_info"
    except Exception:
        pass

    # 2) 自行计算：收盘价 × 股本
    price = None
    shares = None

    # 收盘价
    try:
        hist = t.history(period="5d")
        if hist is not None and not hist.empty:
            price = _safe_float(hist["Close"].dropna().iloc[-1])
    except Exception:
        price = None

    # 股本优先 get_shares_full（若不可用再从 info 兜底）
    try:
        sf = t.get_shares_full()
        if sf is not None and not sf.empty:
            shares = _safe_float(sf.iloc[-1])
    except Exception:
        shares = None

    if shares is None and info is not None:
        shares = _safe_float(info.get("sharesOutstanding"))

    if price is not None and shares is not None and price > 0 and shares > 0:
        return price * shares, "calc"

    return None, "fail"


def _yf_market_caps_bulk(tickers, sleep_between=0.0, desc="Fetching"):
    """
    逐只调用 _yf_fetch_market_cap_with_method（为了稳妥与兼容），带进度条。
    返回 DataFrame：['代码','market_cap','method']
    """
    records = []
    for sym in tqdm(tickers, desc=desc):
        try:
            mc, mtd = _retry(_yf_fetch_market_cap_with_method, sym, max_retries=3)
        except Exception:
            mc, mtd = None, "fail"
        records.append({"代码": sym, "market_cap": mc, "method": mtd})
        if sleep_between > 0:
            time.sleep(sleep_between)
    return pd.DataFrame(records)


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

    # 映射到 yfinance 代码：'6' 开头 -> 上证 '.SS'； 其他常见（0/3）-> 深证 '.SZ'
    def cn_to_yf(code6: str) -> str:
        c = (code6 or "").strip()
        if c.startswith("6"):
            return f"{c}.SS"
        else:
            return f"{c}.SZ"

    hs300_df["代码"] = hs300_df["成份券代码Constituent Code"].map(cn_to_yf)

    # 用 yfinance 获取市值
    caps_df = _yf_market_caps_bulk(hs300_df["代码"].tolist(), sleep_between=0.0, desc="HS300")
    merged = pd.merge(
        hs300_df,
        caps_df,
        on="代码",
        how="inner"
    )

    # 数值化总市值
    merged["总市值"] = pd.to_numeric(merged.get("market_cap"), errors="coerce")

    # 汇总并格式化（CNY）
    total_market_cap_billion = merged["总市值"].sum() / 1e9  # 元 -> 十亿元
    formatted_cap = f"{total_market_cap_billion:,.0f} B CNY"

    # 明细输出（尽量复用原列名）
    cols_exist = [c for c in ["品种名称", "代码", "总市值"] if c in merged.columns]
    if not cols_exist:
        cols_exist = ["代码", "总市值"]
    result_df = merged[cols_exist].copy()
    if "总市值" in result_df.columns:
        result_df["总市值（B CNY）"] = (result_df["总市值"] / 1e9).map(lambda x: f"{x:,.2f} B CNY")
        result_df = result_df.sort_values(by="总市值", ascending=False)

    # 保存
    os.makedirs("output/raw_data", exist_ok=True)
    result_df.to_excel("output/raw_data/沪深300_成分股市值明细.xlsx", index=False)

    # —— 日志：记录方法统计 —— 
    method_counts = caps_df["method"].value_counts().to_dict()
    logging.info(f"HS300 method stats: {method_counts}")

    return formatted_cap


def get_hsi_cap():
    '''
    获取恒生指数总市值
    算法：求和每个成分股市值
    获取恒生指数成分股市值：https://www.aastocks.com/tc/stocks/market/index/hk-index-con.aspx?index=HSI
    需下载excel格式，放置在data文件夹下
    '''
    # 读取文件（仅作“成分股名录”，不再使用表内“市值”列）
    files = glob.glob(os.path.join("data", "AASTOCKS_Export*.xlsx"))
    if not files:
        raise FileNotFoundError("未找到匹配的 AASTOCKS_Export*.xlsx 文件")
    file_path = max(files, key=os.path.getmtime)
    df = pd.read_excel(file_path)

    # 代码列名兼容处理
    if "代號" not in df.columns and "代号" in df.columns:
        df.rename(columns={"代号": "代號"}, inplace=True)
    if "代號" not in df.columns:
        raise RuntimeError("Excel文件中未找到 '代號' 列")

    # —— 关键修复：若原表自带“市值”，先改名，避免合并后出现 _x/_y 后缀 —— 
    if "市值" in df.columns:
        df.rename(columns={"市值": "市值_AASTOCKS"}, inplace=True)

    # 代码转 yfinance：香港代码通常 4 位，左侧补零并加 .HK
    def hk_to_yf(code_raw) -> str:
        s = str(code_raw).strip()
        s = ''.join(ch for ch in s if ch.isdigit())
        s = s[-4:] if len(s) >= 4 else s.zfill(4)
        return f"{s}.HK"

    df["yf_code"] = df["代號"].map(hk_to_yf)

    # —— 从 yfinance 获取市值（HKD），并带进度条 —— 
    caps_df = _yf_market_caps_bulk(df["yf_code"].tolist(), sleep_between=0.0, desc="HSI")
    # yfinance 的结果列显式命名为 市值_yf，避免与原表冲突
    caps_df.rename(columns={"代码": "yf_code", "market_cap": "市值_yf"}, inplace=True)

    # 合并至原表（不使用原“市值_AASTOCKS”列）
    df = df.merge(caps_df, on="yf_code", how="left")

    # 计算市值（单位：十亿港元 B HKD）—— 只基于 yfinance 的 市值_yf
    df["市值_yf"] = pd.to_numeric(df["市值_yf"], errors="coerce")
    df["市值（B HKD）"] = (df["市值_yf"] / 1e9).map(lambda x: f"{x:,.2f} B HKD")

    total_b = df["市值_yf"].sum() / 1e9
    formatted_total = f"{total_b:,.0f} B HKD"

    # 与原列尽量兼容
    name_col = "名稱" if "名稱" in df.columns else ("名称" if "名称" in df.columns else None)
    cols = []
    if name_col:
        cols.append(name_col)
    cols.extend([c for c in ["代號", "市值（B HKD）"] if (c in df.columns) or (c == "市值（B HKD）")])
    result_df = df[cols].sort_values(by="市值（B HKD）", ascending=False)

    # 保存结果
    os.makedirs("output/raw_data", exist_ok=True)
    result_df.to_excel("output/raw_data/恒生指数成分股市值明细.xlsx", index=False)

    # —— 日志：记录方法统计 —— 
    method_counts = caps_df["method"].value_counts().to_dict()
    logging.info(f"HSI method stats: {method_counts}")

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

    —— 已改：市值改为从 yfinance 获取；保留原下载 constituents 与写盘逻辑 ——
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
    symbols_raw = sp500_df["SYMBOL"].astype(str).str.upper().str.strip().tolist()

    # yfinance 规范化：将 BRK.B -> BRK-B 这类点号改连字符
    def us_to_yf(sym: str) -> str:
        return sym.replace(".", "-")

    yf_syms = [us_to_yf(s) for s in symbols_raw]

    # —— 用 yfinance 获取所有成分市值（USD），带进度条 —— 
    caps_df = _yf_market_caps_bulk(yf_syms, sleep_between=0.0, desc="S&P 500")
    caps_df.rename(columns={"market_cap": "MKT_CAP_PARSED"}, inplace=True)
    # 写盘原始
    caps_df.to_excel(os.path.join(raw_data_dir, "sp500_yf_market_caps.xlsx"), index=False)

    # 匹配与落盘（尽量维持原字段与输出节奏）
    final_df = caps_df.copy()
    final_df["代码_CLEAN"] = final_df["代码"]
    matched_df = final_df.copy()

    # 统计未匹配（理论上 yfinance 都能匹配，仍按原逻辑保留）
    symbols_upper = set(yf_syms)
    matched_all = set(matched_df["代码_CLEAN"]) if "代码_CLEAN" in matched_df.columns else set()
    unmatched_final = [s for s in symbols_upper if s not in matched_all]

    # 校验：市值为 0 或空值（保留原统计）
    null_stats = {}
    def check_mktcap_issues(df, col_name, label):
        try:
            df = df.copy()
            df.columns = df.columns.str.upper()
            col_name_upper = col_name.upper()

            if col_name_upper not in df.columns:
                null_stats[label] = -1
                return

            df[col_name_upper] = pd.to_numeric(df[col_name_upper], errors='coerce')
            invalid_df = df[(df[col_name_upper].isna()) | (df[col_name_upper] == 0)]
            count = len(invalid_df)

            null_stats[label] = count

            if count > 0:
                invalid_df.to_excel(f"output/raw_data/invalid_mktcap_{label}.xlsx", index=False)

        except Exception:
            null_stats[label] = -1

    if 'matched_df' in locals() and not matched_df.empty:
        check_mktcap_issues(matched_df, "MKT_CAP_PARSED", "YF接口")

    # 总市值估算
    total_market_cap = pd.to_numeric(final_df["MKT_CAP_PARSED"], errors="coerce").sum()

    final_df.to_excel("output/raw_data/merged_us_sp500_market_cap.xlsx", index=False)

    summary_path = "output/raw_data/summary.txt"
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"标普500市值估算汇总\n")
        f.write(f"--------------------------\n")
        f.write(f"合并后总数：{len(final_df)}\n")
        f.write(f"总市值估算：{total_market_cap / 1e9:,.2f} B USD\n")
        f.write(f"最终仍未匹配数量：{len(unmatched_final)}\n")

        # 市值为 0 或缺失统计
        f.write("\n市值为 0 或缺失统计：\n")
        for label, count in null_stats.items():
            if count > 0:
                f.write(f" - {label}：{count} 条\n")
            elif count == 0:
                f.write(f" - {label}：无缺失\n")

    spy_cap = f'{total_market_cap / 1e9:,.0f} B USD'

    # —— 日志：记录方法统计 —— 
    method_counts = caps_df["method"].value_counts().to_dict()
    logging.info(f"S&P500 method stats: {method_counts}")

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
