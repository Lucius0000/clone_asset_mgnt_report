# -*- coding: utf-8 -*-
"""
Index free-float market cap (approx) at two dates with Gainer.
- Indices: HS300 (CNY), S&P 500 (USD), HSI (HKD)
- Price: yfinance Close (未复权收盘价)
- Shares: prefer floatShares; fallback to sharesOutstanding / impliedSharesOutstanding
- Constituents:
    * HS300 via ak.index_stock_cons("000300"); '品种代码' -> try ".SS"/".SZ"
    * S&P500 via datahub CSV (fallback to local data/constituents.csv)
    * HSI via latest data/AASTOCKS_Export*.xlsx (both dates use the same file)
- Features: logging, retry with backoff+jitter, tqdm progress bar
- NEW: save per-ticker details (close, float shares, market cap) into output/raw_data
"""

import os
os.environ.setdefault("PYTHONWARNINGS", "ignore")

import re
import sys
import time
import glob
import math
import json
import random
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings("ignore")

import io
import pandas as pd
import requests

try:
    from tqdm import tqdm
except Exception:
    def tqdm(x, **k):  # minimal fallback
        return x

import yfinance as yf
import akshare as ak

# 如需代理，请保留；否则可注释掉
os.environ['http_proxy'] = 'http://127.0.0.1:7890'
os.environ['https_proxy'] = 'http://127.0.0.1:7890'

# 目录
DATA_DIR = "data"
RAW_DIR = os.path.join("output", "raw_data")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)

# 日志
LOG_PATH = os.path.join(RAW_DIR, "index_freefloat_cap.log")
logger = logging.getLogger("index_cap")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
fh.setFormatter(fmt)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(fmt)
logger.handlers = [fh, ch]


# retry 装饰函数: 实现重试机制。
# 若函数调用失败，会按照指数回退（exponential backoff）策略重新尝试，最多重试4次。
def retry(max_retries=4, base_delay=1.5, jitter=(0.2, 0.9), exceptions=(Exception,)):
    """Decorator: exponential backoff with jitter."""
    def deco(fn):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as e:
                    if attempt >= max_retries:
                        logger.error(f"{fn.__name__} failed after {max_retries} retries: {repr(e)}")
                        raise
                    sleep_s = (base_delay ** attempt) + random.uniform(*jitter)
                    logger.warning(f"{fn.__name__} error: {repr(e)}; retry {attempt+1}/{max_retries} after {sleep_s:.2f}s")
                    time.sleep(sleep_s)
        return wrapper
    return deco


def _parse_date(d: str) -> datetime:
    return datetime.strptime(d, "%Y-%m-%d")


def _fmt_billions(x: float, unit: str) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return f"NaN {unit}"
    return f"{x/1e9:,.2f} B {unit}"


@retry()
def _yf_download_close(ticker: str, target_date: str) -> Optional[float]:
    """
    Download Close around target_date and return the last available Close <= target_date.
    If no data at all in small window, return None.
    """
    d = _parse_date(target_date)
    start = (d - timedelta(days=7)).strftime("%Y-%m-%d")
    end = (d + timedelta(days=3)).strftime("%Y-%m-%d")
    df = yf.download(ticker, start=start, end=end, auto_adjust=False, progress=False, threads=False)
    if df is None or df.empty:
        return None
    df = df.reset_index()
    df["Date_only"] = pd.to_datetime(df["Date"]).dt.date
    tgt = d.date()
    df = df[df["Date_only"] <= tgt]
    if df.empty:
        return None
    close = float(df.iloc[-1]["Close"])
    if math.isnan(close) or close <= 0:
        return None
    return close


@retry()
def _yf_fetch_shares(ticker: str) -> Optional[int]:
    """
    Try to fetch float shares; fallback to shares outstanding.
    We'll attempt: Ticker.fast_info and Ticker.get_info (legacy).
    """
    t = yf.Ticker(ticker)

    shares_candidates = []
    try:
        fi = t.fast_info
        for key in ["shares_float", "float_shares", "shares_outstanding", "implied_shares_outstanding"]:
            val = getattr(fi, key, None)
            if val is not None and float(val) > 0:
                shares_candidates.append(int(val))
    except Exception:
        pass

    try:
        info = t.get_info()
        for key in ["floatShares", "sharesOutstanding", "impliedSharesOutstanding"]:
            val = info.get(key)
            if val is not None and float(val) > 0:
                shares_candidates.append(int(val))
    except Exception:
        pass

    if not shares_candidates:
        return None
    # 简化：返回第一个候选（通常是 float），否则取最大值兜底
    return int(shares_candidates[0]) if shares_candidates else None


def _nearest_cap_with_details(ticker: str, dates: Tuple[str, str]) -> Dict[str, object]:
    """
    Return detailed components for cap: shares + close + cap for each date.
    {
      'shares': <int or None>,
      DATE_OLD: {'close': <float or None>, 'cap': <float or None>},
      DATE_NEW: {'close': <float or None>, 'cap': <float or None>}
    }
    """
    s_est = _yf_fetch_shares(ticker)
    out = {"shares": s_est}
    for dt in dates:
        px = _yf_download_close(ticker, dt)
        cap = float(px) * float(s_est) if (s_est is not None and px is not None) else None
        out[dt] = {"close": px, "cap": cap}
    return out


# 获取三个股指的成分股名录
@retry()
def get_sp500_symbols() -> List[str]:
    """
    S&P 500 constituents via datahub (fallback to local CSV),
    then convert to yfinance tickers (BRK.B -> BRK-B)
    """
    url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
    local = os.path.join(DATA_DIR, "constituents.csv")
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        with open(local, "w", encoding="utf-8") as f:
            f.write(r.text)
        df = pd.read_csv(io.StringIO(r.text))
    except Exception:
        df = pd.read_csv(local)
    df.columns = [c.upper() for c in df.columns]
    syms = df["SYMBOL"].astype(str).str.upper().str.strip().tolist()
    syms = [s.replace(".", "-") for s in syms]  # e.g., BRK.B -> BRK-B
    return syms


def _hk_fix_code_like_aastocks(val: str) -> str:
    """
    AASTOCKS '代號' often like '01211.HK' (5 digits with leading zero).
    Requirement: drop the first digit -> '1211.HK'.
    Also robust to values without '.HK'.
    """
    s = str(val).strip().upper()
    if not s:
        return s
    if ".HK" in s:
        left, _, _ = s.partition(".HK")
        left_digits = re.sub(r"\D", "", left)
        if len(left_digits) == 5:
            left_digits = left_digits[1:]
        elif len(left_digits) <= 4:
            left_digits = left_digits.zfill(4)
        return f"{left_digits}.HK"
    else:
        digits = re.sub(r"\D", "", s)
        if len(digits) == 5:
            digits = digits[1:]
        elif len(digits) <= 4:
            digits = digits.zfill(4)
        return f"{digits}.HK"


def get_hsi_symbols_from_excel() -> List[str]:
    """
    Read latest AASTOCKS_Export*.xlsx from data/ and map to yfinance .HK tickers.
    Both dates use the same file as per requirement.
    """
    files = glob.glob(os.path.join(DATA_DIR, "AASTOCKS_Export*.xlsx"))
    if not files:
        raise FileNotFoundError("未找到匹配的 data/AASTOCKS_Export*.xlsx，请先下载恒指成分股Excel。")
    file_path = max(files, key=os.path.getmtime)
    df = pd.read_excel(file_path)

    code_col = None
    for c in df.columns:
        if str(c).strip() in ["代號", "代码", "代号", "Code", "code", "Symbol"]:
            code_col = c
            break
    if code_col is None:
        raise RuntimeError("恒指Excel中未找到‘代號/代码’列。")

    syms = [_hk_fix_code_like_aastocks(x) for x in df[code_col].astype(str).tolist()]
    return syms

def get_hs300_symbols() -> List[str]:
    """
    从本地Excel文件加载沪深300成分股，根据"成份券代码"和"交易所"列生成后缀。
    保证成份券代码保持完整，保留前导零。
    """
    # 读取本地表格
    file_path = os.path.join(DATA_DIR, "000300cons.xls")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"未找到文件 {file_path}，请检查文件路径。")
    
    df = pd.read_excel(file_path, dtype={"成份券代码Constituent Code": str})

    # 检查表格是否包含需要的列
    if "成份券代码Constituent Code" not in df.columns or "交易所Exchange" not in df.columns:
        raise RuntimeError("Excel文件中未找到 '成份券代码Constituent Code' 或 '交易所Exchange' 列。")

    # 根据“成份券代码”和“交易所Exchange”列生成股票代码
    symbols = []
    for _, row in df.iterrows():
        code = row["成份券代码Constituent Code"].strip()  # 保证去掉多余的空格
        exchange = str(row["交易所Exchange"]).strip()

        # 根据交易所确定后缀
        if exchange == "深圳证券交易所":
            suffix = ".SZ"
        elif exchange == "上海证券交易所":
            suffix = ".SS"
        else:
            logger.warning(f"无法识别的交易所：{exchange}，跳过 {code}")
            continue

        # 生成完整的股票代码
        full_code = f"{code}{suffix}"
        symbols.append(full_code)

    return symbols

# 市值计算：接受成分股列表和两个日期，遍历每只股票，获取其在这两个日期的收盘价和流通股数，计算市值。
# 重试机制：如果某只股票数据获取失败，会记录并进行二次尝试。
def compute_index_caps(symbols: List[str], date_old: str, date_new: str, unit: str, name: str) -> Tuple[Optional[float], Optional[float]]:
    """
    For a list of yfinance symbols, compute total free-float cap at two dates.
    Adds a second-pass batch retry for tickers that failed in the first pass.
    Saves:
      1) missing_{name}_{date_old}_vs_{date_new}.csv
      2) per_ticker_details_{name}_{date_old}_vs_{date_new}.csv
    """
    total_old = 0.0
    total_new = 0.0
    miss_old = miss_new = 0

    missing: Dict[str, Dict[str, int]] = {}
    # 用 dict 聚合，便于二次重试后覆盖更新
    rows_map: Dict[str, Dict[str, object]] = {}

    # -------- 首轮遍历 --------
    for sym in tqdm(symbols, desc=f"{name} 成分股估算", ncols=90):
        try:
            det = _nearest_cap_with_details(sym, (date_old, date_new))
            co = det[date_old]["cap"]
            cn = det[date_new]["cap"]

            # 汇总
            if co is not None:
                total_old += co
            else:
                miss_old += 1
            if cn is not None:
                total_new += cn
            else:
                miss_new += 1

            # 记录明细
            rows_map[sym] = {
                "index": name,
                "unit": unit,
                "ticker": sym,
                "shares": det.get("shares"),
                "close_old": det[date_old]["close"],
                "cap_old": co,
                "close_new": det[date_new]["close"],
                "cap_new": cn,
                "missing_old": int(co is None),
                "missing_new": int(cn is None),
                "retried": 0,
                "recovered_old": 0,
                "recovered_new": 0,
            }

            # 缺失清单
            if (co is None) or (cn is None):
                missing[sym] = {
                    "missing_old": int(co is None),
                    "missing_new": int(cn is None),
                    "retried": 0,
                    "recovered_old": 0,
                    "recovered_new": 0,
                }

        except Exception as e:
            logger.warning(f"{name} {sym}: 计算失败 {repr(e)}")
            rows_map[sym] = {
                "index": name,
                "unit": unit,
                "ticker": sym,
                "shares": None,
                "close_old": None,
                "cap_old": None,
                "close_new": None,
                "cap_new": None,
                "missing_old": 1,
                "missing_new": 1,
                "retried": 0,
                "recovered_old": 0,
                "recovered_new": 0,
                "error": repr(e),
            }
            missing[sym] = {
                "missing_old": 1,
                "missing_new": 1,
                "retried": 0,
                "recovered_old": 0,
                "recovered_new": 0,
            }

    # -------- 二次批量重试（仅对首轮失败个股）--------
    failed = list(missing.keys())
    if failed:
        logger.info(f"{name}: 开始二次尝试获取首轮失败的个股，共 {len(failed)} 只 ...")
        time.sleep(2.0)  # 微间隔，减少限频

        rec_old = rec_new = 0
        for sym in tqdm(failed, desc=f"{name} 二次尝试", ncols=90):
            missing[sym]["retried"] = 1
            rows_map[sym]["retried"] = 1
            try:
                det2 = _nearest_cap_with_details(sym, (date_old, date_new))
                co2 = det2[date_old]["cap"]
                cn2 = det2[date_new]["cap"]
            except Exception as e:
                logger.warning(f"{name} {sym}: 二次尝试失败 {repr(e)}")
                continue

            # 旧日恢复
            if missing[sym]["missing_old"] == 1 and co2 is not None:
                total_old += co2
                missing[sym]["missing_old"] = 0
                missing[sym]["recovered_old"] = 1
                rows_map[sym]["missing_old"] = 0
                rows_map[sym]["recovered_old"] = 1
                rows_map[sym]["close_old"] = det2[date_old]["close"]
                rows_map[sym]["cap_old"] = co2
                # shares 也一并更新（更可信）
                rows_map[sym]["shares"] = det2.get("shares")
                rec_old += 1

            # 新日恢复
            if missing[sym]["missing_new"] == 1 and cn2 is not None:
                total_new += cn2
                missing[sym]["missing_new"] = 0
                missing[sym]["recovered_new"] = 1
                rows_map[sym]["missing_new"] = 0
                rows_map[sym]["recovered_new"] = 1
                rows_map[sym]["close_new"] = det2[date_new]["close"]
                rows_map[sym]["cap_new"] = cn2
                rows_map[sym]["shares"] = det2.get("shares")
                rec_new += 1

        logger.info(f"{name}: 二次尝试恢复 —— 旧日 {rec_old} / 新日 {rec_new}")

        # 更新缺失计数
        miss_old = sum(v["missing_old"] for v in missing.values())
        miss_new = sum(v["missing_new"] for v in missing.values())

    # -------- 输出清单（缺失 & 明细）--------
    if rows_map:
        details_df = pd.DataFrame(list(rows_map.values()))
        out_details = os.path.join(RAW_DIR, f"per_ticker_details_{name}_{date_old}_vs_{date_new}.csv")
        details_df.to_csv(out_details, index=False, encoding="utf-8-sig")
        logger.info(f"{name}: 个股明细（收盘价/股本/市值）已保存：{out_details}（{len(details_df)} 行）")

    if missing:
        rows = [{"ticker": k, **v} for k, v in sorted(missing.items())]
        out_csv = os.path.join(RAW_DIR, f"missing_{name}_{date_old}_vs_{date_new}.csv")
        pd.DataFrame(rows).to_csv(out_csv, index=False, encoding="utf-8-sig")
        logger.info(f"{name}: 未获取到价格/股本的股票清单已保存：{out_csv}（{len(rows)} 只）")

    logger.info(f"{name}: 缺失(旧日){miss_old} / 缺失(新日){miss_new}")
    return (total_old if total_old > 0 else None,
            total_new if total_new > 0 else None)



def main():
    DATE_OLD = input("请输入旧日期（YYYY-MM-DD）：")  # 旧日期（含当天，若当天休市，将回溯至最近一交易日）
    DATE_NEW = input("请输入新日期（YYYY-MM-DD）：")  # 新日期（含当天，同上规则）

    logger.info(f"==== 开始计算（{DATE_OLD} vs {DATE_NEW}）====")

    # ---- Constituents ----
    logger.info("加载 S&P 500 名录 ...")
    sp500_syms = get_sp500_symbols()
    logger.info(f"S&P 500 数量：{len(sp500_syms)}")

    logger.info("加载 恒生指数 名录（Excel） ...")
    hsi_syms = get_hsi_symbols_from_excel()
    logger.info(f"恒生指数 数量：{len(hsi_syms)}")

    logger.info("加载 沪深300 名录（Excel）")
    hs300_syms = get_hs300_symbols()
    logger.info(f"沪深300 数量：{len(hs300_syms)}")

    # ---- Compute caps ----
    caps = {}

    logger.info("计算 标普500 自由流通市值 ...")
    us_old, us_new = compute_index_caps(sp500_syms, DATE_OLD, DATE_NEW, "USD", "S&P 500")
    caps["US"] = (us_old, us_new)

    logger.info("计算 恒生指数 自由流通市值 ...")
    hk_old, hk_new = compute_index_caps(hsi_syms, DATE_OLD, DATE_NEW, "HKD", "HSI")
    caps["HK"] = (hk_old, hk_new)

    logger.info("计算 沪深300 自由流通市值 ...")
    cn_old, cn_new = compute_index_caps(hs300_syms, DATE_OLD, DATE_NEW, "CNY", "HS300")
    caps["CN"] = (cn_old, cn_new)

    # ---- Output ----
    print("\n================ 结果汇总 ================\n")
    for k, unit, title in [("CN", "CNY", "沪深300"), ("US", "USD", "标普500"), ("HK", "HKD", "恒生指数")]:
        old_v, new_v = caps.get(k, (None, None))
        gainer = (new_v - old_v) if (old_v is not None and new_v is not None) else None
        print(f"{title}（{DATE_OLD}）总市值（自由流通近似）：{_fmt_billions(old_v, unit)}")
        print(f"{title}（{DATE_NEW}）总市值（自由流通近似）：{_fmt_billions(new_v, unit)}")
        print(f"{title} Gainer = 新 − 旧 ：{_fmt_billions(gainer, unit)}")
        print("")

    # 返回（在交互式环境可用一个变量名接收）
    Gainer = {
        "HS300": {
            "unit": "CNY",
            "date_old": DATE_OLD, "cap_old": cn_old,
            "date_new": DATE_NEW, "cap_new": cn_new,
            "gainer": (cn_new - cn_old) if (cn_old and cn_new) else None,
        },
        "SP500": {
            "unit": "USD",
            "date_old": DATE_OLD, "cap_old": us_old,
            "date_new": DATE_NEW, "cap_new": us_new,
            "gainer": (us_new - us_old) if (us_old and us_new) else None,
        },
        "HSI": {
            "unit": "HKD",
            "date_old": DATE_OLD, "cap_old": hk_old,
            "date_new": DATE_NEW, "cap_new": hk_new,
            "gainer": (hk_new - hk_old) if (hk_old and hk_new) else None,
        }
    }

    out_json = os.path.join(RAW_DIR, f"index_freefloat_caps_{DATE_OLD}_vs_{DATE_NEW}.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(Gainer, f, ensure_ascii=False, indent=2)

    logger.info(f"结果已保存：{out_json}")
    logger.info(f"日志文件：{LOG_PATH}")

    return Gainer


if __name__ == "__main__":
    Gainer = main()
