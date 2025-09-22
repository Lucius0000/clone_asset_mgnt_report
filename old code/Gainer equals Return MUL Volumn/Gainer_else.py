#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gainer calculator — supports Config + CLI end-date
Metric: Gainer = Return * Volume
- Return = (latest close) - (close from one month earlier)
- Volume = sum of daily Volume over the past month window (exclusive of the month-ago date, inclusive of the latest date)
Data source: yfinance ('BTC-USD', 'GLD')

Outputs:
  - Logs under output/raw_data/else_gainer.log  （--debug/-d 或 Config 中 DEBUG_DEFAULT 控制是否同步在控制台输出）
  - Raw CSV under output/raw_data/<TICKER>.csv
  - Results Excel under output/gainer_else.xlsx【仅包含展示列，避免重复；且无科学计数法】

Notes:
  - 'Volumn' 字段名按你的要求保留此拼写
  - 'Gainer' 以十亿（Billion）为单位；当 |Gainer| < 1 B 时显示两位小数，否则无小数。
"""

# ---- 代理 & 忽略告警 ----
import os
import warnings
os.environ['http_proxy'] = 'http://127.0.0.1:7890'
os.environ['https_proxy'] = 'http://127.0.0.1:7890'
warnings.filterwarnings("ignore")

# ============================== Config ===============================
# 在 Spyder/IDE 中直接修改；命令行传参会覆盖这里的设置
END_DATE_STR = None          # 例如 "2025-08-15"；None 表示用最新交易日
DEBUG_DEFAULT = False        # True 时在控制台同步打印日志
WINDOW_DAYS = 120            # 指定 end-date 时向前抓取的最小天数
PERIOD = "90d"               # 未指定 end-date 时 yfinance 的 period
INTERVAL = "1d"              # yfinance bar 间隔
# ====================================================================

import logging
import argparse
from typing import Optional, List

import pandas as pd
import yfinance as yf
import openpyxl  # noqa: F401
from tqdm import tqdm

# ----------------------------- Paths ---------------------------------
TICKERS = ["BTC-USD", "GLD"]

OUTPUT_DIR = "output"
RAW_DIR = os.path.join(OUTPUT_DIR, "raw_data")
LOG_PATH = os.path.join(RAW_DIR, "else_gainer.log")

# 货币单位映射（可以按需扩展）
TICKER_CURRENCY = {
    "BTC-USD": "USD",
    "GLD": "USD",
}

RETURN_DECIMALS = 2     # Return 保留小数位
BILLION = 1_000_000_000


# --------------------------- Utilities --------------------------------

def ensure_dirs():
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def setup_logging(log_path: str = LOG_PATH, debug: bool = False) -> None:
    """Configure logging to file (always) and to console when debug=True."""
    ensure_dirs()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # Clear existing handlers (if re-run)
    for h in list(logger.handlers):
        logger.removeHandler(h)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # File handler (always on)
    fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Console handler (guarded by debug)
    if debug:
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    logging.info("Log file: %s", log_path)
    logging.info("Console logging enabled: %s", debug)


def format_thousands_number(x, decimals=0):
    """Format number with thousands separator and given decimals (as string to avoid scientific notation)."""
    if x is None:
        return ""
    fmt = f"{{:,.{decimals}f}}"
    return fmt.format(x)


def format_gainer_billion(value: float, currency: str) -> str:
    """
    Convert numeric gainer (in base units) to '<#,###> B <CURRENCY>' string.
    Rule: if |value| < 1B -> show 2 decimals; else show 0 decimals.
    """
    if value is None:
        return ""
    b = value / BILLION
    decimals = 2 if abs(value) < BILLION else 0
    s = format_thousands_number(b, decimals=decimals)
    unit = currency or ""
    return f"{s} B {unit}".strip()


# --------------------------- Core Logic --------------------------------

def fetch_and_save_raw(ticker: str, end_dt: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    """
    Fetch OHLCV for the given ticker and save raw CSV.
    - 若 end_dt 提供：使用 start = end_dt - WINDOW_DAYS, end = end_dt + 1 天（确保包含结束日）
    - 否则：使用 period=PERIOD
    Returns the full DataFrame with columns including ['Open','High','Low','Close','Adj Close','Volume'].
    """
    if end_dt is None:
        logging.info("Fetching data for %s (period=%s, interval=%s)", ticker, PERIOD, INTERVAL)
        df = yf.download(ticker, period=PERIOD, interval=INTERVAL, auto_adjust=False, progress=False)
    else:
        start_dt = end_dt - pd.Timedelta(days=WINDOW_DAYS)
        end_inclusive = end_dt + pd.Timedelta(days=1)  # yfinance 的 end 为开区间
        logging.info("Fetching data for %s (start=%s, end=%s, interval=%s)",
                     ticker, start_dt.date(), end_inclusive.date(), INTERVAL)
        df = yf.download(
            ticker,
            start=start_dt.strftime("%Y-%m-%d"),
            end=end_inclusive.strftime("%Y-%m-%d"),
            interval=INTERVAL,
            auto_adjust=False,
            progress=False,
        )

    if df.empty:
        raise ValueError(f"No data returned by yfinance for {ticker}")
    df.index = pd.to_datetime(df.index)
    df.sort_index(inplace=True)
    raw_path = os.path.join(RAW_DIR, f"{ticker}.csv")
    df.to_csv(raw_path)
    logging.info("[%s] Raw data saved to %s; rows=%d; range: %s → %s",
                 ticker, raw_path, len(df), df.index.min().date(), df.index.max().date())
    return df


def compute_gainer_for_df(ticker: str, df: pd.DataFrame, end_dt: Optional[pd.Timestamp] = None) -> dict:
    """
    Compute Gainer metric for a ticker using its daily dataframe.
    - 若 end_dt 提供，则仅使用 df.index <= end_dt 的数据计算 as_of 与回溯窗口
    """
    for col in ["Close", "Volume"]:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' missing for {ticker}")

    if end_dt is not None:
        df = df.loc[df.index <= end_dt]
        if df.empty:
            raise ValueError(f"No data on/before specified end-date for {ticker}.")

    as_of_date = df["Close"].dropna().index.max()
    close_latest = float(df.loc[as_of_date, "Close"])
    logging.info("[%s] Latest close date chosen: %s; Close=%.6f", ticker, as_of_date.date(), close_latest)

    target_month_ago = as_of_date - pd.DateOffset(months=1)
    month_ago_candidates = df.loc[:target_month_ago].index
    if len(month_ago_candidates) == 0:
        raise ValueError(f"Insufficient history to find month-ago close for {ticker}.")
    month_ago_date = month_ago_candidates.max()
    close_month_ago = float(df.loc[month_ago_date, "Close"])
    logging.info("[%s] Month-ago reference target: %s; selected trading day: %s; Close=%.6f",
                 ticker, target_month_ago.date(), month_ago_date.date(), close_month_ago)

    # Return
    ret = close_latest - close_month_ago
    logging.info("[%s] Return = Latest Close - Month-ago Close = %.6f - %.6f = %.6f",
                 ticker, close_latest, close_month_ago, ret)

    # Volume sum over (month_ago_date, as_of_date]
    window_mask = (df.index > month_ago_date) & (df.index <= as_of_date)
    vol_sum = float(df.loc[window_mask, "Volume"].fillna(0).sum())
    logging.info("[%s] Volumn sum over (%s, %s] = %.0f",
                 ticker, month_ago_date.date(), as_of_date.date(), vol_sum)

    # Gainer
    gainer = ret * vol_sum
    logging.info("[%s] Gainer = Return * Volumn = %.6f * %.0f = %.6f", ticker, ret, vol_sum, gainer)

    currency = TICKER_CURRENCY.get(ticker, "")

    # 展示列（字符串，避免科学计数法）
    return {
        "ticker": ticker,
        "as_of_date": as_of_date.date().isoformat(),
        "month_ago_date": month_ago_date.date().isoformat(),
        "Return": format_thousands_number(ret, decimals=RETURN_DECIMALS),
        "Volumn": format_thousands_number(vol_sum, decimals=0),
        "Gainer": format_gainer_billion(gainer, currency),
    }


def main():
    parser = argparse.ArgumentParser(description="Compute Gainer metric from yfinance data.")
    # CLI 默认值取自 Config（便于 Spyder/IDE 调整）；命令行传参会覆盖
    parser.add_argument("-d", "--debug", action="store_true", default=DEBUG_DEFAULT,
                        help="Enable console logging (logs are always saved to file).")
    parser.add_argument("--end-date", type=str, default=END_DATE_STR,
                        help="结束日期（YYYY-MM-DD），将使用该日或之前最近交易日作为 as_of。")
    args = parser.parse_args()

    ensure_dirs()
    setup_logging(LOG_PATH, debug=args.debug)

    end_dt: Optional[pd.Timestamp] = None
    if args.end_date:
        try:
            end_dt = pd.to_datetime(args.end_date).normalize()
        except Exception as e:
            raise SystemExit(f"--end-date 解析失败：{args.end_date}，请用 YYYY-MM-DD 格式。错误：{e}")

    logging.info("Configured end-date: %s",
                 end_dt.date().isoformat() if isinstance(end_dt, pd.Timestamp) else "latest")
    logging.info("=== Starting Gainer computation ===")

    results: List[dict] = []

    for t in tqdm(TICKERS, desc="Processing tickers for else", ncols=100):
        try:
            raw_df = fetch_and_save_raw(t, end_dt=end_dt)
            res = compute_gainer_for_df(t, raw_df, end_dt=end_dt)
            results.append(res)
        except Exception as e:
            logging.exception("Failed to compute Gainer for %s: %s", t, e)

    if not results:
        logging.error("No results were computed.")
        return

    # 仅保留展示列，避免与数值原始列“重复”
    summary = pd.DataFrame(results)
    # 字符串排序在这里是词典序，若需严格数值排序，可额外返回数值列做临时排序后再丢弃
    summary_sorted = summary.sort_values("Gainer", ascending=False, key=lambda s: s)

    # 保存 Excel（只有展示列 → 无科学计数法）
    out_xlsx = os.path.join(RAW_DIR, "gainer_else.xlsx")
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        summary_sorted.to_excel(writer, index=False, sheet_name="Gainer")

        # 适当调宽列宽（纯展示优化）
        ws = writer.book["Gainer"]
        for col_idx, col_name in enumerate(summary_sorted.columns, start=1):
            max_len = max(len(str(col_name)), *(len(str(v)) for v in summary_sorted[col_name].astype(str)))
            ws.column_dimensions[openpyxl.utils.get_column_letter(col_idx)].width = min(max_len + 2, 50)

    logging.info("Saved results to %s", out_xlsx)

    # 控制台输出同样只展示展示列（不受科学计数法影响）
    # print("\nGainer results (descending):\n")
    # print(summary_sorted.to_string(index=False))

    # 顶部条目（仅做日志提示）
    top_row = summary_sorted.iloc[0]
    logging.info("Top instrument by Gainer: %s (%s)", top_row["ticker"], top_row["Gainer"])
    logging.info("=== Completed Gainer computation ===")


if __name__ == "__main__":
    main()
