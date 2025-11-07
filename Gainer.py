"""
Aggregate Gainer metrics across bonds, equities, gold, and bitcoin with extra progress hints.

This script keeps the original asset calculators untouched, orchestrates their outputs,
and writes ``output/Gainer.xlsx`` in the required layout.
"""

from __future__ import annotations

import builtins
import contextlib
import io
import runpy
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

import os
os.environ['http_proxy'] = 'http://127.0.0.1:7890'
os.environ['https_proxy'] = 'http://127.0.0.1:7890'

import Bond_Gainer
import Gold_Gainer
import Stock_Gainer

OUTPUT_PATH = Path("output") / "Gainer.xlsx"
BTC_SCRIPT_PATH = Path("BTC_Gainer.py")


def _prompt_date(message: str, default: Optional[datetime] = None) -> datetime:
    while True:
        raw = input(message).strip()
        if not raw and default is not None:
            return default
        try:
            return datetime.strptime(raw, "%Y-%m-%d")
        except ValueError:
            suffix = f"，默认值 {default:%Y-%m-%d}" if default else ""
            print(f"日期格式无效，请使用 YYYY-MM-DD{suffix}")


def _format_billion(value: Optional[float], unit: str, decimals: int = 2) -> str:
    if value is None:
        return "N/A"
    return f"{value:,.{decimals}f} B {unit}"


def _to_billions(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return value / 1_000_000_000


def _load_us_treasury_series() -> pd.DataFrame:
    files = sorted(Bond_Gainer.DATA_DIR.glob("MSPD_SumSecty*.csv"))
    if not files:
        raise FileNotFoundError("未找到 data/MSPD_SumSecty*.csv，请先更新美国国债数据。")
    csv_path = max(files, key=lambda p: p.stat().st_mtime)
    df = pd.read_csv(csv_path)
    if df.empty:
        raise RuntimeError(f"{csv_path.name} 没有数据。")
    df = df[df["Security Type Description"].astype(str).str.strip() == "Total Marketable"].copy()
    if df.empty:
        raise RuntimeError("CSV 中未找到 Security Type Description = Total Marketable 的记录。")
    df["Record Date"] = pd.to_datetime(df["Record Date"], errors="coerce")
    df = df.dropna(subset=["Record Date"]).sort_values("Record Date")
    df["Record Date"] = df["Record Date"].dt.normalize()

    def _to_float(value: object) -> float:
        return float(str(value).replace(",", ""))

    df["billions_usd"] = df["Total Public Debt Outstanding (in Millions)"].apply(_to_float) / 1000.0
    df = df[["Record Date", "billions_usd"]].drop_duplicates("Record Date", keep="last")
    return df


def _us_treasury_value_for(date_obj: datetime, series: pd.DataFrame) -> float:
    mask = series["Record Date"] <= date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
    if not mask.any():
        raise ValueError(f"美国国债数据缺少 {date_obj:%Y-%m-%d} 及之前的记录。")
    return float(series.loc[mask].iloc[-1]["billions_usd"])


def _compute_bond_caps(current_date: datetime, previous_date: datetime) -> Dict[str, Dict[str, Optional[float]]]:
    results: Dict[str, Dict[str, Optional[float]]] = {
        "US": {"unit": "USD", "current": None, "previous": None},
        "CN": {"unit": "CNY", "current": None, "previous": None},
    }

    print("- 正在读取美国国债规模...")
    try:
        us_series = _load_us_treasury_series()
        results["US"]["current"] = _us_treasury_value_for(current_date, us_series)
        results["US"]["previous"] = _us_treasury_value_for(previous_date, us_series)
    except Exception as exc:
        print(f"  [警告] 美国国债数据获取失败：{exc}")

    print("- 正在读取中国国债规模...")
    try:
        results["CN"]["current"] = Bond_Gainer._extract_cn_treasury_value(current_date)
        results["CN"]["previous"] = Bond_Gainer._extract_cn_treasury_value(previous_date)
    except Exception as exc:
        print(f"  [警告] 中国国债数据获取失败：{exc}")

    return results


def _compute_stock_caps(date_old: str, date_new: str) -> Dict[str, Dict[str, Optional[float]]]:
    results: Dict[str, Dict[str, Optional[float]]] = {
        "US": {"unit": "USD", "name": "S&P 500", "old": None, "new": None},
        "CN": {"unit": "CNY", "name": "HS300", "old": None, "new": None},
        "HK": {"unit": "HKD", "name": "HSI", "old": None, "new": None},
    }

    print("- 正在计算标普500市值差异...")
    try:
        sp500_syms = Stock_Gainer.get_sp500_symbols()
        old_val, new_val = Stock_Gainer.compute_index_caps(sp500_syms, date_old, date_new, "USD", "S&P 500")
        results["US"]["old"], results["US"]["new"] = old_val, new_val
    except Exception as exc:
        print(f"  [警告] 标普500市值计算失败：{exc}")

    print("- 正在计算沪深300市值差异...")
    try:
        hs300_syms = Stock_Gainer.get_hs300_symbols()
        old_val, new_val = Stock_Gainer.compute_index_caps(hs300_syms, date_old, date_new, "CNY", "HS300")
        results["CN"]["old"], results["CN"]["new"] = old_val, new_val
    except Exception as exc:
        print(f"  [警告] 沪深300市值计算失败：{exc}")

    print("- 正在计算恒生指数市值差异...")
    try:
        hsi_syms = Stock_Gainer.get_hsi_symbols_from_excel()
        old_val, new_val = Stock_Gainer.compute_index_caps(hsi_syms, date_old, date_new, "HKD", "HSI")
        results["HK"]["old"], results["HK"]["new"] = old_val, new_val
    except Exception as exc:
        print(f"  [警告] 恒生指数市值计算失败：{exc}")

    return results


@contextlib.contextmanager
def _patched_input(responses: List[str]):
    original_input = builtins.input
    iterator = iter(responses)

    def fake_input(prompt: str = "") -> str:
        try:
            value = next(iterator)
        except StopIteration:
            raise RuntimeError("BTC_Gainer 需要的输入次数超出预期。")
        # 回显选择，方便排查
        print(f"{prompt}{value}")
        return value

    builtins.input = fake_input
    try:
        yield
    finally:
        builtins.input = original_input


def _compute_btc_caps(previous_date: datetime, current_date: datetime) -> Tuple[Optional[float], Optional[float]]:
    if not BTC_SCRIPT_PATH.exists():
        print("  [警告] 未找到 BTC_Gainer.py，跳过 BTC 市值计算。")
        return None, None

    responses = [previous_date.strftime("%Y-%m-%d"), current_date.strftime("%Y-%m-%d")]
    buffer = io.StringIO()
    print("- 正在收集 BTC 市值差异...")
    try:
        with _patched_input(responses), contextlib.redirect_stdout(buffer):
            globals_dict = runpy.run_path(str(BTC_SCRIPT_PATH), run_name="__main__")
    except Exception as exc:
        print(f"  [警告] BTC 市值获取失败：{exc}")
        return None, None
    finally:
        output_text = buffer.getvalue().strip()
        if output_text:
            print("  [信息] BTC_Gainer 输出摘要：")
            for line in output_text.splitlines()[:4]:
                print(f"    {line}")

    old_val = globals_dict.get("mcap_old")
    new_val = globals_dict.get("mcap_new")
    return (float(old_val) if old_val is not None else None,
            float(new_val) if new_val is not None else None)


def _compute_gold_caps(previous_price: float, current_price: float) -> Tuple[float, float]:
    print("- 正在计算黄金市值差异...")
    prev_cap = Gold_Gainer._calc_total_market_cap(previous_price)
    curr_cap = Gold_Gainer._calc_total_market_cap(current_price)
    return prev_cap, curr_cap


def main() -> None:
    today = datetime.today()
    default_current = today.replace(hour=0, minute=0, second=0, microsecond=0)
    current_date = _prompt_date(f"请输入本周末日期（YYYY-MM-DD），周六最佳，回车默认 {default_current:%Y-%m-%d}： ", default=default_current)
    default_previous = current_date - timedelta(days=13)
    previous_date = _prompt_date(
        f"请输入两周前周末日期（YYYY-MM-DD，回车默认 {default_previous:%Y-%m-%d}）： ",
        default=default_previous,
    )

    print("\n请提供 LBMA Gold Price PM（USD/oz）")
    print("LBMA src: https://www.lbma.org.uk/cn/prices-and-data#/")
    current_gold_price = Gold_Gainer._prompt_price("本周末价格：")
    previous_gold_price = Gold_Gainer._prompt_price("两周前周末价格：")
    
    date_old_str = previous_date.strftime("%Y-%m-%d")
    date_new_str = current_date.strftime("%Y-%m-%d")

    print("\n[步骤 1] 汇总股票市值数据")
    stock_caps = _compute_stock_caps(date_old_str, date_new_str)

    print("\n[步骤 2] 汇总债券市值数据")
    bond_caps = _compute_bond_caps(current_date, previous_date)

    print("\n[步骤 3] 汇总黄金市值数据")
    gold_prev, gold_curr = _compute_gold_caps(previous_gold_price, current_gold_price)

    print("\n[步骤 4] 汇总 BTC 市值数据")
    btc_prev, btc_curr = _compute_btc_caps(previous_date, current_date)

    rows: List[Dict[str, Optional[str]]] = []

    us_stock_old = _to_billions(stock_caps["US"]["old"])
    us_stock_new = _to_billions(stock_caps["US"]["new"])
    rows.append({
        "区域": "美国",
        "资产大类": "股票权益",
        "Market Cap Last 2 week": _format_billion(us_stock_old, "USD"),
        "Market Cap This week": _format_billion(us_stock_new, "USD"),
        "Gainer": _format_billion(
            None if us_stock_old is None or us_stock_new is None else us_stock_new - us_stock_old,
            "USD",
        ),
    })

    us_bond_prev = bond_caps["US"]["previous"]
    us_bond_curr = bond_caps["US"]["current"]
    us_bond_prev_b = us_bond_prev if us_bond_prev is None else float(us_bond_prev)
    us_bond_curr_b = us_bond_curr if us_bond_curr is None else float(us_bond_curr)
    rows.append({
        "区域": None,
        "资产大类": "债券固收",
        "Market Cap Last 2 week": _format_billion(us_bond_prev_b, "USD", decimals=0),
        "Market Cap This week": _format_billion(us_bond_curr_b, "USD", decimals=0),
        "Gainer": _format_billion(
            None if us_bond_prev_b is None or us_bond_curr_b is None else us_bond_curr_b - us_bond_prev_b,
            "USD",
            decimals=0,
        ),
    })

    cn_stock_old = _to_billions(stock_caps["CN"]["old"])
    cn_stock_new = _to_billions(stock_caps["CN"]["new"])
    rows.append({
        "区域": "中国",
        "资产大类": "股票权益",
        "Market Cap Last 2 week": _format_billion(cn_stock_old, "CNY"),
        "Market Cap This week": _format_billion(cn_stock_new, "CNY"),
        "Gainer": _format_billion(
            None if cn_stock_old is None or cn_stock_new is None else cn_stock_new - cn_stock_old,
            "CNY",
        ),
    })

    cn_bond_prev = bond_caps["CN"]["previous"]
    cn_bond_curr = bond_caps["CN"]["current"]
    rows.append({
        "区域": None,
        "资产大类": "债券固收",
        "Market Cap Last 2 week": _format_billion(cn_bond_prev, "CNY"),
        "Market Cap This week": _format_billion(cn_bond_curr, "CNY"),
        "Gainer": _format_billion(
            None if cn_bond_prev is None or cn_bond_curr is None else cn_bond_curr - cn_bond_prev,
            "CNY",
        ),
    })

    hk_stock_old = _to_billions(stock_caps["HK"]["old"])
    hk_stock_new = _to_billions(stock_caps["HK"]["new"])
    rows.append({
        "区域": "香港",
        "资产大类": "股票权益",
        "Market Cap Last 2 week": _format_billion(hk_stock_old, "HKD"),
        "Market Cap This week": _format_billion(hk_stock_new, "HKD"),
        "Gainer": _format_billion(
            None if hk_stock_old is None or hk_stock_new is None else hk_stock_new - hk_stock_old,
            "HKD",
        ),
    })

    gold_prev_b = _to_billions(gold_prev)
    gold_curr_b = _to_billions(gold_curr)
    rows.append({
        "区域": "商品与贵金属（黄金）",
        "资产大类": None,
        "Market Cap Last 2 week": _format_billion(gold_prev_b, "USD"),
        "Market Cap This week": _format_billion(gold_curr_b, "USD"),
        "Gainer": _format_billion(
            None if gold_prev_b is None or gold_curr_b is None else gold_curr_b - gold_prev_b,
            "USD",
        ),
    })

    btc_prev_b = _to_billions(btc_prev)
    btc_curr_b = _to_billions(btc_curr)
    rows.append({
        "区域": "数字货币（BTC）",
        "资产大类": None,
        "Market Cap Last 2 week": _format_billion(btc_prev_b, "USD"),
        "Market Cap This week": _format_billion(btc_curr_b, "USD"),
        "Gainer": _format_billion(
            None if btc_prev_b is None or btc_curr_b is None else btc_curr_b - btc_prev_b,
            "USD",
        ),
    })

    df = pd.DataFrame(rows, columns=["区域", "资产大类", "Market Cap Last 2 week", "Market Cap This week", "Gainer"])
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(OUTPUT_PATH, index=False)
    print(f"\n整合结果已保存至：{OUTPUT_PATH}")


if __name__ == "__main__":
    main()
