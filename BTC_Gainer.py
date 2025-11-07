from pycoingecko import CoinGeckoAPI
import pandas as pd
from datetime import datetime, timedelta, timezone

cg = CoinGeckoAPI()

def _to_epoch_seconds(dt_utc):
    return int(dt_utc.replace(tzinfo=timezone.utc).timestamp())

def _fetch_btc_market_caps_utc(start_date, end_date):
    """
    拉取 [start_date, end_date]（含缓冲）区间内的 BTC 市值时间序列（USD）
    返回：DataFrame(index=datetime64[ns, UTC], column='market_cap_usd')
    """
    # 为了稳妥，给前后各加一天缓冲，避免某日无采样点
    start_utc = datetime.combine(start_date, datetime.min.time(), tzinfo=timezone.utc) - timedelta(days=1)
    end_utc   = datetime.combine(end_date,   datetime.max.time(), tzinfo=timezone.utc) + timedelta(days=1)

    data = cg.get_coin_market_chart_range_by_id(
        id="bitcoin",
        vs_currency="usd",
        from_timestamp=_to_epoch_seconds(start_utc),
        to_timestamp=_to_epoch_seconds(end_utc)
    )
    caps = pd.DataFrame(data.get("market_caps", []), columns=["ts_ms", "market_cap_usd"])
    if caps.empty:
        raise RuntimeError("从 CoinGecko 未获取到任何市值数据，请检查日期范围是否合理。")

    caps["ts"] = pd.to_datetime(caps["ts_ms"], unit="ms", utc=True)
    caps = caps.drop(columns=["ts_ms"]).set_index("ts").sort_index()
    return caps

def _pick_cap_for_date(caps_df, date_obj):
    """
    选定“某个自然日（UTC）”的市值。优先取该日最后一个采样点；
    若该日无点，则在 ±3 天范围内就近取值。
    """
    day_start = pd.Timestamp(datetime.combine(date_obj, datetime.min.time()), tz="UTC")
    day_end   = pd.Timestamp(datetime.combine(date_obj, datetime.max.time()), tz="UTC")

    # 当天的最后一个采样点
    same_day = caps_df.loc[(caps_df.index >= day_start) & (caps_df.index <= day_end)]
    if not same_day.empty:
        return float(same_day["market_cap_usd"].iloc[-1])

    # 若缺失，允许在 ±3 天内寻找最近点
    window = caps_df.loc[(caps_df.index >= day_start - pd.Timedelta(days=3)) &
                         (caps_df.index <= day_end   + pd.Timedelta(days=3))]
    if window.empty:
        raise ValueError(f"日期 {date_obj} 附近没有可用市值数据。")
    # 选择与目标日相差时间最小的点
    target = pd.Timestamp(datetime.combine(date_obj, datetime.min.time()), tz="UTC")
    nearest_idx = (window.index - target).abs().argmin()
    return float(window["market_cap_usd"].iloc[nearest_idx])

def format_usd(x):
    return f"${x:,.2f}"

def format_billion(x):
    return f"{x/1e9:,.2f} B"

# ---- 交互：要求用户输入旧日期和新日期（YYYY-MM-DD）----
old_date_str = input("请输入【旧日期】(YYYY-MM-DD): ").strip()
new_date_str = input("请输入【新日期】(YYYY-MM-DD): ").strip()

# 解析与校验
try:
    old_date = datetime.strptime(old_date_str, "%Y-%m-%d").date()
    new_date = datetime.strptime(new_date_str, "%Y-%m-%d").date()
except ValueError:
    raise SystemExit("日期格式错误，请使用 YYYY-MM-DD，例如 2017-12-17")

if new_date < old_date:
    raise SystemExit("新日期必须不早于旧日期。")

# 拉取区间数据并取两日市值
caps = _fetch_btc_market_caps_utc(old_date, new_date)
mcap_old = _pick_cap_for_date(caps, old_date)
mcap_new = _pick_cap_for_date(caps, new_date)

# 计算 Gainer
Gainer = mcap_new - mcap_old

# 输出
print("\n=== 比特币总市值（USD） ===")
print(f"{old_date}  市值: {format_usd(mcap_old)}  ({format_billion(mcap_old)} USD)")
print(f"{new_date}  市值: {format_usd(mcap_new)}  ({format_billion(mcap_new)} USD)")
print(f"\nGainer = 新日期 - 旧日期 = {format_usd(Gainer)}  ({format_billion(Gainer)} USD)")

# 为了在 Spyder 变量窗中可直接查看差值，保留变量名 Gainer
Gainer
