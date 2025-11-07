"""
银行利率分析
输出银行利率表：output/interest_rate_metrics.xlsx
输出近两年银行利率走势图：output/interest_rate_trend_2y.png
输出计算日志：output/interest_rate.log
"""

import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import os
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import warnings

warnings.filterwarnings("ignore")

# ----------------------------
# 日志配置：文件(仅debug=True) + 控制台
# ----------------------------
def _setup_logger(debug: bool):
    os.makedirs("output", exist_ok=True)
    if debug:
        os.makedirs("output/raw_data", exist_ok=True)

    logger = logging.getLogger("interest_rate")
    logger.handlers = []
    logger.setLevel(logging.INFO if debug else logging.WARNING)

    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO if debug else logging.WARNING)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # 文件日志仅在 debug=True 时启用
    if debug:
        fh = logging.FileHandler("output/raw_data/interest_rate.log", encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger

# 全局 logger 引用，但不立刻绑定 handler，等待 main(debug) 设置
logger = logging.getLogger("interest_rate")


# ----------------------------
# 数据获取
# ----------------------------
def get_interest_rate_data():
    """
    获取美国、中国、香港的利率数据（自适应拉取行数）
    返回: 包含各国利率数据的字典
    """
    try:
        # 美国：联邦基金利率（表头通常为 '日期'、'今值'）
        us_rate_fed = ak.macro_bank_usa_interest_rate().tail(50)

        # 中国：使用 LPR 一年期替代基准利率（确保列为 '日期'、'今值'）
        cn_lpr_all = ak.macro_china_lpr()
        cn_rate_fed = (
            cn_lpr_all[["TRADE_DATE", "LPR1Y"]]
            .rename(columns={"TRADE_DATE": "日期", "LPR1Y": "今值"})
            .dropna()
            .sort_values("日期")
            .tail(80)
        )

        # 中国 Chibor（天频）
        cn_rate_interbank_1d = ak.rate_interbank(
            market="中国银行同业拆借市场", symbol="Chibor人民币", indicator="隔夜"
        ).tail(2000)
        cn_rate_interbank_1m = ak.rate_interbank(
            market="中国银行同业拆借市场", symbol="Chibor人民币", indicator="1月"
        ).tail(2000)

        # 香港 Hibor（天频）
        hk_rate_interbank_1d = ak.rate_interbank(
            market="香港银行同业拆借市场", symbol="Hibor港币", indicator="隔夜"
        ).tail(2000)
        hk_rate_interbank_1m = ak.rate_interbank(
            market="香港银行同业拆借市场", symbol="Hibor港币", indicator="1月"
        ).tail(2000)

        # 香港人民币 Hibor（天频）
        hk_rate_interbank_1m_cny = ak.rate_interbank(
            market="香港银行同业拆借市场", symbol="Hibor人民币", indicator="1月"
        ).tail(2000)

        data = {
            "US_fed": us_rate_fed,
            "CN_lpr_1m": cn_rate_fed,
            "CN_interbank_1d": cn_rate_interbank_1d,
            "CN_interbank_1m": cn_rate_interbank_1m,
            "HK_interbank_1d": hk_rate_interbank_1d,
            "HK_interbank_1m": hk_rate_interbank_1m,
            "HK_interbank_1m_cny": hk_rate_interbank_1m_cny,
        }

        logger.info("完成利率数据获取，各表行数：%s", {k: v.shape for k, v in data.items()})
        return data

    except Exception as e:
        logger.error("获取利率数据时出错: %s", str(e))
        return None

# ----------------------------
# 原始数据保存
# ----------------------------
def save_raw_interest_data(rate_data, output_path="output/raw_data"):
    os.makedirs(output_path, exist_ok=True)
    for key, df in rate_data.items():
        file_path = os.path.join(output_path, f"interest_rate_{key}.xlsx")
        try:
            df.to_excel(file_path, index=False)
            logger.info("已保存原始数据：%s", file_path)
        except Exception as e:
            logger.error("保存原始数据失败 [%s]: %s", key, str(e))

# ----------------------------
# 计算工具：窗口内“取最近目标日”的一行
# ----------------------------
def _select_nearest_in_window(df, date_col, target_date):
    """
    在 df 的窗口记录中，返回“与 target_date 差的绝对值最小”的一行（Series）。
    窗口为空则返回 None。
    """
    if df is None or df.empty:
        return None
    idx = (df[date_col] - target_date).abs().argsort()
    return df.iloc[idx.iloc[0]] if len(idx) > 0 else None

# ----------------------------
# 指标计算（含详细日志）
# ----------------------------
def calculate_interest_rate_metrics(rate_data):
    """
    计算利率指标：当前值、日期、MoM、YoY、5年均值（含详细日志）
    """
    if not rate_data:
        logger.error("未提供利率数据，无法计算指标")
        return None

    results = []

    for metric, data in rate_data.items():
        try:
            logger.info("=" * 60)
            logger.info("开始处理指标：%s", metric)
            logger.info("原始数据形状：%s  列：%s", data.shape, list(data.columns)[:10])

            # 列名识别
            if "今值" in data.columns:  # 央行/公告类
                value_col = "今值"
                date_col = "日期"
                is_fed_like = True
            else:  # 同业拆借
                value_col = "利率"
                date_col = "报告日"
                is_fed_like = False

            # 转换日期、按时间倒序
            data = data.copy()
            data[date_col] = pd.to_datetime(data[date_col])
            data = data.sort_values(date_col, ascending=False)

            # 最新值
            valid = data[data[value_col].notna()]
            if valid.empty:
                logger.warning("没有有效数据点：%s", metric)
                results.append(
                    {
                        "region": metric,
                        "current_value": np.nan,
                        "current_date": None,
                        "mom": np.nan,
                        "yoy": np.nan,
                        "five_year_avg": np.nan,
                        "date_range": None,
                    }
                )
                continue

            latest = valid.iloc[0]
            current_value = float(latest[value_col])
            current_date = pd.to_datetime(latest[date_col])
            logger.info("最新值：%s = %.6f ，日期 = %s", metric, current_value, current_date.date())

            result = {
                "region": metric,
                "current_value": current_value,
                "current_date": current_date.strftime("%Y-%m-%d"),
                "mom": np.nan,
                "yoy": np.nan,
                "five_year_avg": np.nan,
                "date_range": None,
            }

            # ---------------- MoM ----------------
            if is_fed_like and metric == "US_fed":
                # 使用上一条记录作为环比基准（而非“上一不同值”）
                if len(valid) >= 2 and float(valid.iloc[1][value_col]) != 0:
                    prev_row = valid.iloc[1]
                    prev_val = float(prev_row[value_col])
                    result["mom"] = (current_value - prev_val) / prev_val
                    logger.info(
                        "MoM[US_fed]：当前=%.6f@%s，上一条=%.6f@%s，MoM=%.6f",
                        current_value,
                        current_date.date(),
                        prev_val,
                        pd.to_datetime(prev_row[date_col]).date(),
                        result["mom"],
                    )
                else:
                    logger.info("MoM[US_fed]：缺少上一条记录或上一值为0，跳过")
            elif metric == "CN_lpr_1m":
                # LPR 月度发布时间约每月20日：用 ±10 天窗口，在目标月附近取最近
                target_date = current_date - pd.DateOffset(days=30)
                window = data[
                    (data[date_col] >= target_date - pd.DateOffset(days=10))
                    & (data[date_col] <= target_date + pd.DateOffset(days=10))
                    & (data[value_col].notna())
                ]
                prev_row = _select_nearest_in_window(window, date_col, target_date)
                if prev_row is not None and float(prev_row[value_col]) != 0:
                    prev_val = float(prev_row[value_col])
                    result["mom"] = (current_value - prev_val) / prev_val
                    logger.info(
                        "MoM[LPR]：目标日=%s，窗口=[%s, %s]，选中=%.6f@%s，MoM=%.6f",
                        target_date.date(),
                        (target_date - pd.DateOffset(days=10)).date(),
                        (target_date + pd.DateOffset(days=10)).date(),
                        prev_val,
                        pd.to_datetime(prev_row[date_col]).date(),
                        result["mom"],
                    )
                else:
                    logger.info("MoM[LPR]：窗口无有效点，或上一值为0，跳过")
            else:
                # 通用：取 T-30 天内窗口（最多到 target_date），选最近
                target_date = current_date - pd.DateOffset(days=30)
                window = data[
                    (data[date_col] >= target_date - pd.DateOffset(days=10))
                    & (data[date_col] <= target_date)
                    & (data[value_col].notna())
                ]
                prev_row = _select_nearest_in_window(window, date_col, target_date)
                if prev_row is not None and float(prev_row[value_col]) != 0:
                    prev_val = float(prev_row[value_col])
                    result["mom"] = (current_value - prev_val) / prev_val
                    logger.info(
                        "MoM[通用]：目标日=%s，选中=%.6f@%s，MoM=%.6f",
                        target_date.date(),
                        prev_val,
                        pd.to_datetime(prev_row[date_col]).date(),
                        result["mom"],
                    )
                else:
                    logger.info("MoM[通用]：窗口无有效点，或上一值为0，跳过")

            # ---------------- YoY ----------------
            if is_fed_like:
                # Fed：近似选 350 天前（季度公告更稳）
                target_date = current_date - pd.DateOffset(days=350)
                yoy_window = data[
                    (data[date_col] >= target_date - pd.DateOffset(months=1))
                    & (data[date_col] <= target_date)
                    & (data[value_col].notna())
                ]
            else:
                # 同业拆借：365 天前 ±10 天
                target_date = current_date - pd.DateOffset(days=365)
                yoy_window = data[
                    (data[date_col] >= target_date - pd.DateOffset(days=10))
                    & (data[date_col] <= target_date)
                    & (data[value_col].notna())
                ]

            yoy_row = _select_nearest_in_window(yoy_window, date_col, target_date)
            if yoy_row is not None and float(yoy_row[value_col]) != 0:
                yoy_val = float(yoy_row[value_col])
                result["yoy"] = (current_value - yoy_val) / yoy_val
                logger.info(
                    "YoY：目标日=%s，选中=%.6f@%s，YoY=%.6f",
                    target_date.date(),
                    yoy_val,
                    pd.to_datetime(yoy_row[date_col]).date(),
                    result["yoy"],
                )
            else:
                logger.info("YoY：窗口无有效点，或上一年值为0，跳过")

            # ---------------- 5年均值 ----------------
            five_years_ago = current_date - pd.DateOffset(years=5)
            five_year_data = data[(data[date_col] >= five_years_ago) & (data[value_col].notna())].copy()

            if is_fed_like:
                # 季度末取最后一个：'Q'（修复原 'QE'）
                if not five_year_data.empty:
                    five_year_data = (
                        five_year_data.set_index(date_col).resample("Q").last().reset_index()
                    )
            else:
                # 月末取最后一个：'M'（修复原 'ME'）
                if not five_year_data.empty:
                    five_year_data = (
                        five_year_data.set_index(date_col).resample("M").last().reset_index()
                    )

            if not five_year_data.empty and len(five_year_data) >= 8:
                span_days = (five_year_data[date_col].max() - five_year_data[date_col].min()).days
                if span_days >= int(365 * 4.5):
                    result["five_year_avg"] = float(five_year_data[value_col].mean())
                    min_d = pd.to_datetime(five_year_data[date_col].min())
                    max_d = pd.to_datetime(five_year_data[date_col].max())
                    result["date_range"] = f"{min_d.strftime('%Y-%m')} to {max_d.strftime('%Y-%m')}"
                    logger.info(
                        "5年均值：区间=%s，点数=%d，跨度≈%.1f年，均值=%.6f",
                        result["date_range"],
                        len(five_year_data),
                        span_days / 365.0,
                        result["five_year_avg"],
                    )
                else:
                    logger.info(
                        "5年均值：覆盖不足（%.1f年），跳过", span_days / 365.0
                    )
            else:
                logger.info("5年均值：点数不足（<8），或无数据，跳过")

            results.append(result)

        except Exception as e:
            logger.error("计算 [%s] 指标时出错：%s", metric, str(e))
            results.append(
                {
                    "region": metric,
                    "current_value": np.nan,
                    "current_date": None,
                    "mom": np.nan,
                    "yoy": np.nan,
                    "five_year_avg": np.nan,
                    "date_range": None,
                }
            )

    return pd.DataFrame(results)

# ----------------------------
# 映射为输出表
# ----------------------------
def map_interest_format(row):
    mapping = {
        "US_fed": ("美国", "USD", "US Federal Fund Rate"),
        "CN_lpr_1m": ("中国", "CNY", "中国央行LPR 1年"),
        "CN_interbank_1d": (None, "CNY", "Chibor 隔夜"),
        "CN_interbank_1m": (None, "CNY", "Chibor 1月"),
        "HK_interbank_1d": ("香港", "HKD", "HIBOR 隔夜"),
        "HK_interbank_1m": (None, "HKD", "HIBOR 1月"),
        "HK_interbank_1m_cny": (None, "CNY", "HIBOR人民币 1月"),
    }
    region, currency, label = mapping.get(row["region"], (None, "未知", row["region"]))

    def _fmt_date(x):
        try:
            return pd.to_datetime(x).strftime("%Y-%m-%d")
        except Exception:
            return "-"

    def _r(v, nd=2):
        return round(v, nd) if pd.notna(v) else "-"

    return pd.Series(
        {
            "区域": region,
            "货币": currency,
            "利率久期": label,
            "当前值": _r(row.get("current_value"), 2),
            "当前值日期": _fmt_date(row.get("current_date")),
            "MoM(%)": _r(row.get("mom") * 100, 1) if pd.notna(row.get("mom")) else "-",
            "YoY(%)": _r(row.get("yoy") * 100, 1) if pd.notna(row.get("yoy")) else "-",
            "5年均值": _r(row.get("five_year_avg"), 2),
            "5年均值日期": row.get("date_range") if pd.notna(row.get("date_range")) else "-",
        }
    )

# ----------------------------
# 近N年走势图
# ----------------------------
def plot_interest_rate_trend(rate_data, output_path="output", years=2):
    # 字体与样式（仅用于渲染美观，不影响计算）
    plt.rcParams["font.family"] = "SimHei"
    plt.rcParams["axes.unicode_minus"] = False
    plt.style.use("seaborn-v0_8-muted")

    selected = {
        "US_fed": {"date_col": "日期", "value_col": "今值", "label": "美国联邦基金利率", "step": True},
        "CN_lpr_1m": {"date_col": "日期", "value_col": "今值", "label": "中国LPR(1年)", "step": False},
        "CN_interbank_1m": {"date_col": "报告日", "value_col": "利率", "label": "中国同业拆借(1月)", "step": False},
        "HK_interbank_1m": {"date_col": "报告日", "value_col": "利率", "label": "香港Hibor(1月)", "step": False},
        "HK_interbank_1m_cny": {"date_col": "报告日", "value_col": "利率", "label": "香港Hibor人民币(1月)", "step": False},
    }

    plt.figure(figsize=(12, 6))
    now = pd.Timestamp.today()
    start_date = now - pd.DateOffset(years=years)

    for key, meta in selected.items():
        df = rate_data.get(key)
        if df is None or df.empty:
            logger.info("走势图：%s 数据为空，跳过绘制", key)
            continue
        df = df.copy()
        df[meta["date_col"]] = pd.to_datetime(df[meta["date_col"]])
        df = df.sort_values(meta["date_col"])
        df = df[df[meta["date_col"]] >= start_date]
        df_m = df.set_index(meta["date_col"]).resample("M").last()
        if key == "US_fed":
            df_m = df_m.ffill()
        df_m = df_m.reset_index()

        if meta.get("step"):
            plt.step(df_m[meta["date_col"]], df_m[meta["value_col"]], where="post", label=meta["label"])
        else:
            plt.plot(df_m[meta["date_col"]], df_m[meta["value_col"]], label=meta["label"])

    plt.title(f"近{years}年主要利率走势", fontsize=16)
    plt.xlabel("日期", fontsize=12)
    plt.ylabel("利率（%）", fontsize=12)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.xticks(rotation=45)
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend(fontsize=10)
    plt.tight_layout()

    os.makedirs(output_path, exist_ok=True)
    out_path = os.path.join(output_path, f"interest_rate_trend_{years}y.png")
    plt.savefig(out_path, dpi=300)
    plt.close()
    logger.info("已保存走势图：%s", out_path)

# ----------------------------
# 报告生成
# ----------------------------
def generate_report():
    logger.info("")
    logger.info("=== 3. 利率分析 ===")
    rate_data = get_interest_rate_data()
    save_raw_interest_data(rate_data)

    rate_metrics = calculate_interest_rate_metrics(rate_data)
    logger.info("指标汇总（未经映射）：\n%s", rate_metrics)

    formatted_rate_df = rate_metrics.apply(map_interest_format, axis=1)

    # 在 debug=True 模式下，既写文件也把“映射后的指标表”写入日志
    try:
        xlsx_path = "output/interest_rate_metrics.xlsx"
        formatted_rate_df.to_excel(xlsx_path, index=False)
        logger.info("已保存指标表：%s", xlsx_path)
    except Exception as e:
        logger.error("保存指标表失败: %s", str(e))

    logger.info("指标表（映射后）：\n%s", formatted_rate_df)

    plot_interest_rate_trend(rate_data)

    return formatted_rate_df

def main(debug: bool = False):
    try:
        global logger
        logger = _setup_logger(debug)

        formatted_rate_df = generate_report()

        # 非调试模式：仅在控制台输出最终结果表，不保存过程日志
        if not debug:
            print("\n3. 利率分析")
            print("-"*30)
            print(formatted_rate_df)

    except Exception as e:
        # 即使在非调试模式下也输出错误到控制台
        logger.error("生成报告时出错: %s", str(e))

if __name__ == "__main__":
    main(debug=False)

