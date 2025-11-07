
"""
银行利率分析
输出银行利率表：interest_rate_metrics.xlsx
输出近两年银行利率走势图：interest_rate_trend_2y.png
"""

import logging
import os
from pathlib import Path
from typing import Dict

import akshare as ak
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# 日志配置
LOG_DIR = Path('logs')
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / 'interest_rate_metrics.log'


def _configure_logger() -> logging.Logger:
    """为当前模块配置带文件输出的日志记录器。"""
    logger = logging.getLogger(__name__)
    logger.propagate = False
    if logger.handlers:
        return logger

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.setLevel(logging.INFO)
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    return logger


logger = _configure_logger()

METRIC_CONFIG: Dict[str, Dict[str, object]] = {
    'US_fed': {
        'label': '美国联邦基金利率',
        'date_col': '日期',
        'value_col': '今值',
        'resample_rule': 'ME',
        'yoy_periods': 12,
        'min_points_for_average': 54,
    },
    'CN_lpr_1m': {
        'label': '中国 LPR (1 年)',
        'date_col': '日期',
        'value_col': '今值',
        'resample_rule': 'ME',
        'yoy_periods': 12,
        'min_points_for_average': 54,
    },
    'CN_interbank_1d': {
        'label': 'Chibor 隔夜',
        'date_col': '报告日',
        'value_col': '利率',
        'resample_rule': 'ME',
        'yoy_periods': 12,
        'min_points_for_average': 54,
    },
    'CN_interbank_1m': {
        'label': 'Chibor 1 个月',
        'date_col': '报告日',
        'value_col': '利率',
        'resample_rule': 'ME',
        'yoy_periods': 12,
        'min_points_for_average': 54,
    },
    'HK_interbank_1d': {
        'label': 'HIBOR 隔夜',
        'date_col': '报告日',
        'value_col': '利率',
        'resample_rule': 'ME',
        'yoy_periods': 12,
        'min_points_for_average': 54,
    },
    'HK_interbank_1m': {
        'label': 'HIBOR 1 个月',
        'date_col': '报告日',
        'value_col': '利率',
        'resample_rule': 'ME',
        'yoy_periods': 12,
        'min_points_for_average': 54,
    },
    'HK_interbank_1m_cny': {
        'label': 'HIBOR 人民币 1 个月',
        'date_col': '报告日',
        'value_col': '利率',
        'resample_rule': 'ME',
        'yoy_periods': 12,
        'min_points_for_average': 54,
    },
}


AVAILABLE_DATE_COLUMNS = ('日期', '报告日', '交易日', '公布日期', '发布时间')
AVAILABLE_VALUE_COLUMNS = ('今值', '利率', '数值', '收盘价', '最新值')


def _prepare_rate_dataframe(raw_df: pd.DataFrame, date_col: str, value_col: str) -> pd.DataFrame:
    """清洗利率数据，确保日期和值为可用格式。"""
    df = raw_df[[date_col, value_col]].copy()
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    df[value_col] = pd.to_numeric(df[value_col], errors='coerce')
    df = df.dropna(subset=[date_col, value_col])
    df = df.sort_values(date_col)
    return df


def _resample_series(df: pd.DataFrame, date_col: str, value_col: str, rule: str) -> pd.Series:
    """将数据按指定频率重采样至单一时间序列。"""
    series = df.set_index(date_col)[value_col].resample(rule).last().ffill()
    series = series.dropna()
    return series


def get_interest_rate_data():
    """
    获取美国、中国、香港的利率数据。
    返回: 包含各类利率数据的字典。
    """
    try:
        us_rate_fed = ak.macro_bank_usa_interest_rate().tail(50)

        cn_lpr_all = ak.macro_china_lpr()
        cn_rate_fed = cn_lpr_all[['TRADE_DATE', 'LPR1Y']].rename(
            columns={'TRADE_DATE': '日期', 'LPR1Y': '今值'}
        ).dropna()
        cn_rate_fed = cn_rate_fed.sort_values('日期').tail(80)

        cn_rate_interbank_1d = ak.rate_interbank(
            market="中国银行同业拆借市场", symbol="Chibor人民币", indicator="隔夜"
        ).tail(2000)

        cn_rate_interbank_1m = ak.rate_interbank(
            market="中国银行同业拆借市场", symbol="Chibor人民币", indicator='1月'
        ).tail(2000)

        hk_rate_interbank_1d = ak.rate_interbank(
            market="香港银行同业拆借市场", symbol="Hibor港币", indicator="隔夜"
        ).tail(2000)

        hk_rate_interbank_1m = ak.rate_interbank(
            market="香港银行同业拆借市场", symbol="Hibor港币", indicator='1月'
        ).tail(2000)

        hk_rate_interbank_1m_cny = ak.rate_interbank(
            market="香港银行同业拆借市场", symbol="Hibor人民币", indicator='1月'
        ).tail(2000)

        return {
            'US_fed': us_rate_fed,
            'CN_lpr_1m': cn_rate_fed,
            'CN_interbank_1d': cn_rate_interbank_1d,
            'CN_interbank_1m': cn_rate_interbank_1m,
            'HK_interbank_1d': hk_rate_interbank_1d,
            'HK_interbank_1m': hk_rate_interbank_1m,
            'HK_interbank_1m_cny': hk_rate_interbank_1m_cny,
        }

    except Exception as e:
        logger.error(f"获取利率数据时出现异常: {str(e)}")
        return None


def save_raw_interest_data(rate_data, output_path='output/raw_data'):
    """
    将原始利率数据分别保存为 Excel 文件。
    Args:
        rate_data: 字典形式的原始利率数据
        output_path: 保存路径（默认是 output/raw_data 文件夹）
    """
    if not rate_data:
        return

    os.makedirs(output_path, exist_ok=True)
    for key, df in rate_data.items():
        file_path = os.path.join(output_path, f"interest_rate_{key}.xlsx")
        try:
            df.to_excel(file_path, index=False)
            logger.info("%s 原始数据已保存至 %s", key, os.path.abspath(file_path))
        except PermissionError as exc:
            logger.warning("%s 原始数据无法写入（文件可能被占用）: %s", key, exc)
        except Exception as exc:
            logger.error("保存 %s 原始数据失败: %s", key, exc)


def calculate_interest_rate_metrics(rate_data, debug=False):
    """
    计算利率指标：当前值、日期、MoM、YoY、5年均值，并记录详细日志。

    Args:
        rate_data: 不同地区利率数据的字典
        debug: 是否启用 DEBUG 级别日志
    """
    if not rate_data:
        logger.error("未获取到任何利率数据，无法计算指标。")
        return pd.DataFrame()

    log_level = logging.DEBUG if debug else logging.INFO
    logger.setLevel(log_level)
    for handler in logger.handlers:
        handler.setLevel(log_level)

    results = []

    min_span_days = int(365 * 4.5)

    for metric, raw_df in rate_data.items():
        config = METRIC_CONFIG.get(metric, {})
        label = config.get('label', metric)
        logger.info("==== 开始处理 %s (%s) ====", label, metric)

        if raw_df is None or raw_df.empty:
            logger.warning("%s 数据为空，跳过计算。", label)
            results.append({
                'region': metric,
                'current_value': np.nan,
                'current_date': None,
                'mom': np.nan,
                'yoy': np.nan,
                'five_year_avg': np.nan,
                'date_range': None,
            })
            continue

        date_col = config.get('date_col')
        value_col = config.get('value_col')

        if date_col not in raw_df.columns:
            date_col = next((col for col in AVAILABLE_DATE_COLUMNS if col in raw_df.columns), None)
        if value_col not in raw_df.columns:
            value_col = next((col for col in AVAILABLE_VALUE_COLUMNS if col in raw_df.columns), None)

        if not date_col or not value_col:
            logger.error("%s 缺少必要的日期或数值列，无法计算。", label)
            results.append({
                'region': metric,
                'current_value': np.nan,
                'current_date': None,
                'mom': np.nan,
                'yoy': np.nan,
                'five_year_avg': np.nan,
                'date_range': None,
            })
            continue

        cleaned_df = _prepare_rate_dataframe(raw_df, date_col, value_col)
        if cleaned_df.empty:
            logger.warning("%s 清洗后数据为空。", label)
            results.append({
                'region': metric,
                'current_value': np.nan,
                'current_date': None,
                'mom': np.nan,
                'yoy': np.nan,
                'five_year_avg': np.nan,
                'date_range': None,
            })
            continue

        start_date = cleaned_df[date_col].iloc[0]
        end_date = cleaned_df[date_col].iloc[-1]
        logger.info("%s 清洗后数据量: %d 条，日期范围: %s 至 %s", label, len(cleaned_df), start_date.date(), end_date.date())

        series = _resample_series(cleaned_df, date_col, value_col, config.get('resample_rule', 'ME'))
        if series.empty:
            logger.warning("%s 重采样后数据为空。", label)
            results.append({
                'region': metric,
                'current_value': np.nan,
                'current_date': None,
                'mom': np.nan,
                'yoy': np.nan,
                'five_year_avg': np.nan,
                'date_range': None,
            })
            continue

        if debug:
            logger.debug("%s 最近重采样数据:\n%s", label, series.tail(6).to_string())

        current_date = series.index[-1]
        current_value = float(series.iloc[-1])
        logger.info("%s 当前值: %s -> %.4f", label, current_date.date(), current_value)

        mom = np.nan
        if len(series) > 1:
            prev_value = float(series.iloc[-2])
            prev_date = series.index[-2]
            if prev_value != 0:
                mom = (current_value - prev_value) / prev_value
                logger.info(
                    "%s MoM: 基于 %s 的 %.4f -> %.4f, 变动率 %.4f",
                    label,
                    prev_date.date(),
                    prev_value,
                    current_value,
                    mom,
                )
            else:
                logger.warning("%s MoM 基期数值为 0，无法计算。", label)
        else:
            logger.warning("%s MoM 缺少上一期数据。", label)

        yoy = np.nan
        yoy_periods = int(config.get('yoy_periods', 12))
        if len(series) > yoy_periods:
            yoy_value = float(series.iloc[-(yoy_periods + 1)])
            yoy_date = series.index[-(yoy_periods + 1)]
            if yoy_value != 0:
                yoy = (current_value - yoy_value) / yoy_value
                logger.info(
                    "%s YoY: 基于 %s 的 %.4f -> %.4f, 变动率 %.4f",
                    label,
                    yoy_date.date(),
                    yoy_value,
                    current_value,
                    yoy,
                )
            else:
                logger.warning("%s YoY 基期数值为 0，无法计算。", label)
        else:
            logger.warning("%s YoY 数据不足，至少需要 %d 期历史数据。", label, yoy_periods + 1)

        five_year_avg = np.nan
        date_range = None
        five_year_start = current_date - pd.DateOffset(years=5)
        five_year_series = series[series.index >= five_year_start]
        if not five_year_series.empty:
            span_days = (five_year_series.index[-1] - five_year_series.index[0]).days
            min_points = int(config.get('min_points_for_average', 36))
            if span_days >= min_span_days and len(five_year_series) >= min_points:
                five_year_avg = float(five_year_series.mean())
                date_range = f"{five_year_series.index[0].strftime('%Y-%m')} to {five_year_series.index[-1].strftime('%Y-%m')}"
                logger.info(
                    "%s 5 年均值: 时间范围 %s, 数据点 %d, 均值 %.4f",
                    label,
                    date_range,
                    len(five_year_series),
                    five_year_avg,
                )
            else:
                logger.warning(
                    "%s 5 年均值数据覆盖不足: %d 天 / %d 个点, 需至少 %d 天 / %d 个点",
                    label,
                    span_days,
                    len(five_year_series),
                    min_span_days,
                    min_points,
                )
        else:
            logger.warning("%s 近 5 年内缺少有效数据，无法计算 5 年均值。", label)

        results.append({
            'region': metric,
            'current_value': current_value,
            'current_date': current_date.strftime('%Y-%m-%d'),
            'mom': mom,
            'yoy': yoy,
            'five_year_avg': five_year_avg,
            'date_range': date_range,
        })

    return pd.DataFrame(results)


def map_interest_format(row):
    """
    将原始利率指标行格式化为标准输出格式。
    """
    mapping = {
        'US_fed': ('美国', 'USD', 'US Federal Fund Rate'),
        'CN_lpr_1m': ('中国', 'CNY', '中国央行LPR 1年'),
        'CN_interbank_1d': (None, 'CNY', 'Chibor 隔夜'),
        'CN_interbank_1m': (None, 'CNY', 'Chibor 1个月'),
        'HK_interbank_1d': ('香港', 'HKD', 'HIBOR 隔夜'),
        'HK_interbank_1m': (None, 'HKD', 'HIBOR 1个月'),
        'HK_interbank_1m_cny': (None, 'CNY', 'HIBOR人民币 1个月'),
    }

    region, currency, label = mapping.get(row['region'], (None, '未知', row['region']))

    return pd.Series({
        '区域': region,
        '货币': currency,
        '利率久期': label,
        '当前值': round(row['current_value'], 2) if pd.notna(row['current_value']) else '-',
        '当前值日期': pd.to_datetime(row['current_date']).strftime('%Y-%m-%d') if pd.notna(row['current_date']) else '-',
        'MoM(%)': round(row['mom'] * 100, 1) if pd.notna(row['mom']) else '-',
        'YoY(%)': round(row['yoy'] * 100, 1) if pd.notna(row['yoy']) else '-',
        '5年均值': round(row['five_year_avg'], 2) if pd.notna(row['five_year_avg']) else '-',
        '5年均值日期': row['date_range'] if pd.notna(row.get('date_range')) else '-',
    })


def plot_interest_rate_trend(rate_data, output_path='output', years=2):
    """
    可视化近N年主要银行利率走势（月度）。

    参数:
        rate_data: get_interest_rate_data() 返回的字典
        output_path: 图像保存路径
        years: 展示的年限（默认2年）
    """
    plt.rcParams['font.family'] = 'SimHei'
    plt.rcParams['axes.unicode_minus'] = False
    plt.style.use('seaborn-v0_8-muted')

    selected_metrics = {
        'US_fed': {'date_col': '日期', 'value_col': '今值', 'label': '美国联邦基金利率', 'step': True},
        'CN_lpr_1m': {'date_col': '日期', 'value_col': '今值', 'label': '中国LPR(1年)', 'step': False},
        'CN_interbank_1m': {'date_col': '报告日', 'value_col': '利率', 'label': '中国同业拆借(1个月)', 'step': False},
        'HK_interbank_1m': {'date_col': '报告日', 'value_col': '利率', 'label': '香港Hibor(1个月)', 'step': False},
        'HK_interbank_1m_cny': {'date_col': '报告日', 'value_col': '利率', 'label': '香港Hibor人民币(1个月)', 'step': False},
    }

    plt.figure(figsize=(12, 6))
    now = pd.Timestamp.today()
    start_date = now - pd.DateOffset(years=years)

    for key, meta in selected_metrics.items():
        df = rate_data.get(key)
        if df is None or df.empty:
            continue
        df[meta['date_col']] = pd.to_datetime(df[meta['date_col']])
        df = df.sort_values(meta['date_col'])
        df = df[df[meta['date_col']] >= start_date]
        df_monthly = df.set_index(meta['date_col']).resample('M').last().ffill().reset_index()

        if meta.get('step'):
            plt.step(df_monthly[meta['date_col']], df_monthly[meta['value_col']], where='post', label=meta['label'])
        else:
            plt.plot(df_monthly[meta['date_col']], df_monthly[meta['value_col']], label=meta['label'])

    plt.title(f"近{years}年主要利率走势", fontsize=16)
    plt.xlabel("日期", fontsize=12)
    plt.ylabel("利率(%)", fontsize=12)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.xticks(rotation=45)
    plt.grid(True, linestyle='--', alpha=0.4)
    plt.legend(fontsize=10)
    plt.tight_layout()

    os.makedirs(output_path, exist_ok=True)
    plt.savefig(os.path.join(output_path, f'interest_rate_trend_{years}y.png'), dpi=300)
    plt.close()


def generate_report(debug=False):
    """
    生成宏观经济指标报告。

    Args:
        debug: 是否启用调试日志
    """
    print("\n3. 利率分析")
    print("-" * 30)

    rate_data = get_interest_rate_data()
    if rate_data is None:
        logger.error("获取利率数据失败，无法生成报告。")
        return

    save_raw_interest_data(rate_data)

    rate_metrics = calculate_interest_rate_metrics(rate_data, debug)
    if rate_metrics.empty:
        logger.error("利率指标计算失败。")
        return

    print(rate_metrics)

    output_path = 'output'
    os.makedirs(output_path, exist_ok=True)

    formatted_rate_df = rate_metrics.apply(map_interest_format, axis=1)
    formatted_rate_df.to_excel(f"{output_path}/interest_rate_metrics.xlsx", index=False)
    logger.info("利率指标表已输出至 %s", os.path.abspath(f"{output_path}/interest_rate_metrics.xlsx"))

    plot_interest_rate_trend(rate_data)
    logger.info("利率走势图已输出至 %s", os.path.abspath(output_path))


def main(debug=False):
    try:
        generate_report(debug=debug)
    except Exception as e:
        logger.error(f"生成报告时出现异常: {str(e)}")


if __name__ == "__main__":
    main()
