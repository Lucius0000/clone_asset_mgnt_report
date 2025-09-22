import os
import pandas as pd
from fredapi import Fred

def fetch_gdp_all_from_fred():
    # 从环境变量中读取 FRED API KEY
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        raise ValueError("环境变量 FRED_API_KEY 未设置")

    fred = Fred(api_key=api_key)

    # 获取三个国家/地区的 GDP 序列
    series_dict = {
        "US_GDP_Billion_USD": 'GDP',                   # 美国，十亿美元
        "CN_GDP_Billion_CNY": 'CHNGDPNQDSMEI',         # 中国，单位：元（按季度）
        "HK_GDP_Billion_HKD": 'HKGGDPNQDSMEI'          # 香港，单位：亿港元（按季度）
    }

    gdp_df = pd.DataFrame()
    for col_name, series_id in series_dict.items():
        try:
            s = fred.get_series(series_id)
            gdp_df[col_name] = s
        except Exception as e:
            print(f"获取 {col_name} ({series_id}) 失败：{e}")

    gdp_df.index.name = "Date"
    gdp_df = gdp_df.dropna(how="all")

    # 保存为 Excel
    output_path = "gdp_total_data_fred.xlsx"
    gdp_df.to_excel(output_path)

    print(f"GDP 总量数据已保存到 {output_path}")
    return gdp_df

# 示例运行
if __name__ == "__main__":
    df = fetch_gdp_all_from_fred()
    print(df.tail())
    df.to_excel('output/gdp.xlsx')
