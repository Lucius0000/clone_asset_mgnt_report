"""
直接执行该代码，即可调用其余各个脚本，方便一键执行指标分析
"""

import logging

# 配置日志格式
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main(debug=False):
    try:
        import cpi
        cpi.main(debug=debug)
    except Exception as e:
        logging.error(f"cpi 执行失败: {e}")

    try:
        import GDP_new
        GDP_new.main(debug=debug)
    except Exception as e:
        logging.error(f"GDP_new 执行失败: {e}")

    try:
        import interest_rate
        interest_rate.main(debug=debug)
    except Exception as e:
        logging.error(f"interest_rate 执行失败: {e}")

    try:
        import carry_trade
        carry_trade.main(debug=debug)
    except Exception as e:
        logging.error(f"carry_trade 执行失败: {e}")
  
    try:
        import asset_stock_index
        asset_stock_index.main(debug=debug)
    except Exception as e:
        logging.error(f"asset_stock_index 执行失败: {e}")

    try:
        import currency
        currency.main(debug=debug)
    except Exception as e:
        logging.error(f"currency 执行失败: {e}")
        
    try:
        import precious_metals
    except Exception as e:
        logging.error(f"precious_metals 执行失败: {e}")

    try:
        import bonds
        bonds.main(debug=debug)
    except Exception as e:
        logging.error(f"bonds 执行失败: {e}")

    try:
        import crypto_market_report as crypto_market_report
        crypto_market_report.main(debug=debug)
    except Exception as e:
        logging.error(f"crypto_market_report 执行失败: {e}")
        

if __name__ == "__main__":
    main()
