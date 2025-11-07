"""Calculate the change in gold total market capitalization across two weekends."""

ABOVE_GROUND_STOCK_TONS = 216_265  # Modify as needed to reflect above-ground gold stock in tons.
OZT_PER_TON = 32_150.7466


def _prompt_price(prompt: str) -> float:
    while True:
        user_input = input(prompt).strip()
        if not user_input:
            print("请输入价格，允许使用数字和逗号，例如 2035.25 或 2,035.25。")
            continue
        cleaned = user_input.replace(",", "")
        try:
            price = float(cleaned)
        except ValueError:
            print("无法解析输入，请输入有效的 LBMA Gold Price PM 数值。")
            continue
        if price < 0:
            print("价格不能为负数，请重新输入。")
            continue
        return price


def _calc_total_market_cap(price_pm: float) -> float:
    return ABOVE_GROUND_STOCK_TONS * OZT_PER_TON * price_pm


def _format_billion_usd(value: float) -> str:
    return f"{value / 1_000_000_000:,.2f} B USD"


def main() -> None:
    print("计算黄金总市值差值：本周末总市值 - 前两周周末总市值\n")
    current_price = _prompt_price("请输入本周末的 LBMA Gold Price PM（USD/oz）：")
    previous_price = _prompt_price("请输入前两周周末的 LBMA Gold Price PM（USD/oz）：")

    current_cap = _calc_total_market_cap(current_price)
    previous_cap = _calc_total_market_cap(previous_price)
    diff = current_cap - previous_cap

    print("\n结果：")
    print(f"本周末总市值：{_format_billion_usd(current_cap)}")
    print(f"前两周周末总市值：{_format_billion_usd(previous_cap)}")
    print(f"差值（本周末 - 前两周）：{_format_billion_usd(diff)}")


if __name__ == "__main__":
    main()
