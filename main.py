"""一键串联所有专题脚本，统一触发指标生成流程。"""

from __future__ import annotations

import importlib
import logging
from types import ModuleType
from typing import Callable, Optional


# 配置日志格式
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


Runner = Optional[Callable[[ModuleType], None]]


def _run_task(module_name: str, runner: Runner, debug: bool) -> None:
    """Import module and optionally call its runner."""
    logging.info("开始执行 %s", module_name)
    try:
        module = importlib.import_module(module_name)
        if runner is not None:
            runner(module)
        logging.info("%s 执行完成", module_name)
    except Exception as exc:  # pragma: no cover - 防御性日志
        logging.error("%s 执行失败: %s", module_name, exc, exc_info=debug)


def main(debug: bool = False) -> None:
    """Run all indicator scripts in sequence."""

    tasks: list[tuple[str, Runner]] = [
        ("cpi", lambda mod: mod.main(debug=debug)),
        ("GDP_new", lambda mod: mod.main(debug=debug)),
        ("interest_rate", lambda mod: mod.main(debug=debug)),
        ("carry_trade", lambda mod: mod.main(debug=debug)),
        ("asset_stock_index", lambda mod: mod.main(debug=debug)),
        ("currency", lambda mod: mod.main(debug=debug)),
        ("crypto_market_report", lambda mod: mod.main(debug=debug)),
        # 以下脚本在导入时即会执行主流程
        ("precious_metals", None),
        ("bonds", None),
    ]

    for module_name, runner in tasks:
        _run_task(module_name, runner, debug)


if __name__ == "__main__":
    main()
