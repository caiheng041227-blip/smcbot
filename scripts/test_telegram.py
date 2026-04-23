"""独立验证 Telegram 配置是否正确。

使用方式（项目根目录执行）：
    python scripts/test_telegram.py

前置条件：
  1) 已在 .env 中填好 TELEGRAM_BOT_TOKEN、TELEGRAM_CHAT_ID（可选 TELEGRAM_PROXY）
  2) 已 pip install -r requirements.txt

该脚本会：
  - 发送一条启动测试文本
  - 再发送一条示例信号（走 format_signal 路径），便于肉眼检查 Markdown 渲染
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytz
from dotenv import load_dotenv

# 保证能 import 到项目内模块
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from notify.telegram_bot import TelegramNotifier  # noqa: E402
from utils.logger import logger, setup_logger  # noqa: E402


def _demo_signal() -> SimpleNamespace:
    ny = pytz.timezone("America/New_York")
    now_et = datetime.now(tz=ny)
    return SimpleNamespace(
        signal_id="demo_001",
        symbol="ETHUSDT",
        direction="long",
        triggered_level="pdPOC",
        poi_type="order_block",
        poi_high=2263.5,
        poi_low=2260.0,
        entry_price=2264.80,
        stop_loss=2258.00,
        take_profit=2275.00,
        risk_reward=2.0,
        total_score=6,
        scores={"C1": 2, "C3": 1, "C4": 1, "C6": 2},
        created_at=int(now_et.timestamp() * 1000),
        liquidity_sweep_candle={"wick_body_ratio": 3.2, "direction": "lower"},
    )


async def _main() -> int:
    setup_logger("INFO")
    load_dotenv(ROOT / ".env")

    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    proxy = os.getenv("TELEGRAM_PROXY", "").strip() or None

    if not token or not chat_id:
        logger.error(
            "请在 .env 中配置 TELEGRAM_BOT_TOKEN 和 TELEGRAM_CHAT_ID。"
            "可复制 .env.example 为 .env 后填写。"
        )
        return 1

    logger.info(f"使用代理: {proxy or '(不启用)'}")
    notifier = TelegramNotifier(token=token, chat_id=chat_id, proxy=proxy)
    await notifier.start()

    ok1 = await notifier.send_text("🤖 SMC Bot 启动测试")
    logger.info(f"启动测试消息: {'OK' if ok1 else 'FAIL'}")

    ok2 = await notifier.send_signal(_demo_signal())
    logger.info(f"示例信号消息: {'OK' if ok2 else 'FAIL'}")

    await notifier.stop()
    return 0 if (ok1 and ok2) else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
