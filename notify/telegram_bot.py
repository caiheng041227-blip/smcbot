"""Telegram Bot 异步封装(只发送,不接收用户反馈)。

使用 python-telegram-bot>=20.0 的 Application:
- 外发:`send_signal(s)` 将 SignalState 格式化后推送到 Telegram
- 不再接入入站回调(持仓门控已移除,用户不需要回传开平仓状态)
- 所有失败记 error log,不抛出
- 支持可选 SOCKS5/HTTP 代理
"""
from __future__ import annotations

from typing import Any, Optional

try:
    from telegram.constants import ParseMode
    from telegram.ext import Application
    from telegram.request import HTTPXRequest
except ImportError:  # 允许在无依赖环境下 import 模块
    ParseMode = None  # type: ignore[assignment]
    Application = None  # type: ignore[assignment]
    HTTPXRequest = None  # type: ignore[assignment]

from utils.logger import logger

from notify.formatter import format_signal


class TelegramNotifier:
    """单向 Telegram 发信封装。"""

    def __init__(
        self,
        token: str,
        chat_id: str,
        proxy: Optional[str] = None,
    ) -> None:
        if not token:
            raise ValueError("TelegramNotifier 需要非空 token")
        if not chat_id:
            raise ValueError("TelegramNotifier 需要非空 chat_id")
        self._token = token
        self._chat_id = str(chat_id)
        self._proxy = proxy or None
        self._app: Optional[Any] = None

    async def start(self) -> None:
        if Application is None:
            raise RuntimeError(
                "python-telegram-bot 未安装,请先 pip install -r requirements.txt"
            )
        builder = Application.builder().token(self._token)
        if self._proxy:
            builder = builder.proxy(self._proxy).get_updates_proxy(self._proxy)
        self._app = builder.build()
        try:
            await self._app.initialize()
            await self._app.start()
            me = await self._app.bot.get_me()
            logger.info(f"TelegramNotifier 已连接: @{me.username}")
        except Exception as e:  # noqa: BLE001
            logger.error(f"TelegramNotifier 启动失败: {e}")

    async def stop(self) -> None:
        if self._app is None:
            return
        try:
            if self._app.running:
                await self._app.stop()
            await self._app.shutdown()
        except Exception as e:  # noqa: BLE001
            logger.warning(f"TelegramNotifier 关闭异常: {e}")
        finally:
            self._app = None

    async def send_signal(self, signal: Any, with_actions: bool = False) -> bool:
        """发送信号消息。`with_actions` 参数保留为兼容性占位,当前不生成按钮。"""
        try:
            text = format_signal(signal)
        except Exception as e:  # noqa: BLE001
            logger.error(f"格式化信号失败: {e}")
            return False
        return await self.send_text(text)

    async def send_text(self, text: str) -> bool:
        if self._app is None or self._app.bot is None:
            logger.error("TelegramNotifier 未初始化")
            return False
        try:
            await self._app.bot.send_message(
                chat_id=self._chat_id,
                text=text,
                parse_mode=ParseMode.MARKDOWN if ParseMode is not None else "Markdown",
                disable_web_page_preview=True,
            )
            return True
        except Exception as e:  # noqa: BLE001
            logger.error(f"Telegram 发送失败: {e}")
            return False


__all__ = ["TelegramNotifier"]
