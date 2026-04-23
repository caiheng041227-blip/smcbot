"""Telegram 通知模块。

对外暴露：
- format_signal(signal): 把 signal 对象格式化为 Markdown 文本
- TelegramNotifier: 异步 Telegram Bot 封装
"""
from notify.formatter import format_signal
from notify.telegram_bot import TelegramNotifier

__all__ = ["format_signal", "TelegramNotifier"]
