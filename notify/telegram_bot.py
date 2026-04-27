"""Telegram Bot 异步封装(双向:推送 signal + 接收命令查询)。

使用 python-telegram-bot>=20.0 的 Application:
- 外发:`send_signal(s)` 将 SignalState 格式化后推送到 Telegram
- 入站命令(仅 chat_id 匹配的 message 响应,其余忽略):
    /signals [hours]  查近 N 小时 notified 信号,默认 6,limit 20
    /active           列当前 in-flight 信号(Step1~STEP6)
    /status           bot 状态:最近 1h close 时间、in-flight 计数
    /help             帮助
- 所有失败记 error log,不抛出
- 支持可选 SOCKS5/HTTP 代理
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

try:
    import pytz
    from telegram import KeyboardButton, ReplyKeyboardMarkup
    from telegram.constants import ParseMode
    from telegram.ext import Application, CommandHandler, MessageHandler, filters
    from telegram.request import HTTPXRequest
except ImportError:  # 允许在无依赖环境下 import 模块
    ParseMode = None  # type: ignore[assignment]
    Application = None  # type: ignore[assignment]
    CommandHandler = None  # type: ignore[assignment]
    MessageHandler = None  # type: ignore[assignment]
    KeyboardButton = None  # type: ignore[assignment]
    ReplyKeyboardMarkup = None  # type: ignore[assignment]
    filters = None  # type: ignore[assignment]
    HTTPXRequest = None  # type: ignore[assignment]
    pytz = None  # type: ignore[assignment]


# 按钮文字 → 对应命令映射(点击时 bot 收到的是文本)
_BTN_6H = "📊 近 6h"
_BTN_24H = "📊 近 24h"
_BTN_3D = "📊 近 3 天"
_BTN_ACTIVE = "⏳ in-flight"
_BTN_STATUS = "🔧 状态"
_BTN_HELP = "❓ 帮助"


def _build_reply_keyboard() -> Any:
    """持久化底部键盘(tap = send text)。"""
    if ReplyKeyboardMarkup is None or KeyboardButton is None:
        return None
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(_BTN_6H), KeyboardButton(_BTN_24H), KeyboardButton(_BTN_3D)],
            [KeyboardButton(_BTN_ACTIVE), KeyboardButton(_BTN_STATUS), KeyboardButton(_BTN_HELP)],
        ],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="点按钮或输入 /signals 12",
    )

from utils.logger import logger

from notify.formatter import format_signal


def _html_escape(s: Any) -> str:
    """TG HTML 模式只需要转义 < > &"""
    if s is None:
        return ""
    text = str(s)
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


class TelegramNotifier:
    """双向 Telegram 封装:推送 + 命令查询。"""

    def __init__(
        self,
        token: str,
        chat_id: str,
        proxy: Optional[str] = None,
        db: Any = None,
        engine: Any = None,
        tracker: Any = None,
        tz_name: str = "America/New_York",
    ) -> None:
        if not token:
            raise ValueError("TelegramNotifier 需要非空 token")
        if not chat_id:
            raise ValueError("TelegramNotifier 需要非空 chat_id")
        self._token = token
        self._chat_id = str(chat_id)
        self._proxy = proxy or None
        self._app: Optional[Any] = None
        self._db = db
        self._engine = engine
        self._tracker = tracker
        self._tz_name = tz_name

    async def start(self) -> None:
        if Application is None:
            raise RuntimeError(
                "python-telegram-bot 未安装,请先 pip install -r requirements.txt"
            )
        builder = Application.builder().token(self._token)
        if self._proxy:
            builder = builder.proxy(self._proxy).get_updates_proxy(self._proxy)
        self._app = builder.build()
        # 注册命令 + 按钮 handler:仅当 db/engine 至少有一个注入时开启入站
        if CommandHandler is not None and (self._db is not None or self._engine is not None):
            self._app.add_handler(CommandHandler("signals", self._cmd_signals))
            self._app.add_handler(CommandHandler("active", self._cmd_active))
            self._app.add_handler(CommandHandler("status", self._cmd_status))
            self._app.add_handler(CommandHandler("close", self._cmd_close))
            self._app.add_handler(CommandHandler("help", self._cmd_help))
            self._app.add_handler(CommandHandler("start", self._cmd_help))
            # 文本按钮(ReplyKeyboard 点击后发来的是纯文本)
            if MessageHandler is not None and filters is not None:
                self._app.add_handler(
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self._on_text)
                )
        try:
            await self._app.initialize()
            await self._app.start()
            # start_polling 开启 updater 接收用户消息
            if self._app.updater is not None:
                await self._app.updater.start_polling(drop_pending_updates=True)
            me = await self._app.bot.get_me()
            logger.info(f"TelegramNotifier 已连接: @{me.username}  (命令接收: 已启用)")
        except Exception as e:  # noqa: BLE001
            logger.error(f"TelegramNotifier 启动失败: {e}")

    async def stop(self) -> None:
        if self._app is None:
            return
        try:
            if self._app.updater is not None and self._app.updater.running:
                await self._app.updater.stop()
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

    async def send_text(
        self,
        text: str,
        with_keyboard: bool = False,
        parse_mode: Optional[str] = "Markdown",
    ) -> bool:
        """发送文本。
        - with_keyboard=True 时附带持久化按钮(启动消息用)
        - parse_mode=None 时发纯文本(避免 _ / * 踩坑)
        """
        if self._app is None or self._app.bot is None:
            logger.error("TelegramNotifier 未初始化")
            return False
        try:
            kwargs: dict = {
                "chat_id": self._chat_id,
                "text": text,
                "disable_web_page_preview": True,
            }
            if parse_mode is not None:
                kwargs["parse_mode"] = parse_mode
            if with_keyboard:
                kb = _build_reply_keyboard()
                if kb is not None:
                    kwargs["reply_markup"] = kb
            await self._app.bot.send_message(**kwargs)
            return True
        except Exception as e:  # noqa: BLE001
            logger.error(f"Telegram 发送失败: {e}")
            return False

    # ---- 命令 handlers(仅自己 chat 可调) ---------------------------------

    def _is_authorized(self, update: Any) -> bool:
        """只允许配置的 chat_id 调用命令,防止别人骚扰。"""
        chat = getattr(update, "effective_chat", None)
        if chat is None:
            return False
        return str(chat.id) == self._chat_id

    def _fmt_ts(self, ts: Optional[int]) -> str:
        if ts is None:
            return "-"
        try:
            tz = pytz.timezone(self._tz_name) if pytz is not None else timezone.utc
            dt = datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone(tz)
            return dt.strftime("%m-%d %H:%M")
        except Exception:  # noqa: BLE001
            return str(ts)

    async def _cmd_signals(self, update: Any, context: Any) -> None:
        if not self._is_authorized(update):
            return
        if self._db is None:
            await update.message.reply_text("DB 未接入,无法查询")
            return
        hours = 6
        if context.args:
            try:
                hours = max(1, min(int(context.args[0]), 168))
            except ValueError:
                pass
        rows = await self._db.recent_signals(hours=hours, limit=60, include_scored=True)
        if not rows:
            await update.message.reply_text(f"近 {hours}h 无 SCORED/NOTIFIED 信号")
            return
        # 折叠:同 (direction, entry, sl, tp) 的信号视为同一交易机会,取最新时间 + 来源列表
        groups: Dict[tuple, Dict[str, Any]] = {}
        for r in rows:
            key = (
                r.get("direction"),
                round(float(r.get("entry_price") or 0), 2),
                round(float(r.get("stop_loss") or 0), 2),
                round(float(r.get("take_profit") or 0), 2),
            )
            ts = r.get("notified_at") or r.get("scored_at") or r.get("created_at")
            g = groups.get(key)
            if g is None:
                groups[key] = {
                    "row": r, "ts": ts, "count": 1,
                    "any_notified": bool(r.get("notified_at")),
                    "sources": set(),
                    "max_score": float(r.get("total_score") or 0),
                }
                src = r.get("triggered_level") or r.get("poi_type")
                if src:
                    groups[key]["sources"].add(src)
            else:
                g["count"] += 1
                if ts and (g["ts"] is None or ts > g["ts"]):
                    g["ts"] = ts
                    g["row"] = r
                if r.get("notified_at"):
                    g["any_notified"] = True
                src = r.get("triggered_level") or r.get("poi_type")
                if src:
                    g["sources"].add(src)
                if float(r.get("total_score") or 0) > g["max_score"]:
                    g["max_score"] = float(r.get("total_score") or 0)
        # 按最新时间排序
        ordered = sorted(groups.values(), key=lambda g: g["ts"] or 0, reverse=True)
        n_notified_groups = sum(1 for g in ordered if g["any_notified"])
        n_scored_groups = len(ordered) - n_notified_groups
        lines = [
            f"<b>近 {hours}h 交易机会</b>  {len(ordered)} 单  "
            f"(notified={n_notified_groups} / scored-only={n_scored_groups})  "
            f"原始 {len(rows)} 条(已折叠同机会)\n"
        ]
        def _num(v: Any, decimals: int = 2) -> str:
            if isinstance(v, (int, float)):
                return f"{float(v):.{decimals}f}"
            return "-"

        for g in ordered[:20]:
            r = g["row"]
            t_str = self._fmt_ts(g["ts"])
            tag = "🟢 notified" if g["any_notified"] else "🟡 scored"
            dir_ = r.get("direction", "?")
            srcs = "/".join(sorted(g["sources"])) or "-"
            entry = r.get("entry_price")
            sl = r.get("stop_loss")
            tp = r.get("take_profit")
            rr = r.get("risk_reward")
            score = g["max_score"]
            outcome = r.get("outcome") or "open"
            pnl = r.get("pnl_r")
            pnl_str = f" {pnl:+.2f}R" if pnl is not None else ""
            dup_str = f" (×{g['count']}源)" if g["count"] > 1 else ""
            lines.append(
                f"<code>{_html_escape(t_str)}</code> {tag} {_html_escape(dir_)} "
                f"{_html_escape(srcs)}{dup_str}\n"
                f"  E={_num(entry)} SL={_num(sl)} TP={_num(tp)}  "
                f"RR={_num(rr)}  score={score:.1f}  [{_html_escape(outcome)}]{pnl_str}"
            )
        await update.message.reply_text("\n".join(lines), parse_mode="HTML")

    async def _cmd_active(self, update: Any, context: Any) -> None:
        if not self._is_authorized(update):
            return
        if self._engine is None:
            await update.message.reply_text("engine 未接入,无法查询")
            return
        active = getattr(self._engine, "active_signals", {}) or {}
        in_flight = []
        for sid, s in active.items():
            step = getattr(s.step, "value", str(s.step))
            if step in ("NOTIFIED", "EXPIRED", "INVALIDATED"):
                continue
            in_flight.append((sid, s, step))
        if not in_flight:
            await update.message.reply_text("当前无 in-flight 信号")
            return
        lines = [f"<b>in-flight 信号</b> ({len(in_flight)} 条)\n"]
        for sid, s, step in in_flight[:20]:
            poi_src = getattr(s, "poi_source", None) or "-"
            direction = getattr(s, "direction", "?")
            poi_low = getattr(s, "poi_low", None)
            poi_high = getattr(s, "poi_high", None)
            poi_range = f"[{poi_low}-{poi_high}]" if poi_low is not None else "-"
            lines.append(
                f"<code>{_html_escape(sid[:8])}</code> {_html_escape(direction)} "
                f"{_html_escape(step)}\n"
                f"  src={_html_escape(poi_src)}  POI={_html_escape(poi_range)}"
            )
        await update.message.reply_text("\n".join(lines), parse_mode="HTML")

    async def _cmd_status(self, update: Any, context: Any) -> None:
        if not self._is_authorized(update):
            return
        parts = ["<b>Bot 状态</b>\n"]
        if self._engine is not None:
            active = getattr(self._engine, "active_signals", {}) or {}
            by_step: dict = {}
            for s in active.values():
                step = getattr(s.step, "value", str(s.step))
                by_step[step] = by_step.get(step, 0) + 1
            parts.append(f"active_signals: {len(active)}")
            if by_step:
                # 手动格式化,避免 dict repr 的引号/括号破坏 HTML 解析
                dist_str = ", ".join(f"{k}={v}" for k, v in by_step.items())
                parts.append(f"  分布: {_html_escape(dist_str)}")
            candles = getattr(self._engine, "candles", None)
            if candles is not None:
                for tf in ("15m", "1h", "4h"):
                    try:
                        last = candles.last(
                            getattr(self._engine, "symbol", "ETHUSDT") or "ETHUSDT",
                            tf,
                            closed_only=True,
                        )
                        if last is not None:
                            parts.append(f"最近 {tf} close: {self._fmt_ts(last.close_time // 1000)}  C={last.close}")
                    except Exception:  # noqa: BLE001
                        pass
        if self._db is not None:
            try:
                rows = await self._db.recent_signals(hours=24, limit=1, include_scored=True)
                if rows:
                    r0 = rows[0]
                    if r0.get("notified_at"):
                        tag = "🟢 notified"
                        ts = r0.get("notified_at")
                    else:
                        tag = "🟡 scored(未达 threshold)"
                        ts = r0.get("scored_at")
                    parts.append(
                        f"最近信号: {tag}  {self._fmt_ts(ts)}  "
                        f"{_html_escape(r0.get('direction') or '-')} "
                        f"{_html_escape(r0.get('triggered_level') or r0.get('poi_type') or '-')}"
                    )
                else:
                    parts.append("最近 24h: 0 条信号(SCORED 和 NOTIFIED 都没)")
            except Exception as e:  # noqa: BLE001
                parts.append(f"DB 查询异常: {_html_escape(str(e))}")
        await update.message.reply_text("\n".join(parts), parse_mode="HTML")

    async def _cmd_close(self, update: Any, context: Any) -> None:
        """手动关闭一个 tracker(用户没进场或已平仓时用)。"""
        if not self._is_authorized(update):
            return
        if self._tracker is None:
            await update.message.reply_text("tracker 未接入")
            return
        if not context.args:
            await update.message.reply_text("用法: /close <sid 前 8 位>")
            return
        sid_prefix = context.args[0].strip()
        try:
            closed = await self._tracker.close_manual(sid_prefix)
        except Exception as e:  # noqa: BLE001
            await update.message.reply_text(f"关闭失败: {e}")
            return
        if closed is None:
            await update.message.reply_text(f"未找到 open tracker: {sid_prefix}")
        else:
            await update.message.reply_text(
                f"✅ tracker 已关闭: {closed.signal_id[:8]} "
                f"({closed.direction} entry={closed.entry:.2f})"
            )

    async def _cmd_help(self, update: Any, context: Any) -> None:
        if not self._is_authorized(update):
            return
        kb = _build_reply_keyboard()
        await update.message.reply_text(
            "<b>SMC Bot 命令</b>\n\n"
            "👇 <b>点下方按钮</b>(推荐) 或输入 /命令\n\n"
            "<code>/signals [小时]</code>  查近 N 小时(默认 6,最多 168)\n"
            "<code>/active</code>          当前 in-flight 信号\n"
            "<code>/status</code>          Bot 状态 + 最近 K 线时间\n"
            "<code>/close &lt;sid&gt;</code>   手动关闭一个 tracker(不再监控 TP/SL)\n"
            "<code>/help</code>            显示帮助",
            parse_mode="HTML",
            reply_markup=kb,
        )

    async def _on_text(self, update: Any, context: Any) -> None:
        """处理按钮点击(ReplyKeyboard 发来的是纯文本)。"""
        if not self._is_authorized(update):
            return
        text = (update.message.text or "").strip()
        # 模拟 context.args 以复用命令 handler
        if _BTN_6H in text:
            context.args = ["6"]
            await self._cmd_signals(update, context)
        elif _BTN_24H in text:
            context.args = ["24"]
            await self._cmd_signals(update, context)
        elif _BTN_3D in text:
            context.args = ["72"]
            await self._cmd_signals(update, context)
        elif _BTN_ACTIVE in text:
            await self._cmd_active(update, context)
        elif _BTN_STATUS in text:
            await self._cmd_status(update, context)
        elif _BTN_HELP in text:
            await self._cmd_help(update, context)
        # 其它文本:沉默忽略(别人发别的话也不响应)


__all__ = ["TelegramNotifier"]
