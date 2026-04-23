# 交易计划 Agent — 数据采集层

ETHUSDT (Binance USDT 永续) SMC/ICT 交易 bot 的数据采集与处理层：实时订阅 WebSocket、维护 Volume Profile / Delta / CVD / 关键价位。本期不含信号引擎与通知。

## 安装

系统 Python 3.9+ 即可（macOS 自带 3.9.6 可直接用）。若需更新版本：`brew install python@3.11`。

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 配置

```bash
cp .env.example .env          # Telegram 占位，本期留空
```

编辑 `config.yaml`：
- `binance.proxy`：中国大陆用户必须填本地代理。默认 `socks5://127.0.0.1:7890`（Clash/Mihomo 默认端口），按实际情况改；若在能直连 Binance 的网络下运行，设为空字符串 `""`。
- `volume_profile.buckets`：VP 分档数，默认 200，可调。
- `symbol` / `timeframes`：交易品种与订阅周期。

## 运行

```bash
python main.py
```

启动后会：
1. 通过 WebSocket 订阅 `ethusdt@aggTrade` + `ethusdt@kline_{1m,15m,4h,1d}`
2. 每根 K 线 close 时打印 OHLCV + 该 bar Delta + 全局 CVD
3. 每 60s 打印一次当前 session 的 VP（POC/VAH/VAL）与关键价位
4. 跨越 NY 时间 17:00（CME 收盘）时锁定"昨日 VP"并持久化到 SQLite

Ctrl+C 退出。

## 单元测试

```bash
pytest -q
```

## 项目结构

```
data/       Binance WebSocket 客户端 + K 线内存缓存
engine/     Volume Profile、Delta/CVD、关键价位计算
storage/    SQLite 持久化（aiosqlite），存 K 线与每日 VP 快照
utils/      loguru 日志配置
tests/      VP 算法单测
config.yaml 交易参数（品种、时区、分档数、代理）
main.py     入口：启动数据采集、打印状态
```

## 设计备注

- **全异步**：`asyncio` + `websockets` + `aiohttp` + `aiosqlite`。
- **零 pandas/numpy**：VP 用一维 dict 累加 + 两侧扩展，纯 Python 即可。
- **断线重连**：指数退避 1s → 60s，带抖动。
- **VP session 边界**：以 NY 时间 17:00 为"日分界"（对齐 CME 期货收盘）。
- **bucket_size**：启动时以首价的 20% 价格区间 / 档数估算；运行中区间外的价格也会正确落入新 bucket（dict 无上限）。
