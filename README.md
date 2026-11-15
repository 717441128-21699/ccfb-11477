# 足球多源数据采集融合服务（本地模拟）

三家数据商推送（两家 WebSocket + 一家长轮询）的本地采集融合系统。**全部数据来自本地剧本模拟器，不接任何真实数据商。**

## 能力对照需求

| # | 需求 | 实现 |
|---|------|------|
| 1 | 多种推送方式接入、掉线重连、漏事件补回 | `fusion/connectors.py`：WS/长轮询连接器，指数退避自动重连；WS 用 `resume_from_seq`、长轮询用 `since` 游标补传，补传票标记 `backfilled` |
| 2 | 球队/球员/事件类型统一编码 | `fusion/mapping.py`：三家别名目录 + 三种原生报文适配器（A 大写下划线 JSON / B 紧凑短码 / C 中文信封），未知名称抛 `MappingError` 并计数 |
| 3 | 先到+多数确认合并、撤回生效 | `fusion/engine.py`：先到建事件后来投票，2 家或官方源即 `CONFIRMED`，否则 `PENDING`；撤回票全撤则 `RETRACTED` |
| 4 | 比分以官方源为准、别家先报待确认 | 引擎双比分：`authoritative_score`（仅官方源活跃票计分）与 `pending_score`（含未确认票）；官方源撤回触发权威比分回退 |
| 5 | 各家到达时间差与又快又准统计 | 每事件记录各票 `relative_ms`/`lag_ms`；赛后报告含准确率、误报数、延迟分位、最快次数，综合分 = 平均延迟 + 2000ms/误报 |
| 6 | 网页实时刷新比分与时间线 | `web/`：WebSocket 增量推送 + 3 秒轮询兜底，五个视图 |
| 7 | 赛后报告（报错与延迟） | `/api/matches/{id}/report`，终场后可查；无头回放额外落盘 `validation/last_report.json` |
| 校验 | 融合结果与剧本逐事件比对一致 | `validation/`：23 项自动检查（事件齐全、状态、来源、比分、撤回、补传、掉线重连） |

## 目录结构

```
config.py                  端口/时间倍率/融合阈值/退避参数
scenarios/sample_match.json 剧本：21 个真实事件 + 4 个误报撤回 + 2 段掉线 + 乱序注入
simulator/                 本地三家模拟数据商
  scheduler.py             剧本展开：延迟/抖动/乱序/误报撤回/掉线窗口
  encoders.py              三家原生报文格式
  core.py                  发送循环、掉线踢连、缓冲补传、长轮询游标
  server.py                WS 端点 /ws/A、/ws/B；长轮询 /poll/C
fusion/
  models.py                Report/Vote/FusedEvent/Correction/MatchState
  mapping.py               统一编码目录与适配器
  engine.py                融合状态机、双比分、纠错流水、赛后报告
  connectors.py            WS/长轮询连接器（重连+补传）
web/                       aiohttp REST + WS 推送 + 原生 JS 页面
validation/                逐事件比对器、无头回放
tests/                     23 个单元/集成测试
```

## 运行

```bash
pip install -r requirements.txt

# 1) 完整系统（模拟器 + 采集融合 + 网页），打开 http://127.0.0.1:8090
py runner.py all

# 2) 只跑模拟器 / 只跑采集器
py runner.py simulator
py runner.py collector

# 3) 无头端到端回放 + 逐事件校验（加速跑完，输出报告 JSON）
py validation/replay.py

# 4) 全部测试
py -m unittest discover -s tests -v
```

常用环境变量：`WEB_PORT`(8090)、`SIM_PORT`(8091)、`TIME_SCALE`(60，比赛秒/墙钟秒，越大越快)、
`MAJORITY_THRESHOLD`(2)、`OFFICIAL_SOURCE`(A)、`BACKOFF_BASE`(0.5)、`BACKOFF_CAP`(4)、`SCENARIO`。

## 剧本设计（sample_match.json）

- **传输差异**：A=官方 WS（3.0s 延迟，慢而准）、B=快速 WS（0.7s，快但 3 次误报）、C=长轮询（1.8s）
- **掉线补传**：B 在 30' 掉线 9s（EV-G2 靠重连补传）、C 在 50' 掉线 8s（EV-G4 靠游标补传）
- **乱序**：45' 点球给 C 注入 +4s 抖动，使其晚于 46' 黄牌到达
- **误报撤回**：B 的假黄牌/假进球/假红卡；A 官方源自己的假进球（验证权威比分回退）
- **特殊计分**：乌龙球记给对方、点球罚丢不得分

## 网页视图

1. **赛事列表**：比分（权威 + 待确认）、比赛阶段
2. **实时事件**：时间线，每事件展示三家投票、相对到达延迟、最快标记、补传/撤回标记，可按状态筛选
3. **数据源对比**：连接状态/掉线/补传统计 + 逐事件三家到达时间差表
4. **纠错记录**：撤回（RETRACT）、状态变更（STATUS_CHANGE）、比分回退（SCORE_ROLLBACK）流水
5. **赛后报告**：三家综合排名、准确率、延迟分布、逐事件到达顺序、全部纠错

## 校验结果

`py validation/replay.py` 当前输出 **23/23 通过**，终场权威比分 4:3，4 个误报全部 `RETRACTED`，
B/C 掉线事件均以 `backfilled` 补回，融合事件集合与剧本真值一一对应。
