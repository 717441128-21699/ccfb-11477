# 足球实时数据采集融合服务（本地模拟）

从三家模拟数据商（alpha / beta / gamma）的不同推送通道收足球比赛实时数据，
用 asyncio 做归一化、合并、确认、撤回与比分裁决，并通过实时网页展示。
**全部数据由本地剧本模拟，不接任何真实数据商。**

## 架构总览

```
                         ┌────────────────────────── 一个 aiohttp 进程 ──────────────────────────┐
 scripts/*.json 剧本      │                                                                          │
 ───────────────────►   模拟器 MatchSimulator                                                         │
                        ├─ /feed/alpha/ws    WebSocket（alpha，官方源，英文命名）                       │
                        ├─ /feed/beta/sse    SSE 长连接（beta，拼音缩写命名）                          │
                        └─ /feed/gamma/poll  长轮询 503/JSON（gamma，中文命名）                        │
                                 │  报文带单调 seq，支持 ?since_seq= 重连补漏                          │
                                 ▼                                                                     │
                        采集器 Collector（每种传输一个，指数退避重连 + seq 水位线补漏 + 归一化映射）     │
                                 ▼                                                                     │
                        融合引擎 FusionEngine                                                          │
                        （指纹合并 / 先到+多数确认 / 撤回 / 官方比分裁决 / 纠错留痕）                   │
                                 ▼                                                                     │
                        仪表盘 WebSocket /ws/{match_id}  ──实时推送──►  静态网页（五个标签页）          │
                        REST: /api/matches /api/state /api/report                                      │
                        └──────────────────────────────────────────────────────────────────────────┘
```

## 目录结构

```
src/fusion/
  config.py     三家接入配置、球队/球员/事件类型 → 统一编码映射、官方源与确认阈值
  models.py     SourceReport / Incident / Correction / MatchState 数据模型
  simulator.py  剧本驱动模拟器：延迟、乱序、误报+撤回、单家撤回、掉线窗口、append-only 补漏日志
  collector.py  WS / SSE / 长轮询三种采集器，重连退避、since_seq 补漏、私有命名归一化
  engine.py     融合引擎：合并确认、撤回、官方比分裁决、纠错记录
  stats.py      赛后报告：逐事件到达时间差、准确率、误报/撤回/漏报、各家排名
  app.py        aiohttp 应用：模拟端点、采集器托管、REST、仪表盘 WS、静态页
scripts/
  m001_*.json   全场景剧本（12 事件 + alpha 掉线 + 误报 + 单家撤回 + 错比分 + 官方后到）
  m002_*.json   短剧本（gamma 轮询掉线 + 误报）
  verify.py     端到端验证：离线直连引擎跑剧本，与真值逐事件比对（断言套件）
  smoke_http.py HTTP 冒烟：三种传输端点、补漏协议、REST、仪表盘 WS
static/         实时网页（index.html / app.css / app.js）
```

## 快速开始

```powershell
# 1. 建虚拟环境并安装（已完成可跳过）
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e .

# 2. 启动服务（模拟器 + 采集器 + 融合引擎 + 网页，同一进程）
.\.venv\Scripts\python.exe -I -m fusion.app
# 浏览器打开 http://127.0.0.1:8080
```

剧本在服务启动时自动开演，约 2 分钟跑完。网页五个标签页：

1. **赛事列表**：所有比赛卡片，点击切换
2. **实时事件**：融合时间线，每条事件显示三家投票、到达时刻、相对最快源的延迟、确认状态
3. **数据源对比**：逐事件三家到达时间差、报错比分、最快源
4. **纠错记录**：撤回 / 误报 / 状态变更 / 比分修正全程留痕
5. **赛后报告**：完赛后自动出现，各家准确率、平均/最大延迟、最快次数、误报漏报、综合排名

## 验证

```powershell
# 端到端：模拟器+采集器+引擎逐事件比对剧本真值（不经过网络，可重复跑）
$env:PYTHONPATH = (Get-Location).Path
.\.venv\Scripts\python.exe -I scripts\verify.py

# HTTP 冒烟（需先启动服务）
.\.venv\Scripts\python.exe -I scripts\smoke_http.py
```

## 关键设计与需求对应

| 需求 | 实现 |
| --- | --- |
| 三种推送 + 掉线重连 + 补漏 | `collector.py`：WS/SSE/Poll 三个客户端，指数退避重连；每家 feed 是 append-only 日志，重连带 `since_seq` 回放，采集器按 seq 去重 |
| 名称/类型统一编码 | `config.py` 三套私有命名 → 统一编码（如 `进球`/`goal_normal`/`GOAL` → `goal`），在采集器归一化，引擎只见统一编码 |
| 先到 + 多数确认合并 | `engine.py` 指纹匹配（类型/分钟/球队/球员，分钟 ±2 宽松匹配容忍计时偏差），先到建事件，有效来源 ≥2 家 → confirmed |
| 撤回要撤掉 | 撤回按 `(provider, ref_id)` 定位原报道并移出有效投票；投票归零 → retracted，误报与撤回分别写纠错记录 |
| 比分以官方源为准 | alpha 标记 `is_official`；比分类事件无官方有效报道时一律 pending 进 `pending_goals`、**不计比分**；官方到达后 official 并按官方声称比分裁决，非官方错比分（如 m001 e07 beta 报 2-2）被纠正 |
| 到达时间差 / 哪家快准 | 每条报道记录 `received_at`；`stats.py` 以同事件最早到达为基线算各家延迟，统计准确率/最快次数/误报漏报并排名 |
| 网页实时刷新 | 仪表盘 WebSocket 推送完整快照，断线自动降级 1.5s 轮询 |
| 赛后报告 | 终场自动生成 `/api/report/{id}`，网页第五页展示 |

## 剧本里的坑（均已被融合结果正确处理）

- **m001 e03**：gamma 凭空误报 23' 进球，6 秒后自己撤回 → 事件 retracted，不计比分，留 false_report
- **m001 e05**：beta 先报红牌后撤回，alpha/gamma 仍报 → 事件保持 confirmed，beta 投票标撤回
- **m001 e06**：alpha 官方源延迟到 58s 才报点球，且 55–61s 正好掉线 → beta/gamma 先报只标 pending，alpha 重连补漏后转 official 并计分
- **m001 e07**：beta 把乌龙球比分错报成 2-2 → 官方 3-1 裁决，记 score_revision
- **m001 drops**：alpha 掉线 6 秒，重连补回窗口内事件，seq 无缺口
- **m002 drops**：gamma 轮询掉线 6 秒（端点返回 503），窗口内的进球靠重连补漏到达
