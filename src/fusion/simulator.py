"""本地数据商模拟器。

按预设剧本（scripts/*.json）为三家数据商生成推送，各自使用自己的命名体系，
采集端必须做归一化映射。模拟能力：
  - 固定/随机延迟（delay）
  - 乱序到达（用 at_sec 覆盖发送时刻，使报文交叉）
  - 误报 + 限时撤回（false_report / retract_after）
  - 真实事件被单家撤回（reports 里的 retract_after）
  - 掉线窗口（drops）：WS/SSE 主动断连、轮询返回 503，窗口内拒绝新连接
  - 断线补漏：每家维护 append-only 日志，客户端带 since_seq 重连后回放
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import time
from pathlib import Path
from typing import Any

from .config import PROVIDERS


class ProviderFeed:
    """单家数据商、单场比赛的推送日志与连接管理。"""

    def __init__(self, provider: str, match_id: str):
        self.provider = provider
        self.match_id = match_id
        self.log: list[dict[str, Any]] = []      # append-only，seq = 下标 + 1
        self.updated = asyncio.Event()
        self.down_until = 0.0                    # epoch 秒；在此之前拒绝/断开连接
        self._waiters: list[asyncio.Queue] = []  # 实时订阅队列（WS/SSE）

    @property
    def is_down(self) -> bool:
        return time.monotonic() < self.down_until

    def append(self, payload: dict[str, Any]) -> dict[str, Any]:
        entry = {"seq": len(self.log) + 1, "payload": payload}
        self.log.append(entry)
        for q in self._waiters:
            q.put_nowait(entry)
        self.updated.set()
        return entry

    def replay(self, since_seq: int, limit: int | None = None) -> list[dict[str, Any]]:
        entries = self.log[since_seq:]
        return entries if limit is None else entries[:limit]

    @contextlib.asynccontextmanager
    async def subscribe(self):
        q: asyncio.Queue = asyncio.Queue(maxsize=200)
        self._waiters.append(q)
        try:
            yield q
        finally:
            self._waiters.remove(q)


class MatchSimulator:
    """一场比赛的剧本驱动模拟器。"""

    def __init__(self, script: dict[str, Any]):
        self.script = script
        self.match_id = script["match_id"]
        self.home = script["home"]
        self.away = script["away"]
        self.feeds: dict[str, ProviderFeed] = {
            name: ProviderFeed(name, self.match_id) for name in PROVIDERS
        }
        self.t0: float | None = None
        self._tasks: list[asyncio.Task] = []
        self._stopped = asyncio.Event()

    # ---- 报文翻译：统一编码 -> 该家私有词汇 ---------------------------------

    def _provider_vocab(self, provider: str) -> tuple[dict, dict, dict]:
        cfg = PROVIDERS[provider]
        inv_event = {v: k for k, v in cfg.event_map.items()}
        inv_team = {v: k for k, v in cfg.team_map.items()}
        inv_player = {v: k for k, v in cfg.player_map.items()}
        return inv_event, inv_team, inv_player

    def _build_payload(self, provider: str, ev: dict[str, Any], kind: str,
                       ref: str | None = None) -> dict[str, Any]:
        inv_event, inv_team, inv_player = self._provider_vocab(provider)
        if kind == "retract":
            return {"kind": "retract", "provider": provider,
                    "match": self.match_id, "ref": ref, "at": time.time()}
        p: dict[str, Any] = {
            "kind": "report",
            "id": f"{provider}#{ev['id']}",
            "provider": provider,
            "match": self.match_id,
            "type": inv_event.get(ev["type"], ev["type"]),
            "minute": ev["minute"],
            "team": inv_team.get(ev["team"], ev["team"]),
        }
        if ev.get("player"):
            p["player"] = inv_player.get(ev["player"], ev["player"])
        if ev.get("player_in"):
            p["player_in"] = inv_player.get(ev["player_in"], ev["player_in"])
        if ev.get("player_out"):
            p["player_out"] = inv_player.get(ev["player_out"], ev["player_out"])
        if "score" in ev:
            p["score"] = list(ev["score"])
        return p

    # ---- 剧本展开 -------------------------------------------------------------

    def _planned_sends(self) -> dict[str, list[tuple[float, dict[str, Any]]]]:
        """把剧本展开成 {provider: [(相对发送秒, payload), ...]}，按发送时刻排序。"""
        sends: dict[str, list[tuple[float, dict[str, Any]]]] = {n: [] for n in PROVIDERS}
        for ev in sorted(self.script["events"], key=lambda e: (e["at_sec"], e["id"])):
            if "false_report" in ev:
                fr = ev["false_report"]
                provider = fr["provider"]
                ref = f"{provider}#{ev['id']}"
                emits_at = ev["at_sec"] + fr.get("delay", 0.0)
                sends[provider].append(
                    (emits_at, self._build_payload(provider, ev, "report")))
                sends[provider].append(
                    (emits_at + fr["retract_after"],
                     self._build_payload(provider, ev, "retract", ref)))
                continue
            for provider, spec in ev.get("reports", {}).items():
                ref = f"{provider}#{ev['id']}"
                emits_at = spec.get("at_sec", ev["at_sec"] + spec.get("delay", 0.0))
                payload = self._build_payload(provider, ev, "report")
                if "score" in spec:                       # 该家报错比分
                    payload["score"] = list(spec["score"])
                sends[provider].append((emits_at, payload))
                if "retract_after" in spec:              # 单家撤回真实报道
                    sends[provider].append(
                        (emits_at + spec["retract_after"],
                         self._build_payload(provider, ev, "retract", ref)))
        for provider in sends:
            sends[provider].sort(key=lambda x: x[0])
        return sends

    async def start(self) -> None:
        self.t0 = time.monotonic()
        loop = asyncio.get_running_loop()
        sends = self._planned_sends()

        async def emit_at(provider: str, at_sec: float, payload: dict[str, Any]):
            await asyncio.sleep(max(0.0, self.t0 + at_sec - time.monotonic()))
            if self._stopped.is_set():
                return
            entry = self.feeds[provider].append(payload)
            entry["payload"]["_emit_logged_at"] = time.time()

        for provider, items in sends.items():
            for at_sec, payload in items:
                self._tasks.append(loop.create_task(emit_at(provider, at_sec, payload)))

        for drop in self.script.get("drops", []):
            self._tasks.append(loop.create_task(self._drop_window(drop)))

    async def _drop_window(self, drop: dict[str, Any]):
        await asyncio.sleep(max(0.0, self.t0 + drop["at_sec"] - time.monotonic()))
        feed = self.feeds[drop["provider"]]
        feed.down_until = time.monotonic() + drop["down_sec"]
        # 踢掉当前在线的实时连接，触发采集器重连
        feed.updated.set()
        for q in list(feed._waiters):
            q.put_nowait({"_connection_lost": True})

    async def stop(self) -> None:
        self._stopped.set()
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await t


def load_scripts(scripts_dir: str | Path) -> list[dict[str, Any]]:
    scripts = []
    for path in sorted(Path(scripts_dir).glob("*.json")):
        with open(path, encoding="utf-8") as f:
            scripts.append(json.load(f))
    return scripts
