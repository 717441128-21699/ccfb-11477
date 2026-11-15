"""采集器：三种推送方式的客户端，掉线自动重连，重连后按 seq 补回漏掉的事件。

  - WSCollector   : WebSocket（alpha，官方源）
  - SSECollector  : Server-Sent Events（beta）
  - PollCollector : 长轮询（gamma）
三家报文在这里完成私有命名 -> 统一编码的映射，输出 SourceReport 给融合引擎。
"""
from __future__ import annotations

import asyncio
import json
import random
from typing import Any, Callable, Awaitable

import aiohttp

from .config import (
    PROVIDERS, RECONNECT_INITIAL_DELAY, RECONNECT_MAX_DELAY,
    POLL_INTERVAL, HTTP_HOST, HTTP_PORT,
)
from .models import SourceReport, SourceStatus, EventType

BASE_URL = f"http://{HTTP_HOST}:{HTTP_PORT}"
Ingest = Callable[[SourceReport], Awaitable[None]]
StatusCb = Callable[[str, str], Awaitable[None]]


def build_url(provider: str, since_seq: int, match_id: str = "m001") -> str:
    cfg = PROVIDERS[provider]
    return f"{BASE_URL}{cfg.endpoint}?since_seq={since_seq}&match_id={match_id}"


def normalize(provider: str, entry: dict[str, Any]) -> SourceReport | None:
    """把一条原始日志（含 seq）转成统一 SourceReport。无法识别的类型打 unknown 兜底。"""
    cfg = PROVIDERS[provider]
    seq = entry["seq"]
    p = dict(entry["payload"])
    kind = p.get("kind", "report")
    emitted = p.pop("_emit_logged_at", 0.0)

    if kind == "retract":
        return SourceReport(
            seq=seq, provider=provider, retraction=True,
            ref_id=p.get("ref", ""), event_type=EventType.UNKNOWN.value,
            minute=0, team="", player="", player_in="", player_out="",
            score_home=None, score_away=None, raw=p,
            emitted_at=emitted or 0.0,
        )

    event_type = cfg.event_map.get(p.get("type", ""), EventType.UNKNOWN.value)
    score = p.get("score")
    return SourceReport(
        seq=seq, provider=provider, retraction=False,
        ref_id=p.get("id", ""), event_type=event_type,
        minute=int(p.get("minute", 0)),
        team=cfg.team_map.get(p.get("team", ""), p.get("team", "")),
        player=cfg.player_map.get(p.get("player", ""), p.get("player", "")),
        player_in=cfg.player_map.get(p.get("player_in", ""), p.get("player_in", "")),
        player_out=cfg.player_map.get(p.get("player_out", ""), p.get("player_out", "")),
        score_home=score[0] if score else None,
        score_away=score[1] if score else None,
        raw=p, emitted_at=emitted or 0.0,
    )


class BaseCollector:
    """重连退避 + seq 补漏 + 状态上报的公共骨架。"""

    def __init__(self, provider: str, ingest: Ingest, status_cb: StatusCb,
                 match_id: str = "m001"):
        self.provider = provider
        self.ingest = ingest
        self.status_cb = status_cb
        self.match_id = match_id
        self.last_seq = 0                       # 已处理的最大 seq，重连补漏的水位线
        self.report_cache: dict[str, SourceReport] = {}  # ref_id -> 报道，供撤回引用
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    def start(self) -> None:
        self._task = asyncio.create_task(self._run_forever(), name=f"collector-{self.provider}")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run_forever(self) -> None:
        delay = RECONNECT_INITIAL_DELAY
        await self.status_cb(self.provider, SourceStatus.RECONNECTING.value)
        while not self._stop.is_set():
            try:
                await self.status_cb(self.provider, SourceStatus.RECONNECTING.value)
                await self._session()
                delay = RECONNECT_INITIAL_DELAY          # 正常连上后重置退避
            except asyncio.CancelledError:
                raise
            except Exception as exc:                     # 网络断开 / 503 / 协议错误
                await self.status_cb(self.provider, SourceStatus.DISCONNECTED.value)
                if self._stop.is_set():
                    break
                wait = min(delay, RECONNECT_MAX_DELAY) * random.uniform(0.8, 1.2)
                print(f"[collector:{self.provider}] 连接中断({exc!r})，{wait:.1f}s 后重连")
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=wait)
                except asyncio.TimeoutError:
                    pass
                delay = min(delay * 2, RECONNECT_MAX_DELAY)

    async def _session(self) -> None:
        raise NotImplementedError

    async def _handle_entries(self, entries: list[dict[str, Any]]) -> None:
        for entry in entries:
            if entry.get("_connection_lost"):
                raise ConnectionError("服务端主动断开（模拟掉线窗口）")
            report = normalize(self.provider, entry)
            if report is None:
                continue
            if not report.retraction:
                self.report_cache[report.ref_id] = report
            else:
                original = self.report_cache.get(report.ref_id)
                if original:
                    report.event_type = original.event_type
                    report.minute = original.minute
                    report.team = original.team
                    report.player = original.player
                    report.player_in = original.player_in
                    report.player_out = original.player_out
            self.last_seq = max(self.last_seq, report.seq)
            await self.ingest(report)


class WSCollector(BaseCollector):
    async def _session(self) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(build_url(self.provider, self.last_seq, self.match_id)) as ws:
                await self.status_cb(self.provider, SourceStatus.CONNECTED.value)
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        if isinstance(data, list):
                            await self._handle_entries(data)
                        else:
                            await self._handle_entries([data])
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        raise ConnectionError("WebSocket 关闭")


class SSECollector(BaseCollector):
    async def _session(self) -> None:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as session:
            async with session.get(build_url(self.provider, self.last_seq, self.match_id)) as resp:
                if resp.status == 503:
                    raise ConnectionError("SSE 源掉线中（503）")
                resp.raise_for_status()
                await self.status_cb(self.provider, SourceStatus.CONNECTED.value)
                buffer = ""
                async for chunk in resp.content.iter_any():
                    buffer += chunk.decode("utf-8")
                    blocks = buffer.split("\n\n")
                    buffer = blocks.pop()
                    for block in blocks:
                        for line in block.splitlines():
                            if line.startswith("data:"):
                                await self._handle_entries([json.loads(line[5:].strip())])


class PollCollector(BaseCollector):
    async def _session(self) -> None:
        async with aiohttp.ClientSession() as session:
            await self.status_cb(self.provider, SourceStatus.CONNECTED.value)
            while not self._stop.is_set():
                async with session.get(build_url(self.provider, self.last_seq, self.match_id)) as resp:
                    if resp.status == 503:
                        raise ConnectionError("轮询源掉线中（503）")
                    resp.raise_for_status()
                    data = await resp.json()
                if data["entries"]:
                    await self._handle_entries(data["entries"])
                await asyncio.sleep(POLL_INTERVAL)
