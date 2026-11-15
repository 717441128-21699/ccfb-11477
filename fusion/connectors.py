"""数据商连接器：WebSocket（A/B）与长轮询（C）。

统一能力：
- 自动重连，指数退避（0.5s 起，上限 10s）；
- 重连后通过各自协议拉取断线期间漏掉的事件，补传消息标记 backfilled；
- 原生报文经适配器映射为 Report 后交给 FusionEngine；
- 传输层连接/断开/补传统计回写引擎。
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Optional

import aiohttp

import config
from .engine import FusionEngine, now_ms
from .mapping import BaseAdapter, MappingError
from .models import CONTROL_EVENTS, Report

log = logging.getLogger("connectors")


class BaseConnector:
    transport_name = "base"

    def __init__(self, source_id: str, base_url: str, path: str, match_id: str,
                 adapter: BaseAdapter, engine: FusionEngine):
        self.source_id = source_id
        self.base_url = base_url.rstrip("/")
        self.path = path
        self.match_id = match_id
        self.adapter = adapter
        self.engine = engine
        self._task: Optional[asyncio.Task] = None
        self._stopping = asyncio.Event()
        self._backoff = config.BACKOFF_BASE

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"connector-{self.source_id}")

    async def stop(self) -> None:
        self._stopping.set()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def _run(self) -> None:
        await self.engine.mark_transport(self.match_id, self.source_id, connected=False)
        async with aiohttp.ClientSession() as session:
            while not self._stopping.is_set():
                try:
                    await self._connect_and_stream(session)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # 连接断开/503/协议错误均走重连
                    tr = self.engine.matches[self.match_id].transport[self.source_id]
                    tr["errors"] += 1
                    if tr["connected"]:
                        tr["disconnect_count"] += 1
                    await self.engine.mark_transport(self.match_id, self.source_id, connected=False)
                    log.warning("[%s] 连接中断: %r，%.1fs 后重连", self.source_id, exc, self._backoff)
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._stopping.wait(), timeout=self._backoff)
                if not self._stopping.is_set():
                    self._backoff = min(config.BACKOFF_CAP, self._backoff * 2)

    async def _on_connected(self) -> None:
        """握手/首次成功响应后记账：状态置在线并累加连接次数。"""
        tr = self.engine.matches[self.match_id].transport[self.source_id]
        first = not tr["connected"]
        tr["connected"] = True
        if first:
            tr["connect_count"] += 1
        await self.engine.mark_transport(self.match_id, self.source_id, connected=True)

    async def _connect_and_stream(self, session: aiohttp.ClientSession) -> None:
        raise NotImplementedError

    async def _deliver(self, envelope: dict) -> None:
        try:
            report: Report = self.adapter.parse(envelope)
        except MappingError as exc:
            tr = self.engine.matches[self.match_id].transport[self.source_id]
            tr["mapping_errors"] += 1
            log.warning("[%s] 映射失败，丢弃报文: %s | %r", self.source_id, exc, envelope)
            return
        # 成功收包即说明链路健康，退避归零
        self._backoff = config.BACKOFF_BASE
        if report.event_type in CONTROL_EVENTS:
            await self.engine.apply_control(report)
        await self.engine.handle_report(report)


class WSConnector(BaseConnector):
    transport_name = "websocket"

    async def _connect_and_stream(self, session: aiohttp.ClientSession) -> None:
        tr = self.engine.matches[self.match_id].transport[self.source_id]
        last_seq = tr["last_seq"]
        url = f"{self.base_url}{self.path}"
        params = {"match_id": self.match_id}
        if last_seq is not None:
            params["resume_from_seq"] = str(last_seq)
            log.info("[%s] WebSocket 重连，请求补传 seq>%s", self.source_id, last_seq)
        async with session.ws_connect(url, params=params, heartbeat=10) as ws:
            await self._on_connected()
            async for msg in ws:
                if self._stopping.is_set():
                    break
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._deliver(msg.json())
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    raise ConnectionError(f"WebSocket {msg.type}")


class LongPollConnector(BaseConnector):
    transport_name = "long_poll"

    async def _connect_and_stream(self, session: aiohttp.ClientSession) -> None:
        tr = self.engine.matches[self.match_id].transport[self.source_id]
        url = f"{self.base_url}{self.path}"
        # 游标：重连后用最后收到的 seq 让服务端补传漏掉的更新
        cursor = tr["last_seq"] if tr["last_seq"] is not None else -1
        if cursor >= 0:
            log.info("[%s] 长轮询重连，since=%s 补传缺口", self.source_id, cursor)
        connected = False
        while not self._stopping.is_set():
            params = {"match_id": self.match_id, "since": str(cursor),
                      "wait_ms": str(config.SIM_POLL_WAIT_MS)}
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status == 503:
                    raise ConnectionError("长轮询服务端处于掉线窗口 (503)")
                resp.raise_for_status()
                payload = await resp.json()
            if not connected:
                await self._on_connected()
                connected = True
            updates = payload.get("updates", [])
            for env in updates:
                await self._deliver(env)
                if env.get("seq") is not None:
                    cursor = max(cursor, env["seq"])
            # 服务端告知终场且无新消息时优雅结束
            if payload.get("finished") and not updates:
                return


def build_connectors(scenario: dict, sim_base_url: str, engine: FusionEngine,
                     adapters: dict[str, BaseAdapter]) -> list[BaseConnector]:
    classes = {"websocket": WSConnector, "long_poll": LongPollConnector}
    out: list[BaseConnector] = []
    for sid, spec in scenario["sources"].items():
        cls = classes[spec["transport"]]
        out.append(cls(sid, sim_base_url, spec["path"], scenario["match_id"],
                       adapters[sid], engine))
    return out
