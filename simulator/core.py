"""模拟器核心：调度发送、掉线窗口、WebSocket 推送/补传、长轮询游标。"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from typing import Optional

from .encoders import EnvelopeBuilder
from .scheduler import Scheduler

log = logging.getLogger("simulator")


def _now_ms() -> int:
    return int(time.time() * 1000)


class SourceState:
    def __init__(self, source_id: str):
        self.source_id = source_id
        # 已生成的全部报文（seq 升序），重连补传与长轮询游标共用
        self.buffer: list[dict] = []
        self.next_seq = 0
        self.ws_clients: set[object] = set()
        # 长轮询等待者：(since_cursor, future)
        self.poll_waiters: list[asyncio.Future] = []
        self.finished = False


class SimulatorCore:
    def __init__(self, scenario: dict, time_scale: float):
        self.scenario = scenario
        self.match_id = scenario["match_id"]
        t0 = _now_ms() + 200  # 留 200ms 启动余量
        self.scheduler = Scheduler(scenario, time_scale, t0)
        self.builder = EnvelopeBuilder(scenario)
        self.states: dict[str, SourceState] = {
            sid: SourceState(sid) for sid in scenario["sources"]
        }
        self.drop_windows = self.scheduler.drop_windows()
        self._task: Optional[asyncio.Task] = None
        self._stopping = asyncio.Event()

    # ------------------------- 生命周期 -------------------------

    def start(self) -> None:
        self._task = asyncio.create_task(self._run_schedule(), name="sim-schedule")
        self._drop_task = asyncio.create_task(self._run_drop_windows(), name="sim-drops")

    async def stop(self) -> None:
        self._stopping.set()
        for t in (self._task, getattr(self, "_drop_task", None)):
            if t:
                t.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await t

    async def _run_drop_windows(self) -> None:
        """掉线窗口开始时主动踢掉 WS 客户端；长轮询由 is_in_drop 返回 503。"""
        starts: list[tuple[int, str]] = []
        for sid, wins in self.drop_windows.items():
            for start, _end in wins:
                starts.append((start, sid))
        for start, sid in sorted(starts):
            delay = (start - _now_ms()) / 1000.0
            if delay > 0:
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._stopping.wait(), timeout=delay)
                if self._stopping.is_set():
                    return
            state = self.states[sid]
            log.warning("[%s] 掉线窗口开始，主动断开 %d 个 WS 连接", sid, len(state.ws_clients))
            for ws in list(state.ws_clients):
                with contextlib.suppress(Exception):
                    await ws.close(code=1011, message=b"simulated outage")
            state.ws_clients.clear()
            # 挂起的长轮询也立即失败
            for fut in state.poll_waiters:
                if not fut.done():
                    fut.set_exception(ConnectionError("drop window"))
            state.poll_waiters.clear()

    def is_in_drop(self, source_id: str, at_ms: int) -> bool:
        return any(start <= at_ms < end for start, end in self.drop_windows[source_id])

    # ------------------------- 发送循环 -------------------------

    async def _run_schedule(self) -> None:
        items = list(self.scheduler.items)
        for item in items:
            delay = item.send_at_ms - _now_ms()
            if delay > 0:
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._stopping.wait(), timeout=delay / 1000.0)
                if self._stopping.is_set():
                    return
            state = self.states[item.source_id]
            item.seq = state.next_seq
            state.next_seq += 1
            envelope = self.builder.build(item.source_id, item.__dict__, item.seq,
                                          ts_ms=_now_ms())
            state.buffer.append(envelope)
            if self.is_in_drop(item.source_id, _now_ms()):
                # 掉线窗口内：报文只进 buffer，不实时推送，等客户端重连补传
                log.info("[%s] seq=%s 落在掉线窗口，仅入缓冲区等待补传: %s",
                         item.source_id, item.seq, item.truth_id)
                continue
            await self._push_ws(state, envelope)
            self._resolve_pollers(state)
        # 全部计划发完（含撤回），标记终场
        await asyncio.sleep(0.2)
        for state in self.states.values():
            state.finished = True
            self._resolve_pollers(state)
        log.info("剧本全部推送完成")

    async def _push_ws(self, state: SourceState, envelope: dict) -> None:
        dead = []
        for ws in list(state.ws_clients):
            try:
                await ws.send_json(envelope)
            except Exception:
                dead.append(ws)
        for ws in dead:
            state.ws_clients.discard(ws)

    def _resolve_pollers(self, state: SourceState) -> None:
        for fut in list(state.poll_waiters):
            if not fut.done():
                fut.set_result(None)
        state.poll_waiters.clear()

    # ------------------------- WebSocket 接口 -------------------------

    def replay_since(self, source_id: str, resume_from_seq: Optional[int]) -> list[dict]:
        """重连补传：返回 seq > resume_from_seq 的全部缓冲报文。"""
        state = self.states[source_id]
        if resume_from_seq is None:
            return []
        missed = [dict(env, backfilled=(env.get("backfilled") or True))
                  for env in state.buffer if env["seq"] > resume_from_seq]
        if missed:
            log.info("[%s] 补传 %d 条报文 (seq>%s)", source_id, len(missed), resume_from_seq)
        return missed

    # ------------------------- 长轮询接口 -------------------------

    async def long_poll(self, source_id: str, since: int, wait_ms: int) -> dict:
        state = self.states[source_id]
        if self.is_in_drop(source_id, _now_ms()):
            raise ConnectionError("drop window")

        def collect(mark_backfill: bool) -> list[dict]:
            out = []
            for env in state.buffer:
                if env["seq"] <= since:
                    continue
                env = dict(env)
                # 仅重连（since>=0）时，请求开始前已缓冲的报文属于漏掉的补传
                if mark_backfill and since >= 0 and not env.get("backfilled"):
                    env["backfilled"] = True
                out.append(env)
            return out

        updates = collect(mark_backfill=True)
        if not updates and not state.finished:
            fut = asyncio.get_running_loop().create_future()
            state.poll_waiters.append(fut)
            try:
                await asyncio.wait_for(fut, timeout=wait_ms / 1000.0)
            except asyncio.TimeoutError:
                pass
            finally:
                with contextlib.suppress(ValueError):
                    state.poll_waiters.remove(fut)
            if self.is_in_drop(source_id, _now_ms()):
                raise ConnectionError("drop window")
            # 等待期间新到的报文是实时的，不打补传标记
            updates = collect(mark_backfill=False)
        return {"updates": updates, "finished": state.finished}
