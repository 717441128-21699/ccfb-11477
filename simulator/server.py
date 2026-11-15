"""模拟器 HTTP 服务：每家来源一个 WS 或长轮询端点，掉线窗口内 WS 拒绝/断开、长轮询 503。"""
from __future__ import annotations

import logging

from aiohttp import web

from .core import SimulatorCore

CORE_KEY = web.AppKey("core", "SimulatorCore")

log = logging.getLogger("simulator.server")


def build_app(core: SimulatorCore) -> web.Application:
    app = web.Application()
    app[CORE_KEY] = core

    async def health(_request: web.Request) -> web.Response:
        return web.json_response({
            "match_id": core.match_id,
            "sources": {
                sid: {
                    "transport": spec["transport"],
                    "buffered": len(core.states[sid].buffer),
                    "finished": core.states[sid].finished,
                }
                for sid, spec in core.scenario["sources"].items()
            },
        })

    app.router.add_get("/health", health)

    ws_sources = {spec["path"]: sid for sid, spec in core.scenario["sources"].items()
                  if spec["transport"] == "websocket"}
    poll_sources = {spec["path"]: sid for sid, spec in core.scenario["sources"].items()
                    if spec["transport"] == "long_poll"}

    async def ws_handler(request: web.Request) -> web.WebSocketResponse:
        sid = ws_sources.get(request.path)
        core: SimulatorCore = request.app[CORE_KEY]
        state = core.states[sid]
        if core.is_in_drop(sid, _now()):
            # 掉线窗口内拒绝新连接，客户端按 503 退避重连
            raise web.HTTPServiceUnavailable(text=f"source {sid} offline")
        ws = web.WebSocketResponse(heartbeat=10)
        await ws.prepare(request)
        # 重连补传：resume_from_seq 之后的缓冲报文全部重放
        resume = request.query.get("resume_from_seq")
        if resume is not None:
            for env in core.replay_since(sid, int(resume)):
                await ws.send_json(env)
        state.ws_clients.add(ws)
        log.info("[%s] WebSocket 客户端接入 (resume=%s)", sid, resume)
        try:
            async for msg in ws:
                # 模拟器不需要客户端消息，读循环仅为感知断开
                if msg.type == 1:  # TEXT
                    continue
        finally:
            state.ws_clients.discard(ws)
            log.info("[%s] WebSocket 客户端断开", sid)
        return ws

    async def poll_handler(request: web.Request) -> web.Response:
        sid = poll_sources.get(request.path)
        core: SimulatorCore = request.app[CORE_KEY]
        since = int(request.query.get("since", "-1"))
        wait_ms = int(request.query.get("wait_ms", "5000"))
        try:
            payload = await core.long_poll(sid, since, wait_ms)
        except ConnectionError:
            raise web.HTTPServiceUnavailable(text=f"source {sid} offline")
        return web.json_response(payload)

    for path in ws_sources:
        app.router.add_get(path, ws_handler)
    for path in poll_sources:
        app.router.add_get(path, poll_handler)
    return app


def _now() -> int:
    import time
    return int(time.time() * 1000)
