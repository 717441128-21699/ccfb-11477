"""aiohttp 应用：模拟数据商端点、融合引擎托管、REST API、网页实时推送。"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from aiohttp import web, WSMsgType

from .config import PROVIDERS, HTTP_HOST, HTTP_PORT
from .simulator import MatchSimulator, load_scripts
from .collector import WSCollector, SSECollector, PollCollector
from .engine import FusionEngine
from .stats import build_match_report
from .models import SourceReport, SourceStatus


class Runtime:
    """一场比赛的模拟器 + 引擎 + 三家采集器。"""

    def __init__(self, script: dict[str, Any]):
        self.script = script
        self.sim = MatchSimulator(script)
        self.engine = FusionEngine(script["match_id"], script["home"], script["away"])
        self.status: dict[str, str] = {p: SourceStatus.DISCONNECTED.value for p in PROVIDERS}
        self.collectors: dict[str, Any] = {}
        self.ws_clients: set[web.WebSocketResponse] = set()
        self._broadcast_lock = asyncio.Lock()
        self.report_ready = asyncio.Event()

    async def start(self) -> None:
        await self.sim.start()
        for name, cfg in PROVIDERS.items():
            cls = {"websocket": WSCollector, "sse": SSECollector,
                   "long_poll": PollCollector}[cfg.transport]
            collector = cls(name, self.on_report, self.on_status,
                            match_id=self.script["match_id"])
            self.collectors[name] = collector
            collector.start()

    async def stop(self) -> None:
        for c in self.collectors.values():
            await c.stop()
        await self.sim.stop()

    async def on_report(self, report: SourceReport) -> None:
        await self.engine.ingest(report)
        await self.broadcast()
        if self.engine.state.status == "finished":
            self.report_ready.set()

    async def on_status(self, provider: str, status: str) -> None:
        self.status[provider] = status
        await self.broadcast()

    async def broadcast(self) -> None:
        payload = json.dumps(self.snapshot(), ensure_ascii=False)
        dead = []
        async with self._broadcast_lock:
            for ws in list(self.ws_clients):
                try:
                    await ws.send_str(payload)
                except ConnectionError:
                    dead.append(ws)
        for ws in dead:
            self.ws_clients.discard(ws)

    def snapshot(self) -> dict[str, Any]:
        return {
            "kind": "state",
            "match_id": self.script["match_id"],
            "label": self.script.get("label", self.script["match_id"]),
            "source_status": dict(self.status),
            "provider_meta": {
                p: {"transport": c.transport, "is_official": c.is_official}
                for p, c in PROVIDERS.items()
            },
            "state": self.engine.snapshot(),
            "report": build_match_report(self.script, self.engine.snapshot())
                      if self.engine.state.status == "finished" else None,
        }


RUNTIMES: dict[str, Runtime] = {}
# src/fusion/app.py -> 上溯三级到项目根
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
STATIC_DIR = PROJECT_ROOT / "static"


# ---------------- 模拟数据商端点（被采集器访问） -------------------------------

def _get_feed(request: web.Request) -> tuple[Any, int]:
    provider = request.match_info["provider"]
    if provider not in PROVIDERS:
        raise web.HTTPNotFound(reason=f"unknown provider {provider}")
    match_id = request.query.get("match_id", "m001")
    if match_id not in RUNTIMES:
        raise web.HTTPNotFound(reason=f"unknown match {match_id}")
    runtime = RUNTIMES[match_id]
    feed = runtime.sim.feeds[provider]
    since = int(request.query.get("since_seq", "0"))
    return feed, since


async def feed_ws(request: web.Request) -> web.WebSocketResponse:
    feed, since = _get_feed(request)
    ws = web.WebSocketResponse(heartbeat=15)
    await ws.prepare(request)
    if feed.is_down:
        await ws.close(code=1013, message=b"feed down window")
        return ws
    backlog = feed.replay(since)
    if backlog:
        await ws.send_str(json.dumps(backlog, ensure_ascii=False))
    async with feed.subscribe() as q:
        try:
            while True:
                entry = await q.get()
                if entry.get("_connection_lost") or feed.is_down:
                    await ws.close(code=1013, message=b"simulated outage")
                    break
                await ws.send_str(json.dumps(entry, ensure_ascii=False))
        except ConnectionResetError:
            pass
    return ws


async def feed_sse(request: web.Request) -> web.StreamResponse:
    feed, since = _get_feed(request)
    if feed.is_down:
        return web.json_response({"error": "feed down window"}, status=503)
    resp = web.StreamResponse(
        status=200,
        headers={"Content-Type": "text/event-stream; charset=utf-8",
                 "Cache-Control": "no-cache", "Connection": "keep-alive"})
    await resp.prepare(request)

    async def send_entry(entry: dict[str, Any]) -> bool:
        data = json.dumps(entry, ensure_ascii=False)
        try:
            await resp.write(f"data: {data}\n\n".encode("utf-8"))
            return True
        except ConnectionResetError:
            return False

    for entry in feed.replay(since):
        if not await send_entry(entry):
            return resp
    async with feed.subscribe() as q:
        while True:
            entry = await q.get()
            if entry.get("_connection_lost") or feed.is_down:
                await resp.write(b"event: error\ndata: {\"reason\":\"simulated outage\"}\n\n")
                break
            if not await send_entry(entry):
                break
    return resp


async def feed_poll(request: web.Request) -> web.Response:
    feed, since = _get_feed(request)
    if feed.is_down:
        return web.json_response({"error": "feed down window"}, status=503)
    limit = int(request.query.get("limit", "50"))
    return web.json_response({
        "provider": feed.provider,
        "last_seq": len(feed.log),
        "entries": feed.replay(since, limit=limit),
    })


# ---------------- 网页/REST API -----------------------------------------------

async def api_matches(request: web.Request) -> web.Response:
    return web.json_response([
        {"match_id": rt.script["match_id"], "label": rt.script.get("label", rt.script["match_id"]),
         "status": rt.engine.state.status,
         "score": [rt.engine.state.score_home, rt.engine.state.score_away]}
        for rt in RUNTIMES.values()
    ])


async def api_state(request: web.Request) -> web.Response:
    runtime = RUNTIMES[request.match_info["match_id"]]
    return web.json_response(runtime.snapshot())


async def api_report(request: web.Request) -> web.Response:
    runtime = RUNTIMES[request.match_info["match_id"]]
    return web.json_response(
        build_match_report(runtime.script, runtime.engine.snapshot()))


async def ws_dashboard(request: web.Request) -> web.WebSocketResponse:
    runtime = RUNTIMES[request.match_info["match_id"]]
    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(request)
    runtime.ws_clients.add(ws)
    await ws.send_str(json.dumps(runtime.snapshot(), ensure_ascii=False))
    try:
        async for msg in ws:
            if msg.type == WSMsgType.ERROR:
                break
    finally:
        runtime.ws_clients.discard(ws)
    return ws


async def index(request: web.Request) -> web.StreamResponse:
    return web.FileResponse(STATIC_DIR / "index.html")


def build_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_static("/static", STATIC_DIR, name="static")

    # 三家数据商的模拟端点（provider 参数化，传输方式由各处理器区分）
    app.router.add_get("/feed/{provider}/ws", feed_ws)
    app.router.add_get("/feed/{provider}/sse", feed_sse)
    app.router.add_get("/feed/{provider}/poll", feed_poll)

    # 仪表盘 API
    app.router.add_get("/api/matches", api_matches)
    app.router.add_get("/api/state/{match_id}", api_state)
    app.router.add_get("/api/report/{match_id}", api_report)
    app.router.add_get("/ws/{match_id}", ws_dashboard)
    return app


async def _on_startup(app: web.Application) -> None:
    scripts = load_scripts(SCRIPTS_DIR)
    for script in scripts:
        runtime = Runtime(script)
        RUNTIMES[script["match_id"]] = runtime
        await runtime.start()
    print(f"[server] 已启动 {len(RUNTIMES)} 场比赛的模拟与融合")


async def _on_cleanup(app: web.Application) -> None:
    for runtime in RUNTIMES.values():
        await runtime.stop()


def main() -> None:
    app = build_app()
    app.on_startup.append(_on_startup)
    app.on_cleanup.append(_on_cleanup)
    web.run_app(app, host=HTTP_HOST, port=HTTP_PORT, print=None)


if __name__ == "__main__":
    main()
