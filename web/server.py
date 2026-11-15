"""融合服务网页：REST 快照接口 + WebSocket 实时推送。"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from pathlib import Path

from aiohttp import web

from fusion.engine import FusionEngine
from fusion.mapping import MappingCatalog

log = logging.getLogger("web.server")
STATIC_DIR = Path(__file__).parent / "static"

ENGINE_KEY = web.AppKey("engine", FusionEngine)
CATALOG_KEY = web.AppKey("catalog", MappingCatalog)
SCENARIO_KEY = web.AppKey("scenario", dict)
PLAYERS_KEY = web.AppKey("player_names", dict)
TEAMS_KEY = web.AppKey("team_names", dict)


def build_app(engine: FusionEngine, catalog: MappingCatalog, scenario: dict) -> web.Application:
    app = web.Application()
    app[ENGINE_KEY] = engine
    app[CATALOG_KEY] = catalog
    app[SCENARIO_KEY] = scenario
    app[PLAYERS_KEY] = {p["id"]: p["canonical"] for p in scenario["players"]}
    app[TEAMS_KEY] = {
        scenario["home_team"]["id"]: scenario["home_team"]["canonical"],
        scenario["away_team"]["id"]: scenario["away_team"]["canonical"],
    }

    def decorate(ev_dict: dict) -> dict:
        ev_dict["team_name"] = app[TEAMS_KEY].get(ev_dict.get("team_id"))
        ev_dict["player_name"] = app[PLAYERS_KEY].get(ev_dict.get("player_id"))
        ev_dict["player_on_name"] = app[PLAYERS_KEY].get(ev_dict.get("player_on_id"))
        ev_dict["player_off_name"] = app[PLAYERS_KEY].get(ev_dict.get("player_off_id"))
        for v in ev_dict["votes"]:
            v["team_name"] = app[TEAMS_KEY].get(v.get("team_id"))
            v["player_name"] = app[PLAYERS_KEY].get(v.get("player_id"))
        return ev_dict

    async def list_matches(_request: web.Request) -> web.Response:
        return web.json_response({"matches": [m.to_summary() for m in engine.matches.values()]})

    async def match_detail(request: web.Request) -> web.Response:
        m = engine.get_match(request.match_info["match_id"])
        if m is None:
            raise web.HTTPNotFound()
        return web.json_response({
            "summary": m.to_summary(),
            "transport": m.transport,
            "events": [decorate(m.find_event(e.fused_id).to_dict()) for e in m.events],
            "corrections": [c.to_dict() for c in m.corrections],
            "player_names": app[PLAYERS_KEY],
            "team_names": app[TEAMS_KEY],
            "official_source": engine.official_source,
        })

    async def report(request: web.Request) -> web.Response:
        m = engine.get_match(request.match_info["match_id"])
        if m is None:
            raise web.HTTPNotFound()
        if m.phase != "FINISHED":
            raise web.HTTPConflict(text="比赛尚未结束，报告在终场后生成")
        return web.json_response(engine.build_report(m.match_id))

    async def ws_push(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=15)
        await ws.prepare(request)
        queue = engine.subscribe()
        log.info("网页客户端接入实时推送")
        try:
            # 首帧：全量快照，后续只推增量
            m = next(iter(engine.matches.values()), None)
            if m is not None:
                await ws.send_json({
                    "kind": "snapshot",
                    "match_id": m.match_id,
                    "data": {
                        "summary": m.to_summary(),
                        "transport": m.transport,
                    },
                })
            while True:
                msg = await queue.get()
                await ws.send_json(msg)
        finally:
            engine.unsubscribe(queue)
            log.info("网页客户端断开")
        return ws

    app.router.add_get("/api/matches", list_matches)
    app.router.add_get("/api/matches/{match_id}", match_detail)
    app.router.add_get("/api/matches/{match_id}/report", report)
    app.router.add_get("/ws", ws_push)
    app.router.add_static("/static", STATIC_DIR, name="static")

    async def index(_request: web.Request) -> web.Response:
        return web.FileResponse(STATIC_DIR / "index.html")

    app.router.add_get("/", index)
    return app
