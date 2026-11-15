"""统一启动入口。

用法：
  py runner.py all                    # 模拟器 + 融合采集 + 网页（默认）
  py runner.py simulator              # 只启动三家模拟数据商
  py runner.py collector              # 只启动融合采集 + 网页（需模拟器已在跑）
环境变量：SIM_PORT / WEB_PORT / TIME_SCALE / SCENARIO
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys
from pathlib import Path

from aiohttp import web

import config
from fusion.engine import FusionEngine
from fusion.connectors import build_connectors
from fusion.mapping import MappingCatalog, build_adapters
from simulator.core import SimulatorCore
from simulator.server import build_app as build_sim_app
from web.server import build_app as build_web_app


def load_scenario(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


async def run_simulator(scenario: dict) -> tuple[web.AppRunner, SimulatorCore]:
    core = SimulatorCore(scenario, config.TIME_SCALE)
    app = build_sim_app(core)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, config.SIM_HOST, config.SIM_PORT).start()
    core.start()
    logging.info("模拟器已启动: http://%s:%s (WS/长轮询)", config.SIM_HOST, config.SIM_PORT)
    return runner, core


async def run_collector(scenario: dict) -> tuple[web.AppRunner, FusionEngine, list]:
    catalog = MappingCatalog.from_scenario(scenario)
    engine = FusionEngine()
    engine.register_match(scenario)
    adapters = build_adapters(catalog)
    base_url = f"http://{config.SIM_HOST}:{config.SIM_PORT}"
    connectors = build_connectors(scenario, base_url, engine, adapters)
    for c in connectors:
        c.start()
    app = build_web_app(engine, catalog, scenario)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, config.WEB_HOST, config.WEB_PORT).start()
    logging.info("融合服务网页: http://%s:%s", config.WEB_HOST, config.WEB_PORT)
    return runner, engine, connectors


async def _serve(collector_only: bool, simulator_only: bool) -> None:
    scenario = load_scenario(str(Path(config.SCENARIO_PATH)))
    runners = []
    cores = []
    connectors = []
    if not collector_only:
        runner, core = await run_simulator(scenario)
        runners.append(runner)
        cores.append(core)
        await asyncio.sleep(0.3)  # 等模拟器端口就绪
    if not simulator_only:
        runner, _engine, conns = await run_collector(scenario)
        runners.append(runner)
        connectors.extend(conns)
    try:
        while True:
            await asyncio.sleep(3600)
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        for c in connectors:
            await c.stop()
        for core in cores:
            await core.stop()
        for runner in runners:
            await runner.cleanup()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)-16s %(message)s",
        datefmt="%H:%M:%S",
    )
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    if mode not in {"all", "simulator", "collector"}:
        print(__doc__)
        sys.exit(1)
    try:
        asyncio.run(_serve(mode == "collector", mode == "simulator"))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
