"""无头端到端回放：进程内启动模拟器与采集连接器，加速跑完整个剧本并校验。"""
from __future__ import annotations

import asyncio
import contextlib
import json
import socket
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from aiohttp import web

import config
from fusion.connectors import build_connectors
from fusion.engine import FusionEngine
from fusion.mapping import MappingCatalog, build_adapters
from simulator.core import SimulatorCore
from simulator.server import build_app as build_sim_app
from validation.validator import validate


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def run_replay(scenario_path: str, time_scale: float = 120.0,
                     timeout_s: float = 180.0) -> dict:
    scenario = json.loads(Path(scenario_path).read_text(encoding="utf-8"))
    port = free_port()

    core = SimulatorCore(scenario, time_scale)
    runner = web.AppRunner(build_sim_app(core))
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", port).start()
    core.start()

    catalog = MappingCatalog.from_scenario(scenario)
    engine = FusionEngine()
    engine.register_match(scenario)
    connectors = build_connectors(
        scenario, f"http://127.0.0.1:{port}", engine, build_adapters(catalog))
    for c in connectors:
        c.start()

    m = engine.get_match(scenario["match_id"])
    try:
        # 等到终场（全部来源都收齐 fulltime），再留 2s 收尾撤回消息
        deadline = asyncio.get_running_loop().time() + timeout_s
        while asyncio.get_running_loop().time() < deadline:
            if m.phase == "FINISHED" and core.states and all(
                    st.finished for st in core.states.values()):
                await asyncio.sleep(2.0)
                break
            await asyncio.sleep(0.1)
        result = validate(engine, scenario)
        report = engine.build_report(scenario["match_id"])
        result["report"] = report
        return result
    finally:
        for c in connectors:
            await c.stop()
        await core.stop()
        await runner.cleanup()


def main() -> int:
    import logging
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s %(message)s")
    scale = float(config.__dict__.get("REPLAY_SCALE", 120.0))
    result = asyncio.run(run_replay(config.SCENARIO_PATH, time_scale=scale))
    print(json.dumps({k: v for k, v in result.items() if k != "report"},
                     ensure_ascii=False, indent=2))
    out = Path("validation/last_report.json")
    out.write_text(json.dumps(result["report"], ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n赛后报告已写入 {out}")
    print(f"校验 {result['passed_checks']}/{result['total_checks']} 通过 -> "
          f"{'✅ 全部一致' if result['ok'] else '❌ 存在失败'}")
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
