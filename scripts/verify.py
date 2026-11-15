"""端到端验证：模拟器 + 采集器 + 融合引擎跑完整剧本，与剧本真值逐事件比对。

用法（项目根目录）：
    .venv\\Scripts\\python.exe -m scripts.verify
退出码 0 表示全部断言通过。
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from fusion.simulator import MatchSimulator, load_scripts  # noqa: E402
from fusion.collector import normalize                       # noqa: E402
from fusion.engine import FusionEngine                       # noqa: E402
from fusion.stats import build_match_report                  # noqa: E402
from fusion.config import SCORE_EVENTS, PROVIDERS            # noqa: E402
from fusion.models import IncidentStatus, SourceStatus       # noqa: E402

SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"


class OfflineCollector:
    """离线采集器：直接订阅模拟器 feed，模拟三种传输客户端的收包/掉线/补漏。

    队列在整个生命周期内常驻注册，避免"注销/重注册间隙丢事件"；
    实时通道与重连重放可能重复，统一按 seq 水位线去重。
    """

    def __init__(self, runtime_sim: MatchSimulator, provider: str, engine: FusionEngine,
                 drops: list[dict]):
        self.sim = runtime_sim
        self.provider = provider
        self.engine = engine
        self.last_seq = 0
        self.connected = False
        self.reconnects = 0
        self.gap_fills = 0
        self.stop = asyncio.Event()
        self.cache: dict[str, object] = {}
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=300)

    async def run(self) -> None:
        feed = self.sim.feeds[self.provider]
        feed._waiters.append(self.queue)          # 常驻订阅，杜绝注册间隙
        try:
            while not self.stop.is_set():
                if feed.is_down:                  # 掉线窗口：断开，等窗口结束
                    self.connected = False
                    await asyncio.sleep(0.2)
                    continue
                if not self.connected:            # 重连：按 seq 回放补漏
                    self.connected = True
                    if self.last_seq:
                        self.reconnects += 1
                        backlog = feed.replay(self.last_seq)
                        if backlog:
                            self.gap_fills += len(backlog)
                            for entry in backlog:
                                await self._consume(entry)
                try:
                    entry = await asyncio.wait_for(self.queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue
                if entry.get("_connection_lost"):
                    self.connected = False
                    continue
                await self._consume(entry)
        finally:
            with contextlib.suppress(ValueError):
                feed._waiters.remove(self.queue)

    async def _consume(self, entry: dict) -> None:
        if entry.get("seq", 0) <= self.last_seq:  # 重放与实时重复，按 seq 去重
            return
        report = normalize(self.provider, entry)
        if report is None:
            return
        if not report.retraction:
            self.cache[report.ref_id] = report
        else:
            original = self.cache.get(report.ref_id)
            if original:
                report.event_type = original.event_type
                report.minute = original.minute
                report.team = original.team
        self.last_seq = max(self.last_seq, report.seq)
        await self.engine.ingest(report)


def match_incident(incidents: list[dict], ev: dict) -> dict | None:
    for inc in incidents:
        if inc["status"] == IncidentStatus.RETRACTED.value:
            continue
        if inc["event_type"] != ev["type"] or inc["team"] != ev["team"]:
            continue
        if abs(inc["minute"] - ev["minute"]) > 2:
            continue
        if ev.get("player") and inc["player"] and ev["player"] != inc["player"]:
            continue
        return inc
    return None


async def verify_script(script: dict) -> tuple[bool, list[str], dict]:
    failures: list[str] = []
    sim = MatchSimulator(script)
    engine = FusionEngine(script["match_id"], script["home"], script["away"])
    await sim.start()

    collectors = [OfflineCollector(sim, p, engine, script.get("drops", []))
                  for p in PROVIDERS]
    tasks = [asyncio.create_task(c.run()) for c in collectors]

    # 等到终场事件被融合，再留 3 秒让撤回/补漏收尾
    deadline = asyncio.get_running_loop().time() + 150
    while asyncio.get_running_loop().time() < deadline:
        if engine.state.status == "finished":
            break
        await asyncio.sleep(0.2)
    await asyncio.sleep(3.0)

    for c in collectors:
        c.stop.set()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await sim.stop()

    state = engine.snapshot()
    incidents = state["incidents"]

    # 1) 每个剧本真实事件都必须在融合结果中存在且非撤回（false_report 没有真实报道，跳过）
    for ev in script["events"]:
        if "reports" not in ev:
            continue
        inc = match_incident(incidents, ev)
        if inc is None:
            failures.append(f"事件缺失/被误撤：{ev['id']} {ev['type']} {ev['minute']}'")
            continue
        expected_providers = set(ev.get("reports", {}).keys())
        active = set(inc["active_providers"])
        missing_votes = expected_providers - active - set(inc["retracted_sources"])
        if missing_votes:
            failures.append(
                f"{ev['id']} 缺少来源投票 {missing_votes}（可能补漏失败）")
        # 比分类且官方有报道 -> 必须 official
        if ev["type"] in SCORE_EVENTS and "alpha" in expected_providers:
            if inc["status"] != IncidentStatus.OFFICIAL.value:
                failures.append(
                    f"{ev['id']} 比分类未取得官方状态，实际 {inc['status']}")
        elif len(expected_providers - set(inc["retracted_sources"])) >= 2:
            if inc["status"] == IncidentStatus.PENDING.value:
                failures.append(f"{ev['id']} 两家以上有效仍为 pending")

    # 2) 误报（false_report）必须最终撤回，且不能留下对应有效事件
    for ev in script["events"]:
        fr = ev.get("false_report")
        if not fr:
            continue
        ghost = match_incident(incidents, ev)
        if ghost is not None:
            failures.append(
                f"误报 {ev['id']} 未被清理，融合结果仍存在 {ghost['incident_id']}")
        retracted = [i for i in incidents
                     if i["event_type"] == ev["type"] and i["minute"] == ev["minute"]]
        if not any(i["status"] == IncidentStatus.RETRACTED.value for i in retracted):
            failures.append(f"误报 {ev['id']} 缺少 retracted 留痕")

    # 3) 单家撤回（reports[*].retract_after）必须体现在 retracted_sources
    for ev in script["events"]:
        for p, spec in ev.get("reports", {}).items():
            if "retract_after" not in spec:
                continue
            inc = match_incident(incidents, ev)
            if inc is None or p not in inc["retracted_sources"]:
                failures.append(f"{ev['id']} 的 {p} 撤回未生效")

    # 4) 最终比分必须等于剧本最后一条官方比分为准的真值
    expected_score = None
    for ev in reversed(script["events"]):
        if ev["type"] in SCORE_EVENTS and "alpha" in ev.get("reports", {}):
            fr = ev.get("false_report")
            if fr:
                continue
            expected_score = tuple(ev["score"])
            break
    if expected_score and tuple([state["score_home"], state["score_away"]]) != expected_score:
        failures.append(
            f"最终比分错误：{state['score_home']}-{state['score_away']}，应为 {expected_score[0]}-{expected_score[1]}")
    if state["pending_goals"]:
        failures.append(f"完赛后仍有 {len(state['pending_goals'])} 个待确认进球未裁决")

    # 5) 掉线源必须发生重连且成功补漏（seq 连续无缺口）
    for drop in script.get("drops", []):
        c = next(x for x in collectors if x.provider == drop["provider"])
        if c.reconnects == 0:
            failures.append(f"{drop['provider']} 掉线后未观察到重连")
        if c.gap_fills == 0:
            failures.append(f"{drop['provider']} 重连后未补回任何漏掉的事件")
        feed = sim.feeds[drop["provider"]]
        if c.last_seq != len(feed.log):
            failures.append(
                f"{drop['provider']} seq 不连续：已处理 {c.last_seq} / 日志 {len(feed.log)}")

    # 6) 纠错记录非空，且每条撤回/误报都可追溯
    if not state["corrections"]:
        failures.append("纠错记录为空")
    corr_kinds = {c["kind"] for c in state["corrections"]}
    if "false_report" not in corr_kinds and any(
            "false_report" in e for e in script["events"]):
        failures.append("剧本含误报但纠错记录里没有 false_report")

    # 7) 报告可生成且各源统计自洽
    report = build_match_report(script, state)
    for row in report["per_source"]:
        if not (0 <= row["accuracy"] <= 1):
            failures.append(f"{row['provider']} 准确率越界：{row['accuracy']}")
        if row["wrong"] + row["correct"] + row["missing"] == 0 and row["events_seen"]:
            failures.append(f"{row['provider']} 统计不自洽")

    info = {
        "reconnects": {c.provider: c.reconnects for c in collectors},
        "gap_fills": {c.provider: c.gap_fills for c in collectors},
        "incident_count": len(incidents),
        "correction_count": len(state["corrections"]),
        "final_score": [state["score_home"], state["score_away"]],
        "ranking": [(r["provider"], r["accuracy"], r["avg_latency"], r["first_place_count"])
                    for r in report["ranking"]],
    }
    return not failures, failures, info


async def main() -> int:
    scripts = load_scripts(SCRIPTS_DIR)
    all_ok = True
    for script in scripts:
        print(f"\n=== 验证剧本 {script['match_id']}：{script.get('label', '')} ===")
        ok, failures, info = await verify_script(script)
        print(json.dumps(info, ensure_ascii=False, indent=2))
        if ok:
            print(f"[PASS] {script['match_id']} 全部断言通过")
        else:
            all_ok = False
            for f in failures:
                print(f"[FAIL] {f}")
    print("\n" + ("全部剧本验证通过 [PASS-ALL]" if all_ok else "存在失败断言 [HAS-FAIL]"))
    return 0 if all_ok else 1


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    raise SystemExit(asyncio.run(main()))
