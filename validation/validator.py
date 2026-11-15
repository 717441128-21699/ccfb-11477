"""融合结果 vs 剧本真值的逐事件比对器。"""
from __future__ import annotations

from fusion.engine import FusionEngine
from fusion.models import EventStatus, SCORE_EVENTS

# 剧本中被视为"真实发生"的事件类型（误报节点不在其中）
FALSE_NODE_PREFIX = "false_"


def expected_truth_events(scenario: dict) -> list[dict]:
    return [n for n in scenario["timeline"] if not n["type"].startswith(FALSE_NODE_PREFIX)]


def expected_false_events(scenario: dict) -> list[dict]:
    return [n for n in scenario["timeline"] if n["type"].startswith(FALSE_NODE_PREFIX)]


def validate(engine: FusionEngine, scenario: dict) -> dict:
    m = engine.get_match(scenario["match_id"])
    failures: list[str] = []
    checks: list[dict] = []

    truth = expected_truth_events(scenario)
    false_events = expected_false_events(scenario)
    by_truth: dict[str, list] = {}
    for ev in m.events:
        # 通过 source_event_id 反查 truth_id：EV-xxx-A/B/C
        for vote in ev.votes.values():
            tid = vote.source_event_id.rsplit("-", 1)[0]
            by_truth.setdefault(tid, []).append(ev)
            break

    # 1) 每个真实事件都必须融合成功且最终 CONFIRMED
    for node in truth:
        tid = node["truth_id"]
        fused_list = by_truth.get(tid)
        if not fused_list:
            failures.append(f"[缺失] 剧本事件 {tid}({node['type']}) 未出现在融合结果中")
            checks.append({"truth_id": tid, "ok": False, "reason": "missing"})
            continue
        if len(fused_list) > 1:
            failures.append(f"[分裂] 剧本事件 {tid} 被融合成 {len(fused_list)} 个事件（匹配失败）")
        ev = fused_list[0]
        ev.truth_id = tid
        reasons = []
        if ev.event_type != node["type"]:
            reasons.append(f"类型 {ev.event_type}!={node['type']}")
        if ev.minute != node.get("minute"):
            reasons.append(f"分钟 {ev.minute}!={node.get('minute')}")
        if node.get("team"):
            expected_team = node["team"]
            if expected_team == "home":
                expected_team = scenario["home_team"]["id"]
            elif expected_team == "away":
                expected_team = scenario["away_team"]["id"]
            if ev.team_id != expected_team:
                reasons.append(f"球队 {ev.team_id}!={expected_team}")
        if node.get("player") and ev.player_id != node["player"]:
            reasons.append(f"球员 {ev.player_id}!={node['player']}")
        if ev.status != EventStatus.CONFIRMED:
            reasons.append(f"状态 {ev.status.value} 非 CONFIRMED")
        expected_sources = set(node.get("sources", []))
        got_sources = set(ev.sources)
        if got_sources != expected_sources:
            reasons.append(f"来源 {sorted(got_sources)} != 期望 {sorted(expected_sources)}")
        ok = not reasons
        checks.append({"truth_id": tid, "ok": ok, "reason": "; ".join(reasons) or "ok",
                       "fused_id": ev.fused_id})
        if not ok:
            failures.append(f"[不一致] {tid}({node['type']}): {'; '.join(reasons)}")

    # 2) 每个误报事件必须最终 RETRACTED，且没有残留活跃投票
    for node in false_events:
        tid = node["truth_id"]
        fused_list = by_truth.get(tid, [])
        if not fused_list:
            failures.append(f"[误报缺失] {tid} 连临时事件都没有产生，无法验证撤回链路")
            checks.append({"truth_id": tid, "ok": False, "reason": "false event never appeared"})
            continue
        for ev in fused_list:
            ev.truth_id = tid
            if ev.status != EventStatus.RETRACTED or ev.active_votes:
                failures.append(
                    f"[撤回失败] {tid} 状态={ev.status.value}, 活跃来源={ev.sources}")
                checks.append({"truth_id": tid, "ok": False,
                               "reason": f"not retracted: {ev.status.value}"})
            else:
                checks.append({"truth_id": tid, "ok": True, "reason": "retracted correctly",
                               "fused_id": ev.fused_id})

    # 3) 最终权威比分必须等于剧本最后一个 score_after（官方源口径）
    last_score = None
    for node in truth:
        if node["type"] in SCORE_EVENTS and node.get("score_after"):
            last_score = node["score_after"]
    if last_score is not None and m.authoritative_score != last_score:
        failures.append(
            f"[比分] 权威比分 {m.authoritative_score} != 剧本期望 {last_score}")
    if m.phase != "FINISHED":
        failures.append(f"[阶段] 比赛未到 FINISHED，实际 {m.phase}")

    # 4) 掉线窗口的事件必须以 backfilled 方式补回（B: EV-G2，C: EV-G4）
    for tid, sid in (("EV-G2", "B"), ("EV-G4", "C")):
        fused_list = by_truth.get(tid, [])
        if not fused_list:
            continue
        ev = fused_list[0]
        vote = ev.votes.get(sid)
        if not vote or not vote.backfilled:
            failures.append(f"[补传] {tid} 来源 {sid} 未标记 backfilled（断线补传链路异常）")
        else:
            checks.append({"truth_id": tid, "ok": True, "reason": f"{sid} backfilled"})

    # 5) 传输层：B、C 至少各掉线一次且重连成功
    for sid in ("B", "C"):
        tr = m.transport[sid]
        if tr["disconnect_count"] < 1:
            failures.append(f"[掉线] 来源 {sid} 未观测到掉线（剧本配置未生效）")
        if tr["connect_count"] < 2:
            failures.append(f"[重连] 来源 {sid} 未观测到重连成功")

    ok = not failures
    return {
        "ok": ok,
        "match_id": scenario["match_id"],
        "total_checks": len(checks),
        "passed_checks": sum(1 for c in checks if c["ok"]),
        "failures": failures,
        "checks": checks,
        "final_authoritative_score": m.authoritative_score,
        "final_pending_score": m.pending_score,
        "phase": m.phase,
    }
