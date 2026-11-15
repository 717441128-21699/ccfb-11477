"""赛后报告：按剧本真值逐事件评估每家数据商又快又准。

延迟口径：同一事件，该家报道实际到达时间 - 所有家中最早到达时间（相对延迟，秒）。
准确率口径：
  correct   —— 报道过且未撤回，且与剧本某真值事件匹配；
  wrong     —— 误报（撤回后无任何有效支撑）或事件类型/球队/球员对不上；
  retracted —— 撤回次数（含真实事件的单家撤回）；
  missing   —— 剧本有、但该家自始至终没报的事件。
"""
from __future__ import annotations

from typing import Any

from .config import PROVIDERS, OFFICIAL_PROVIDER, SCORE_EVENTS
from .models import Incident, IncidentStatus


def _baseline_events(script: dict[str, Any]) -> dict[tuple, dict[str, Any]]:
    base: dict[tuple, dict[str, Any]] = {}
    for ev in script["events"]:
        key = (ev["type"], ev["minute"], ev["team"], ev.get("player", ""))
        base[key] = ev
    return base


def build_match_report(script: dict[str, Any], state_dict: dict[str, Any]) -> dict[str, Any]:
    providers = list(PROVIDERS)
    baseline = _baseline_events(script)
    incidents: list[dict[str, Any]] = state_dict["incidents"]

    per_source: dict[str, dict[str, Any]] = {
        p: {
            "provider": p, "transport": PROVIDERS[p].transport,
            "is_official": PROVIDERS[p].is_official,
            "latencies": [], "correct": 0, "wrong": 0,
            "retracted": 0, "missing": 0, "score_mismatches": [],
            "events_seen": 0,
        } for p in providers
    }

    # ---- 逐融合事件评估 -------------------------------------------------------
    event_rows: list[dict[str, Any]] = []
    for inc in incidents:
        arrivals = {p: r["received_at"] for p, r in inc["sources"].items()}
        earliest = min(arrivals.values()) if arrivals else 0.0
        row = {
            "incident_id": inc["incident_id"], "event_type": inc["event_type"],
            "minute": inc["minute"], "team": inc["team"], "status": inc["status"],
            "arrival_offsets": {p: round(t - earliest, 3) for p, t in arrivals.items()},
            "first_provider": min(arrivals, key=arrivals.get) if arrivals else None,
            "retracted_by": inc["retracted_sources"],
        }
        event_rows.append(row)
        for p, report in inc["sources"].items():
            stats = per_source[p]
            stats["events_seen"] += 1
            stats["latencies"].append(arrivals[p] - earliest)
            if p in inc["retracted_sources"]:
                stats["retracted"] += 1
                if inc["status"] == IncidentStatus.RETRACTED.value:
                    stats["wrong"] += 1           # 撤回后无人支撑 -> 误报
            else:
                key = (inc["event_type"], inc["minute"], inc["team"], inc["player"])
                loose = any(
                    k[0] == key[0] and k[2] == key[2] and abs(k[1] - key[1]) <= 2
                    and (not key[3] or k[3] == key[3])
                    for k in baseline
                )
                if key in baseline or loose:
                    stats["correct"] += 1
                else:
                    stats["wrong"] += 1
            # 比分核对：带了比分且与官方最终比分不一致的中间态
            if inc["event_type"] in SCORE_EVENTS and report.get("score_home") is not None:
                if (report["score_home"], report["score_away"]) != \
                        (state_dict["score_home"], state_dict["score_away"]):
                    stats["score_mismatches"].append({
                        "minute": inc["minute"],
                        "reported": [report["score_home"], report["score_away"]],
                        "final": [state_dict["score_home"], state_dict["score_away"]],
                    })

    # ---- 漏报：剧本有报道要求、但融合结果里该家没出现 ---------------------------
    for ev in script["events"]:
        expected = set(ev.get("reports", {}).keys())
        for p in expected:
            found = any(
                inc["event_type"] == ev["type"]
                and abs(inc["minute"] - ev["minute"]) <= 2
                and inc["team"] == ev["team"]
                and p in inc["sources"]
                for inc in incidents
            )
            if not found:
                per_source[p]["missing"] += 1

    # ---- 汇总指标 --------------------------------------------------------------
    summary = []
    for p, stats in per_source.items():
        lat = stats["latencies"]
        total_judged = stats["correct"] + stats["wrong"]
        summary.append({
            "provider": p,
            "transport": stats["transport"],
            "is_official": stats["is_official"],
            "events_seen": stats["events_seen"],
            "correct": stats["correct"],
            "wrong": stats["wrong"],
            "retracted": stats["retracted"],
            "missing": stats["missing"],
            "accuracy": round(stats["correct"] / total_judged, 4) if total_judged else 1.0,
            "avg_latency": round(sum(lat) / len(lat), 3) if lat else None,
            "max_latency": round(max(lat), 3) if lat else None,
            "first_place_count": sum(
                1 for r in event_rows if r["first_provider"] == p),
            "score_mismatches": stats["score_mismatches"],
        })
    ranking = sorted(
        summary,
        key=lambda s: (-s["accuracy"], s["avg_latency"] if s["avg_latency"] is not None else 9e9,
                       -s["first_place_count"]),
    )

    return {
        "match_id": state_dict["match_id"],
        "label": script.get("label", state_dict["match_id"]),
        "final_score": [state_dict["score_home"], state_dict["score_away"]],
        "match_status": state_dict["status"],
        "generated_events": len(event_rows),
        "script_event_count": len(script["events"]),
        "ranking": ranking,
        "per_source": summary,
        "event_timeline": event_rows,
        "corrections": state_dict["corrections"],
        "official_provider": OFFICIAL_PROVIDER,
    }
