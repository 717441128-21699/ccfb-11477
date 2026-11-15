"""剧本调度器：把 timeline 展开成每家来源的发送计划（含延迟、抖动、乱序、误报撤回、掉线）。"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Optional

from .encoders import EnvelopeBuilder


@dataclass
class ScheduledItem:
    source_id: str
    send_at_ms: int                     # 墙钟毫秒
    match_sec: int
    truth_id: str
    event_type: str
    source_event_id: str
    retraction: bool = False
    minute: Optional[int] = None
    team_id: Optional[str] = None
    player_id: Optional[str] = None
    player_on_id: Optional[str] = None
    player_off_id: Optional[str] = None
    score_after: Optional[list] = None
    seq: Optional[int] = None           # 由 SimulatorCore 按发送顺序赋值


class Scheduler:
    def __init__(self, scenario: dict, time_scale: float, start_monotonic_ms: int,
                 seed: Optional[int] = None):
        self.scenario = scenario
        self.time_scale = time_scale
        self.t0 = start_monotonic_ms
        self.rng = random.Random(seed if seed is not None else scenario.get("seed", 20261115))
        self.builder = EnvelopeBuilder(scenario)
        self.items: list[ScheduledItem] = []
        self._expand()

    def _jitter(self, source_id: str) -> int:
        spec = self.scenario["sources"][source_id]
        return self.rng.randint(0, int(spec["jitter_ms"]))

    def _add(self, source_id: str, node: dict, match_sec: int, truth_id: str,
             event_type: str, source_event_id: str, retraction: bool,
             extra_delay_ms: int = 0, jitter_override_ms: Optional[int] = None) -> None:
        spec = self.scenario["sources"][source_id]
        jitter = jitter_override_ms if jitter_override_ms is not None else self._jitter(source_id)
        wall_offset_ms = int(match_sec / self.time_scale * 1000)
        send_at = self.t0 + wall_offset_ms + int(spec["delay_ms"]) + jitter + extra_delay_ms
        self.items.append(ScheduledItem(
            source_id=source_id,
            send_at_ms=send_at,
            match_sec=match_sec,
            truth_id=truth_id,
            event_type=event_type,
            source_event_id=source_event_id,
            retraction=retraction,
            minute=node.get("minute"),
            team_id=self._resolve_team(node.get("team")),
            player_id=node.get("player"),
            player_on_id=node.get("player_on"),
            player_off_id=node.get("player_off"),
            score_after=node.get("score_after"),
        ))

    def _resolve_team(self, side: Optional[str]) -> Optional[str]:
        if side is None:
            return None
        if side == "home":
            return self.scenario["home_team"]["id"]
        if side == "away":
            return self.scenario["away_team"]["id"]
        return side  # 已经是球队 id

    def _expand(self) -> None:
        for node in self.scenario["timeline"]:
            overrides = node.get("jitter_overrides", {})
            # 正常多源事件
            for sid in node.get("sources", []):
                jitter_override = overrides.get(sid)
                self._add(sid, node, node["match_sec"], node["truth_id"],
                          node["type"], f"{node['truth_id']}-{sid}", False,
                          jitter_override_ms=jitter_override)
            # 误报：仅指定来源上报，随后撤回
            reported_by = node.get("reported_by")
            if reported_by:
                base_type = node.get("false_event_type") or node["type"].replace(
                    "false_", "").replace("_official", "")
                for sid in reported_by:
                    eid = f"{node['truth_id']}-{sid}"
                    self._add(sid, node, node["match_sec"], node["truth_id"],
                              base_type, eid, False)
                    self._add(sid, node, node["match_sec"], node["truth_id"],
                              base_type, eid, True,
                              extra_delay_ms=int(node["retract_after_ms"]))
        # 序号只标识"事件顺序"，在 SimulatorCore 真正发送时按时间顺序赋值
        self.items.sort(key=lambda x: x.send_at_ms)

    def drop_windows(self) -> dict[str, list[tuple[int, int]]]:
        """返回每家来源的掉线窗口（墙钟毫秒，半开区间 [start, end)）。"""
        out: dict[str, list[tuple[int, int]]] = {}
        for sid, spec in self.scenario["sources"].items():
            wins = []
            for win in spec.get("drop", []):
                start = self.t0 + int(win["match_sec"] / self.time_scale * 1000)
                wins.append((start, start + int(win["duration_ms"])))
            out[sid] = wins
        return out
