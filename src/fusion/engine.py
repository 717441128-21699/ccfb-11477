"""融合引擎：三家报道归一化后的合并、确认、撤回与比分裁决。

规则：
  1. 同一事件按指纹（类型/分钟/球队/球员）匹配，多家报道合并为一条 Incident；
  2. 先到先建，有效报道 >= MAJORITY_THRESHOLD 家 -> confirmed；
  3. 报道被撤回则该家投票剔除；有效投票归零 -> retracted（误报/撤回全程留痕）；
  4. 比分类事件以官方源（alpha）为准：官方确认才计入比分并标 official，
     别家先报只标 pending 放进“待确认进球”，不计比分；
  5. 每次状态翻转、撤回、比分修正都写 Correction。
"""
from __future__ import annotations

import itertools
import time
from typing import Any

from .config import (
    OFFICIAL_PROVIDER, MAJORITY_THRESHOLD, SCORE_EVENTS, PROVIDERS,
)
from .models import (
    SourceReport, Incident, Correction, MatchState,
    IncidentStatus, EventType,
)


def fingerprint(report: SourceReport) -> tuple:
    if report.event_type in ("kickoff", "halftime", "fulltime"):
        return (report.event_type,)
    if report.event_type == "substitution":
        return (report.event_type, report.minute, report.team)
    if report.event_type in ("var",):
        return (report.event_type, report.minute, report.team)
    return (report.event_type, report.minute, report.team,
            report.player, report.player_in, report.player_out)


class FusionEngine:
    def __init__(self, match_id: str, home: str, away: str):
        self.state = MatchState(match_id=match_id, home=home, away=away)
        self._seq = itertools.count(1)
        # fingerprint -> incident_id，快速定位
        self._fp_index: dict[tuple, str] = {}
        # (provider, ref_id) -> incident_id，撤回定位
        self._ref_index: dict[tuple[str, str], str] = {}

    # ---- 主入口 ---------------------------------------------------------------

    async def ingest(self, report: SourceReport) -> None:
        if report.retraction:
            self._handle_retraction(report)
        else:
            self._handle_report(report)
        self._recompute()
        self.state.to_dict()  # 保持 dataclass 视图即时可用

    # ---- 报道合并 -------------------------------------------------------------

    def _handle_report(self, report: SourceReport) -> None:
        fp = fingerprint(report)
        incident = self._find_incident(report, fp)
        if incident is None:
            incident = Incident(
                incident_id=f"inc-{next(self._seq):04d}",
                event_type=report.event_type,
                minute=report.minute,
                team=report.team,
                player=report.player,
                player_in=report.player_in,
                player_out=report.player_out,
            )
            self.state.incidents.append(incident)
            self._fp_index[fp] = incident.incident_id
            if self.state.started_at is None and report.event_type == EventType.KICKOFF.value:
                self.state.started_at = report.received_at
        else:
            # 用后到报道补全可能缺失的字段（换人球员等）
            for attr in ("player", "player_in", "player_out", "team"):
                if not getattr(incident, attr) and getattr(report, attr):
                    setattr(incident, attr, getattr(report, attr))

        is_new_vote = report.provider not in incident.sources
        incident.sources[report.provider] = report
        self._ref_index[(report.provider, report.ref_id)] = incident.incident_id
        if incident.first_arrival is None:
            incident.first_arrival = report.received_at
        incident.last_update = time.time()
        if is_new_vote:
            incident.history.append({
                "at": report.received_at, "action": "report_added",
                "provider": report.provider, "seq": report.seq,
            })

    def _find_incident(self, report: SourceReport, fp: tuple) -> Incident | None:
        # 1) 同指纹精确命中（未撤回的事件优先复用）
        inc_id = self._fp_index.get(fp)
        if inc_id:
            for inc in self.state.incidents:
                if inc.incident_id == inc_id and inc.status != IncidentStatus.RETRACTED.value:
                    return inc
        # 2) 分钟 ±2 的宽松匹配，容忍各家计时偏差（仅同类型同球队）
        for inc in self.state.incidents:
            if inc.status == IncidentStatus.RETRACTED.value:
                continue
            if inc.event_type != report.event_type or inc.team != report.team:
                continue
            if abs(inc.minute - report.minute) > 2:
                continue
            if report.event_type in SCORE_EVENTS | {"red_card", "yellow_card"}:
                if report.player and inc.player and report.player != inc.player:
                    continue
            return inc
        return None

    # ---- 撤回 -----------------------------------------------------------------

    def _handle_retraction(self, report: SourceReport) -> None:
        inc_id = self._ref_index.get((report.provider, report.ref_id))
        incident = next(
            (i for i in self.state.incidents if i.incident_id == inc_id), None)
        if incident is None:
            self._add_correction(
                "retraction", report.provider,
                f"撤回指令 {report.ref_id} 找不到对应报道，已忽略",
                event_type=report.event_type, minute=report.minute)
            return
        if report.provider not in incident.retracted_sources:
            incident.retracted_sources.append(report.provider)
            incident.history.append({
                "at": report.received_at, "action": "retracted",
                "provider": report.provider, "ref": report.ref_id,
            })
        still_active = [p for p in incident.sources if p not in incident.retracted_sources]
        kind = "false_report" if not still_active else "retraction"
        detail = (f"{report.provider} 撤回 {report.ref_id}（"
                  f"{report.event_type} {report.minute}'），"
                  f"剩余有效来源 {len(still_active)} 家")
        self._add_correction(
            kind, report.provider, detail,
            event_type=incident.event_type, minute=incident.minute,
            incident_id=incident.incident_id)

    # ---- 状态与比分重算 --------------------------------------------------------

    def _recompute(self) -> None:
        for inc in self.state.incidents:
            active = inc.active_providers()
            old_status = inc.status
            if not active:
                new_status = IncidentStatus.RETRACTED.value
            elif inc.event_type in SCORE_EVENTS and OFFICIAL_PROVIDER not in active:
                # 比分类：非官方先报，哪怕多家确认也只标待确认
                new_status = (IncidentStatus.CONFIRMED.value
                              if len(active) >= MAJORITY_THRESHOLD
                              else IncidentStatus.PENDING.value)
            elif OFFICIAL_PROVIDER in active and inc.event_type in SCORE_EVENTS:
                new_status = IncidentStatus.OFFICIAL.value
            elif len(active) >= MAJORITY_THRESHOLD:
                new_status = IncidentStatus.CONFIRMED.value
            else:
                new_status = IncidentStatus.PENDING.value
            inc.status = new_status
            if new_status != old_status and old_status is not None:
                inc.history.append({
                    "at": time.time(), "action": "status_change",
                    "from": old_status, "to": new_status,
                })
                self._add_correction(
                    "status_change", "fusion",
                    f"事件 {inc.event_type} {inc.minute}' 状态 {old_status} -> {new_status}",
                    event_type=inc.event_type, minute=inc.minute,
                    incident_id=inc.incident_id)
        self._recompute_score()
        self._recompute_match_status()

    def _recompute_score(self) -> None:
        home = away = 0
        pending: list[dict[str, Any]] = []
        revisions: list[str] = []
        for inc in sorted(self.state.incidents, key=lambda i: (i.minute, i.first_arrival or 0)):
            if inc.status == IncidentStatus.RETRACTED.value or \
                    inc.event_type not in SCORE_EVENTS:
                continue
            official = inc.sources.get(OFFICIAL_PROVIDER)
            if official and OFFICIAL_PROVIDER not in inc.retracted_sources:
                if official.score_home is not None and official.score_away is not None:
                    home, away = official.score_home, official.score_away
                else:  # 官方没带比分就按事件方向推
                    if inc.event_type == "own_goal":
                        away += 1 if inc.team == self.state.home else 0
                        home += 1 if inc.team == self.state.away else 0
                    else:
                        home += 1 if inc.team == self.state.home else 0
                        away += 1 if inc.team == self.state.away else 0
                inc.score_home, inc.score_away = home, away
            else:
                pending.append({
                    "incident_id": inc.incident_id, "event_type": inc.event_type,
                    "minute": inc.minute, "team": inc.team, "player": inc.player,
                    "providers": inc.active_providers(),
                })
        old = (self.state.score_home, self.state.score_away)
        if old != (home, away) and old != (0, 0):
            revisions.append(f"比分 {old[0]}-{old[1]} -> {home}-{away}（官方源裁决）")
        self.state.score_home, self.state.score_away = home, away
        self.state.pending_goals = pending
        for rev in revisions:
            self._add_correction("score_revision", "fusion", rev)

    def _recompute_match_status(self) -> None:
        types = {i.event_type for i in self.state.incidents
                 if i.status != IncidentStatus.RETRACTED.value}
        if EventType.FULLTIME.value in types:
            self.state.status = "finished"
            self.state.finished_at = self.state.finished_at or time.time()
        elif EventType.HALFTIME.value in types:
            self.state.status = "halftime"
        elif EventType.KICKOFF.value in types:
            self.state.status = "live"

    def _add_correction(self, kind: str, provider: str, detail: str,
                        event_type: str = "", minute: int | None = None,
                        incident_id: str = "") -> None:
        self.state.corrections.append(Correction(
            correction_id=f"cor-{next(self._seq):04d}",
            match_id=self.state.match_id,
            incident_id=incident_id,
            kind=kind, provider=provider, detail=detail,
            event_type=event_type, minute=minute,
        ))

    # ---- 快照（给网页/报告） ----------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        return self.state.to_dict()
