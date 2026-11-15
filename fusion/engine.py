"""融合引擎。

职责：
1. 接收三家适配器产出的标准 Report（含撤回）；
2. 同一事件多源匹配（类型/球队/分钟窗口/球员），先到者建事件、后来者投票；
3. 状态机：PENDING -> CONFIRMED（多数阈值 或 官方源投票）；撤回票 -> 全撤则 RETRACTED；
4. 比分：官方源活跃投票为准（authoritative_score），非官方先报的计入 pending_score 并标记待确认；
5. 记录全部纠错流水与每家到达延迟，赛后出报告。
"""
from __future__ import annotations

import asyncio
import time
import uuid
from typing import Optional

import config
from .models import (
    Correction, EventStatus, FusedEvent, MATCH_PHASE, MatchState, Report,
    SCORE_EVENTS, Vote,
)


def now_ms() -> int:
    return int(time.time() * 1000)


class FusionEngine:
    def __init__(self, official_source: str = config.OFFICIAL_SOURCE,
                 majority: int = config.MAJORITY_THRESHOLD,
                 minute_window: int = config.MINUTE_WINDOW):
        self.official_source = official_source
        self.majority = majority
        self.minute_window = minute_window
        self.matches: dict[str, MatchState] = {}
        self._subscribers: set[asyncio.Queue] = set()
        self._lock = asyncio.Lock()

    # ------------------------- 比赛注册 -------------------------

    def register_match(self, scenario: dict) -> MatchState:
        m = MatchState(
            match_id=scenario["match_id"],
            competition=scenario.get("competition", ""),
            home_team_id=scenario["home_team"]["id"],
            away_team_id=scenario["away_team"]["id"],
            home_name=scenario["home_team"]["canonical"],
            away_name=scenario["away_team"]["canonical"],
        )
        for sid in scenario["sources"]:
            m.transport[sid] = {
                "connected": False,
                "connect_count": 0,
                "disconnect_count": 0,
                "backfilled_events": 0,
                "last_seq": None,
                "last_event_ts_ms": None,
                "errors": 0,
                "mapping_errors": 0,
            }
        self.matches[m.match_id] = m
        return m

    def get_match(self, match_id: str) -> Optional[MatchState]:
        return self.matches.get(match_id)

    # ------------------------- 订阅推送（网页用） -------------------------

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subscribers.discard(q)

    async def _publish(self, kind: str, match_id: str, data: dict) -> None:
        msg = {"kind": kind, "match_id": match_id, "data": data, "ts_ms": now_ms()}
        for q in list(self._subscribers):
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                pass

    # ------------------------- 传输层状态回调（连接器调用） -------------------------

    async def mark_transport(self, match_id: str, source_id: str, **fields) -> None:
        m = self.matches[match_id]
        async with self._lock:
            m.transport[source_id].update(fields)
        await self._publish("transport", match_id, {"source": source_id, **fields})

    # ------------------------- 主入口：处理一条标准上报 -------------------------

    async def handle_report(self, report: Report) -> None:
        m = self.matches.get(report.match_id)
        if m is None:
            return
        async with self._lock:
            # 传输层记账
            tr = m.transport[report.source_id]
            tr["last_event_ts_ms"] = report.received_ts_ms
            if report.seq is not None:
                tr["last_seq"] = report.seq
            if report.backfilled:
                tr["backfilled_events"] += 1

            if report.retraction:
                event = self._apply_retraction(m, report)
                kind = "retraction"
                data = event.to_dict() if event else {"source_event_id": report.source_event_id}
            else:
                event, created = self._apply_vote(m, report)
                kind = "event_upsert"
                data = event.to_dict()
            self._recompute_scores(m)
        await self._publish(kind, m.match_id, data)

    def _apply_vote(self, m: MatchState, r: Report) -> tuple[FusedEvent, bool]:
        existing_fused_id = m.source_index.get(r.key)
        if existing_fused_id is not None:
            # 同一家同一 source_event_id 重复送达（补传重叠窗口），幂等忽略
            return m.find_event(existing_fused_id), False

        target = self._match_existing(m, r)
        created = False
        if target is None:
            # 模拟器来源事件 id 约定为 "<truth_id>-<source_id>"，据此关联剧本真值
            truth_id = r.source_event_id.rsplit("-", 1)[0] if "-" in r.source_event_id else None
            target = FusedEvent(
                fused_id=f"F-{uuid.uuid4().hex[:10]}",
                match_id=m.match_id,
                event_type=r.event_type,
                minute=r.minute,
                team_id=r.team_id,
                player_id=r.player_id,
                player_on_id=r.player_on_id,
                player_off_id=r.player_off_id,
                first_seen_ts_ms=r.received_ts_ms,
                truth_id=truth_id,
            )
            m.events.append(target)
            created = True

        target.votes[r.source_id] = Vote(
            source_id=r.source_id,
            source_event_id=r.source_event_id,
            minute=r.minute,
            team_id=r.team_id,
            player_id=r.player_id,
            score_home=r.score_home,
            score_away=r.score_away,
            arrived_ts_ms=r.received_ts_ms,
            source_ts_ms=r.source_ts_ms,
            seq=r.seq,
            backfilled=r.backfilled,
        )
        m.source_index[r.key] = target.fused_id

        # 换人事件补齐 on/off 字段
        if r.player_on_id and not target.player_on_id:
            target.player_on_id = r.player_on_id
        if r.player_off_id and not target.player_off_id:
            target.player_off_id = r.player_off_id

        before_status = target.status
        self._derive_status(m, target)
        if target.status != before_status:
            self._log_correction(
                m, r.source_id, r.source_event_id, target.fused_id,
                "STATUS_CHANGE", r.event_type,
                f"状态 {before_status.value} -> {target.status.value}",
                before={"status": before_status.value},
                after={"status": target.status.value},
            )
        return target, created

    def _apply_retraction(self, m: MatchState, r: Report) -> Optional[FusedEvent]:
        fused_id = m.source_index.get(r.key)
        if fused_id is None:
            # 撤回一个从未收到过的事件（极端乱序）：留痕但不建事件
            self._log_correction(
                m, r.source_id, r.source_event_id, None,
                "RETRACT", r.event_type, "撤回报文先于原事件到达，已记录",
            )
            return None
        target = m.find_event(fused_id)
        vote = target.votes.get(r.source_id)
        if vote is None or vote.retracted:
            return target
        before_status = target.status
        before_score = (list(m.authoritative_score), list(m.pending_score))
        vote.retracted = True
        vote.retract_ts_ms = r.received_ts_ms
        self._log_correction(
            m, r.source_id, r.source_event_id, target.fused_id,
            "RETRACT", target.event_type,
            f"{r.source_id} 撤回其上报的 {target.event_type}（分钟 {target.minute}）",
        )
        if not target.active_votes:
            target.status = EventStatus.RETRACTED
        else:
            self._derive_status(m, target)
        if target.status != before_status:
            self._log_correction(
                m, r.source_id, r.source_event_id, target.fused_id,
                "STATUS_CHANGE", target.event_type,
                f"撤回后状态 {before_status.value} -> {target.status.value}",
                before={"status": before_status.value},
                after={"status": target.status.value},
            )
        if target.event_type in SCORE_EVENTS and (
                before_status != target.status or r.source_id == self.official_source):
            self._log_correction(
                m, r.source_id, r.source_event_id, target.fused_id,
                "SCORE_ROLLBACK", target.event_type,
                f"进球事件撤回触发比分重算（撤回源={r.source_id}）",
                before={"authoritative_score": before_score[0], "pending_score": before_score[1]},
            )
        return target

    # ------------------------- 事件匹配 -------------------------

    def _match_existing(self, m: MatchState, r: Report) -> Optional[FusedEvent]:
        for ev in m.events:
            if ev.status == EventStatus.RETRACTED:
                continue
            if ev.event_type != r.event_type:
                continue
            if r.source_id in ev.votes:
                continue
            if ev.team_id is not None and r.team_id is not None and ev.team_id != r.team_id:
                continue
            if ev.player_id is not None and r.player_id is not None and ev.player_id != r.player_id:
                continue
            if ev.minute is not None and r.minute is not None \
                    and abs(ev.minute - r.minute) > self.minute_window:
                continue
            return ev
        return None

    def _derive_status(self, m: MatchState, ev: FusedEvent) -> None:
        active = ev.active_votes
        if not active:
            ev.status = EventStatus.RETRACTED
            return
        has_official = any(v.source_id == self.official_source for v in active)
        if len(active) >= self.majority or has_official:
            ev.status = EventStatus.CONFIRMED
        else:
            ev.status = EventStatus.PENDING

    # ------------------------- 比分计算 -------------------------

    def _recompute_scores(self, m: MatchState) -> None:
        authoritative = [0, 0]
        pending = [0, 0]
        score_events = [e for e in m.events if e.event_type in SCORE_EVENTS
                        and e.status != EventStatus.RETRACTED]
        score_events.sort(key=lambda e: (e.minute if e.minute is not None else 0,
                                         e.first_seen_ts_ms))
        for ev in score_events:
            side = 0 if ev.team_id == m.home_team_id else 1
            # 乌龙球：记给对方
            if ev.event_type == "own_goal":
                side = 1 - side
            official_vote = ev.votes.get(self.official_source)
            if official_vote and not official_vote.retracted:
                authoritative[side] += 1
                pending[side] += 1
            else:
                # 官方未确认（未报或已撤回）：只进待确认比分
                pending[side] += 1
        m.authoritative_score = authoritative
        m.pending_score = pending

    # ------------------------- 比赛阶段 -------------------------

    async def apply_control(self, report: Report) -> None:
        m = self.matches.get(report.match_id)
        if m is None or report.retraction:
            return
        async with self._lock:
            phase = MATCH_PHASE.get(report.event_type)
            if phase and m.phase != phase:
                old = m.phase
                m.phase = phase
                if phase == "LIVE" and m.started_ts_ms is None:
                    m.started_ts_ms = report.received_ts_ms
                if phase == "FINISHED":
                    m.finished_ts_ms = report.received_ts_ms
                self._log_correction(
                    m, report.source_id, report.source_event_id, None,
                    "STATUS_CHANGE", report.event_type,
                    f"比赛阶段 {old} -> {phase}",
                    before={"phase": old}, after={"phase": phase},
                )
        await self._publish("phase", m.match_id, {"phase": m.phase})

    def _log_correction(self, m: MatchState, source_id: str, source_event_id: str,
                        fused_id: Optional[str], kind: str, event_type: Optional[str],
                        detail: str, before: Optional[dict] = None,
                        after: Optional[dict] = None) -> None:
        m.corrections.append(Correction(
            correction_id=f"C-{uuid.uuid4().hex[:10]}",
            match_id=m.match_id,
            ts_ms=now_ms(),
            source_id=source_id,
            source_event_id=source_event_id,
            fused_id=fused_id,
            kind=kind,
            event_type=event_type,
            detail=detail,
            before=before,
            after=after,
        ))

    # ------------------------- 赛后报告 -------------------------

    def build_report(self, match_id: str) -> dict:
        m = self.matches[match_id]
        source_ids = sorted(m.transport)
        # 每家延迟统计
        per_source: dict[str, dict] = {s: {
            "reported": 0, "retracted": 0, "backfilled": 0,
            "lag_ms": [], "relative_ms": [], "first_count": 0,
            "mapping_errors": 0,
        } for s in source_ids}

        event_rows = []
        for ev in m.events:
            active = ev.active_votes
            first_src = min(ev.votes.values(), key=lambda v: v.arrived_ts_ms).source_id
            for v in ev.votes.values():
                row = per_source[v.source_id]
                if v.retracted:
                    row["retracted"] += 1
                else:
                    row["reported"] += 1
                    if v.source_id == first_src and not v.retracted:
                        row["first_count"] += 1
                if v.backfilled:
                    row["backfilled"] += 1
                if v.source_ts_ms:
                    row["lag_ms"].append(v.arrived_ts_ms - v.source_ts_ms)
                row["relative_ms"].append(v.arrived_ts_ms - ev.first_arrival_ts_ms)
            event_rows.append({
                "fused_id": ev.fused_id,
                "truth_id": ev.truth_id,
                "event_type": ev.event_type,
                "minute": ev.minute,
                "status": ev.status.value,
                "sources": ev.sources,
                "retracted_sources": ev.retracted_sources,
                "first_source": first_src,
                "arrival_order": [
                    {"source": v.source_id, "relative_ms": v.arrived_ts_ms - ev.first_arrival_ts_ms,
                     "backfilled": v.backfilled, "retracted": v.retracted}
                    for v in sorted(ev.votes.values(), key=lambda x: x.arrived_ts_ms)
                ],
            })

        def stats(values: list[int]) -> dict:
            if not values:
                return {"avg": None, "min": None, "max": None, "p95": None}
            s = sorted(values)
            idx = min(len(s) - 1, int(round(0.95 * (len(s) - 1))))
            return {"avg": round(sum(s) / len(s), 1), "min": s[0], "max": s[-1], "p95": s[idx]}

        for s, row in per_source.items():
            row["lag"] = stats(row["lag_ms"])
            row["relative"] = stats(row["relative_ms"])
            row.update(m.transport[s])
            del row["lag_ms"]
            del row["relative_ms"]

        # 又快又准：综合分 = 平均相对延迟（ms，越小越快）+ 误报惩罚（每条撤回按 2000ms 计），
        # 并列时准确率高者靠前。原始指标全部保留，页面可自行排序。
        PENALTY_PER_RETRACTION = 2000
        rankings = []
        for s, row in per_source.items():
            total = row["reported"] + row["retracted"]
            accuracy = (row["reported"] / total) if total else 1.0
            avg_rel = row["relative"]["avg"]
            speed = avg_rel if avg_rel is not None else 0
            composite = round(speed + row["retracted"] * PENALTY_PER_RETRACTION, 1)
            rankings.append({
                "source_id": s,
                "accuracy": round(accuracy, 4),
                "retracted": row["retracted"],
                "avg_relative_ms": avg_rel,
                "composite_score_ms": composite,
                "first_count": row["first_count"],
                "disconnect_count": row["disconnect_count"],
                "backfilled": row["backfilled"],
            })
        rankings.sort(key=lambda x: (x["composite_score_ms"], -x["accuracy"]))
        for i, x in enumerate(rankings):
            x["rank"] = i + 1

        return {
            "match_id": m.match_id,
            "competition": m.competition,
            "home_name": m.home_name,
            "away_name": m.away_name,
            "phase": m.phase,
            "final_authoritative_score": m.authoritative_score,
            "final_pending_score": m.pending_score,
            "duration_wall_ms": (m.finished_ts_ms - m.started_ts_ms)
            if m.started_ts_ms and m.finished_ts_ms else None,
            "per_source": per_source,
            "rankings": rankings,
            "events": event_rows,
            "corrections": [c.to_dict() for c in m.corrections],
        }
