"""融合层数据模型：统一事件编码、投票、融合事件、比赛状态。"""
from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional


class EventStatus(str, enum.Enum):
    PENDING = "PENDING"          # 仅非官方单源上报，待确认
    CONFIRMED = "CONFIRMED"      # 达到多数阈值，或官方源已报
    RETRACTED = "RETRACTED"      # 全部来源撤回（误报撤销）
    CONFLICT = "CONFLICT"        # 官方源与其他来源不一致（保留人工核对标记）


# 统一事件类型编码
EVENT_TYPES = {
    "goal", "penalty_goal", "penalty_miss", "own_goal",
    "yellow_card", "red_card", "substitution",
    "kickoff", "halftime", "fulltime",
}
SCORE_EVENTS = {"goal", "penalty_goal", "own_goal"}
CONTROL_EVENTS = {"kickoff", "halftime", "fulltime"}

# 阶段控制类型 -> 比赛状态
MATCH_PHASE = {
    "kickoff": "LIVE",
    "halftime": "HALFTIME",
    "fulltime": "FINISHED",
}


@dataclass
class Report:
    """一条来源报文经适配器解析 + 统一映射后的标准上报。"""
    source_id: str
    source_event_id: str
    event_type: str
    match_id: str
    minute: Optional[int]
    team_id: Optional[str]
    player_id: Optional[str]
    player_on_id: Optional[str] = None
    player_off_id: Optional[str] = None
    score_home: Optional[int] = None
    score_away: Optional[int] = None
    retraction: bool = False
    source_ts_ms: Optional[int] = None          # 数据商报文自带时间戳（墙钟毫秒）
    received_ts_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    seq: Optional[int] = None                    # 该来源连接上的单调序号
    backfilled: bool = False                     # 是否为重连补传收回
    raw: Optional[dict] = None

    @property
    def key(self) -> tuple:
        return (self.source_id, self.source_event_id)


@dataclass
class Vote:
    """某一家来源对某个融合事件的一次投票（上报）。"""
    source_id: str
    source_event_id: str
    minute: Optional[int]
    team_id: Optional[str]
    player_id: Optional[str]
    score_home: Optional[int]
    score_away: Optional[int]
    arrived_ts_ms: int
    source_ts_ms: Optional[int]
    seq: Optional[int]
    backfilled: bool
    retracted: bool = False
    retract_ts_ms: Optional[int] = None


@dataclass
class FusedEvent:
    """融合后的统一事件。"""
    fused_id: str
    match_id: str
    event_type: str
    minute: Optional[int]
    team_id: Optional[str]
    player_id: Optional[str]
    player_on_id: Optional[str]
    player_off_id: Optional[str]
    first_seen_ts_ms: int
    votes: dict[str, Vote] = field(default_factory=dict)
    status: EventStatus = EventStatus.PENDING
    truth_id: Optional[str] = None               # 校验阶段回填，对应剧本事件

    @property
    def active_votes(self) -> list[Vote]:
        return [v for v in self.votes.values() if not v.retracted]

    @property
    def sources(self) -> list[str]:
        return sorted(v.source_id for v in self.active_votes)

    @property
    def retracted_sources(self) -> list[str]:
        return sorted(v.source_id for v in self.votes.values() if v.retracted)

    @property
    def first_arrival_ts_ms(self) -> int:
        return min(v.arrived_ts_ms for v in self.votes.values())

    def to_dict(self) -> dict:
        return {
            "fused_id": self.fused_id,
            "match_id": self.match_id,
            "event_type": self.event_type,
            "minute": self.minute,
            "team_id": self.team_id,
            "player_id": self.player_id,
            "player_on_id": self.player_on_id,
            "player_off_id": self.player_off_id,
            "status": self.status.value,
            "truth_id": self.truth_id,
            "first_seen_ts_ms": self.first_seen_ts_ms,
            "sources": self.sources,
            "retracted_sources": self.retracted_sources,
            "votes": [
                {
                    "source_id": v.source_id,
                    "source_event_id": v.source_event_id,
                    "minute": v.minute,
                    "team_id": v.team_id,
                    "player_id": v.player_id,
                    "score_home": v.score_home,
                    "score_away": v.score_away,
                    "arrived_ts_ms": v.arrived_ts_ms,
                    "source_ts_ms": v.source_ts_ms,
                    "lag_ms": (v.arrived_ts_ms - v.source_ts_ms) if v.source_ts_ms else None,
                    "relative_ms": v.arrived_ts_ms - self.first_arrival_ts_ms,
                    "seq": v.seq,
                    "backfilled": v.backfilled,
                    "retracted": v.retracted,
                    "retract_ts_ms": v.retract_ts_ms,
                }
                for v in sorted(self.votes.values(), key=lambda x: x.arrived_ts_ms)
            ],
        }


@dataclass
class Correction:
    """纠错/撤回流水。"""
    correction_id: str
    match_id: str
    ts_ms: int
    source_id: str
    source_event_id: str
    fused_id: Optional[str]
    kind: str                                    # RETRACT / STATUS_CHANGE / SCORE_ROLLBACK / OFFICIAL_OVERRIDE
    event_type: Optional[str]
    detail: str
    before: Optional[dict] = None
    after: Optional[dict] = None

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items()}


@dataclass
class MatchState:
    match_id: str
    competition: str = ""
    home_team_id: str = ""
    away_team_id: str = ""
    home_name: str = ""
    away_name: str = ""
    phase: str = "PRE_MATCH"                      # PRE_MATCH / LIVE / HALFTIME / FINISHED
    authoritative_score: list[int] = field(default_factory=lambda: [0, 0])
    pending_score: list[int] = field(default_factory=lambda: [0, 0])
    events: list[FusedEvent] = field(default_factory=list)
    corrections: list[Correction] = field(default_factory=list)
    # 每家来源的传输层计数
    transport: dict[str, dict] = field(default_factory=dict)
    started_ts_ms: Optional[int] = None
    finished_ts_ms: Optional[int] = None
    # (source_id, source_event_id) -> fused_id，撤回与去重用
    source_index: dict[tuple, str] = field(default_factory=dict)

    def to_summary(self) -> dict:
        return {
            "match_id": self.match_id,
            "competition": self.competition,
            "home_team_id": self.home_team_id,
            "away_team_id": self.away_team_id,
            "home_name": self.home_name,
            "away_name": self.away_name,
            "phase": self.phase,
            "authoritative_score": list(self.authoritative_score),
            "pending_score": list(self.pending_score),
            "event_count": len(self.events),
            "started_ts_ms": self.started_ts_ms,
            "finished_ts_ms": self.finished_ts_ms,
        }

    def find_event(self, fused_id: str) -> Optional[FusedEvent]:
        for ev in self.events:
            if ev.fused_id == fused_id:
                return ev
        return None
