"""统一数据模型：三家数据商的报文进入融合引擎后全部归一到这里的结构。"""
from __future__ import annotations

import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any


class EventType(str, Enum):
    """统一事件类型编码。"""
    GOAL = "goal"                       # 进球
    OWN_GOAL = "own_goal"               # 乌龙球
    PENALTY_GOAL = "penalty_goal"       # 点球进球
    PENALTY_MISSED = "penalty_missed"   # 点球未进
    YELLOW_CARD = "yellow_card"         # 黄牌
    RED_CARD = "red_card"               # 红牌
    SUBSTITUTION = "substitution"       # 换人
    VAR = "var"                         # VAR 复核
    KICKOFF = "kickoff"                 # 开球
    HALFTIME = "halftime"               # 半场
    FULLTIME = "fulltime"               # 终场
    UNKNOWN = "unknown"                 # 未识别类型（映射缺失时兜底）


class IncidentStatus(str, Enum):
    """融合事件的生命周期状态。"""
    PENDING = "pending"       # 仅 1 家报道，待更多确认
    CONFIRMED = "confirmed"   # >=2 家确认（多数）
    OFFICIAL = "official"     # 官方源确认的比分类事件
    RETRACTED = "retracted"   # 支撑报道全部撤回 / 被判误报


class SourceStatus(str, Enum):
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    RECONNECTING = "reconnecting"


@dataclass
class SourceReport:
    """单家数据商对一个事件的一次报道（或撤回）。"""
    seq: int                         # 该源内单调递增序号，用于断线补漏
    provider: str                    # alpha / beta / gamma
    retraction: bool                 # True 表示这是一条撤回指令
    ref_id: str                      # 该源对自己早先报道的引用 id（撤回时用）
    event_type: str                  # 已映射的统一类型
    minute: int
    team: str                        # 已映射的统一球队编码
    player: str                      # 已映射的统一球员编码（无则空串）
    player_in: str                   # 换人：上场球员
    player_out: str                  # 换人：下场球员
    score_home: int | None           # 该源声称的累计比分（可能为空）
    score_away: int | None
    raw: dict[str, Any]              # 原始报文，审计用
    emitted_at: float                # 模拟器发出（计划）时间
    received_at: float = field(default_factory=time.time)  # 采集端实际收到时间

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Incident:
    """融合后的统一事件（同一事件多家报道合并为一条）。"""
    incident_id: str
    event_type: str
    minute: int
    team: str
    player: str = ""
    player_in: str = ""
    player_out: str = ""
    status: str = IncidentStatus.PENDING.value
    score_home: int | None = None
    score_away: int | None = None
    sources: dict[str, SourceReport] = field(default_factory=dict)  # provider -> 最新报道
    retracted_sources: list[str] = field(default_factory=list)
    first_arrival: float | None = None
    last_update: float = field(default_factory=time.time)
    history: list[dict[str, Any]] = field(default_factory=list)      # 状态变迁审计

    def active_providers(self) -> list[str]:
        return [p for p in self.sources if p not in self.retracted_sources]

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        # asdict 已把嵌套 SourceReport 递归转成 dict，直接补算有效来源即可
        d["active_providers"] = self.active_providers()
        return d


@dataclass
class Correction:
    """纠错记录：撤回、误报、状态翻转都留痕。"""
    correction_id: str
    match_id: str
    incident_id: str
    kind: str                 # retraction / false_report / status_change / score_revision
    provider: str             # 发起方（fusion 表示融合引擎自身）
    detail: str
    event_type: str = ""
    minute: int | None = None
    at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class MatchState:
    """一场比赛的完整融合状态。"""
    match_id: str
    home: str
    away: str
    status: str = "scheduled"          # scheduled / live / halftime / finished
    score_home: int = 0
    score_away: int = 0
    pending_goals: list[dict[str, Any]] = field(default_factory=list)  # 非官方先报、待官方确认的球
    incidents: list[Incident] = field(default_factory=list)
    corrections: list[Correction] = field(default_factory=list)
    started_at: float | None = None
    finished_at: float | None = None

    def to_dict(self) -> dict[str, Any]:
        # 手动组装：asdict 会递归转换嵌套 dataclass，绕过 Incident.to_dict
        return {
            "match_id": self.match_id,
            "home": self.home,
            "away": self.away,
            "status": self.status,
            "score_home": self.score_home,
            "score_away": self.score_away,
            "pending_goals": list(self.pending_goals),
            "incidents": [i.to_dict() for i in self.incidents],
            "corrections": [c.to_dict() for c in self.corrections],
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }
