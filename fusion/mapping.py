"""名称/编码映射目录 + 三家数据商原生报文适配器。

模拟器按各家"原生格式"发包（刻意做成三种不同风格），
适配器负责解析原生包并通过 MappingCatalog 统一成 fusion.models.Report。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .models import EVENT_TYPES, Report


class MappingError(Exception):
    """映射失败（未知球队/球员/事件码）。"""


# 各家原生事件码 -> 统一编码
SOURCE_EVENT_CODES = {
    "A": {
        "GOAL": "goal",
        "PENALTY_GOAL": "penalty_goal",
        "PENALTY_MISS": "penalty_miss",
        "OWN_GOAL": "own_goal",
        "YELLOW_CARD": "yellow_card",
        "RED_CARD": "red_card",
        "SUBSTITUTION": "substitution",
        "KICKOFF": "kickoff",
        "HALFTIME": "halftime",
        "FULLTIME": "fulltime",
    },
    "B": {
        "G": "goal", "PG": "penalty_goal", "PM": "penalty_miss", "OG": "own_goal",
        "Y": "yellow_card", "R": "red_card", "SUB": "substitution",
        "KO": "kickoff", "HT": "halftime", "FT": "fulltime",
    },
    "C": {
        "进球": "goal",
        "点球进": "penalty_goal",
        "点球未进": "penalty_miss",
        "乌龙球": "own_goal",
        "黄牌": "yellow_card",
        "红牌": "red_card",
        "换人": "substitution",
        "开球": "kickoff",
        "半场": "halftime",
        "完场": "fulltime",
    },
}

# 长轮询 C 的更新类型
C_UPDATE_KIND = {"EVENT": False, "RETRACT": True}


@dataclass
class MappingCatalog:
    match_id: str
    team_alias: dict[str, dict[str, str]]          # source_id -> {alias: canonical_team_id}
    player_alias: dict[str, dict[str, str]]        # source_id -> {alias/player_code: canonical_player_id}
    event_codes: dict[str, dict[str, str]]

    @classmethod
    def from_scenario(cls, scenario: dict) -> "MappingCatalog":
        team_alias: dict[str, dict[str, str]] = {"A": {}, "B": {}, "C": {}}
        for side in ("home_team", "away_team"):
            t = scenario[side]
            for src, alias in t["aliases"].items():
                team_alias[src][alias] = t["id"]
        player_alias: dict[str, dict[str, str]] = {"A": {}, "B": {}, "C": {}}
        for p in scenario["players"]:
            for src, alias in p["aliases"].items():
                player_alias[src][alias] = p["id"]
            # 球衣号也作为可选键（适配器按 source 自行决定使用方式）
        return cls(scenario["match_id"], team_alias, player_alias, SOURCE_EVENT_CODES)

    def map_team(self, source_id: str, raw_name: Optional[str]) -> Optional[str]:
        if raw_name is None:
            return None
        table = self.team_alias.get(source_id, {})
        if raw_name not in table:
            raise MappingError(f"source {source_id} 未知球队名: {raw_name!r}")
        return table[raw_name]

    def map_player(self, source_id: str, raw_name: Optional[str]) -> Optional[str]:
        if raw_name is None:
            return None
        table = self.player_alias.get(source_id, {})
        if raw_name not in table:
            raise MappingError(f"source {source_id} 未知球员名: {raw_name!r}")
        return table[raw_name]

    def map_event_code(self, source_id: str, raw_code: str) -> str:
        table = self.event_codes.get(source_id, {})
        if raw_code not in table:
            raise MappingError(f"source {source_id} 未知事件码: {raw_code!r}")
        event_type = table[raw_code]
        if event_type not in EVENT_TYPES:
            raise MappingError(f"统一事件编码非法: {event_type!r}")
        return event_type


# ------------------------- 三家原生报文适配器 -------------------------

class BaseAdapter:
    source_id = ""

    def __init__(self, catalog: MappingCatalog):
        self.catalog = catalog

    def parse(self, envelope: dict) -> Report:
        raise NotImplementedError


class AdapterA(BaseAdapter):
    """官方源 A：JSON 信封，大写下划线字段，撤回走 action=RETRACT。"""
    source_id = "A"

    def parse(self, env: dict) -> Report:
        c = self.catalog
        retraction = env.get("action") == "RETRACT"
        payload = env["payload"]
        event_type = c.map_event_code("A", payload["event_code"])
        report = Report(
            source_id="A",
            source_event_id=env["event_id"],
            event_type=event_type,
            match_id=env["match_id"],
            minute=payload.get("minute"),
            team_id=c.map_team("A", payload.get("team")),
            player_id=c.map_player("A", payload.get("player")),
            score_home=payload.get("score", [None, None])[0],
            score_away=payload.get("score", [None, None])[1],
            retraction=retraction,
            source_ts_ms=env.get("ts_ms"),
            seq=env.get("seq"),
            backfilled=bool(env.get("backfilled")),
            raw=env,
        )
        return report


class AdapterB(BaseAdapter):
    """快速源 B：紧凑短码信封，换人用 on/off，撤回单独消息类型。"""
    source_id = "B"

    def parse(self, env: dict) -> Report:
        c = self.catalog
        retraction = env.get("msg_type") == "RETRACT"
        event_type = c.map_event_code("B", env["ev"])
        return Report(
            source_id="B",
            source_event_id=env["eid"],
            event_type=event_type,
            match_id=env["mid"],
            minute=env.get("min"),
            team_id=c.map_team("B", env.get("side")),
            player_id=c.map_player("B", env.get("who")),
            player_on_id=c.map_player("B", env.get("on")),
            player_off_id=c.map_player("B", env.get("off")),
            score_home=env.get("sh"),
            score_away=env.get("sa"),
            retraction=retraction,
            source_ts_ms=env.get("t"),
            seq=env.get("seq"),
            backfilled=bool(env.get("backfilled") or env.get("bf")),
            raw=env,
        )


class AdapterC(BaseAdapter):
    """长轮询源 C：中文信封，更新列表，kind=EVENT/RETRACT。"""
    source_id = "C"

    def parse(self, env: dict) -> Report:
        c = self.catalog
        retraction = C_UPDATE_KIND[env["kind"]]
        event_type = c.map_event_code("C", env["etype"])
        return Report(
            source_id="C",
            source_event_id=env["uid"],
            event_type=event_type,
            match_id=env["match"],
            minute=env.get("minute"),
            team_id=c.map_team("C", env.get("team_name")),
            player_id=c.map_player("C", env.get("player_name")),
            player_on_id=c.map_player("C", env.get("player_in")),
            player_off_id=c.map_player("C", env.get("player_out")),
            score_home=env.get("score", (None, None))[0],
            score_away=env.get("score", (None, None))[1],
            retraction=retraction,
            source_ts_ms=env.get("emitted_at"),
            seq=env.get("seq"),
            backfilled=bool(env.get("backfilled") or env.get("replayed")),
            raw=env,
        )


ADAPTERS: dict[str, type[BaseAdapter]] = {"A": AdapterA, "B": AdapterB, "C": AdapterC}


def build_adapters(catalog: MappingCatalog) -> dict[str, BaseAdapter]:
    return {sid: cls(catalog) for sid, cls in ADAPTERS.items()}
