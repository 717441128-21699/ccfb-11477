"""模拟器：三家数据商的原生报文编码器（与 fusion/mapping.py 的适配器一一对应）。"""
from __future__ import annotations

from typing import Optional


class EnvelopeBuilder:
    def __init__(self, scenario: dict):
        self.m = scenario
        self.team_alias = {s: {} for s in scenario["sources"]}
        for side in ("home_team", "away_team"):
            t = scenario[side]
            for src, alias in t["aliases"].items():
                self.team_alias[src][t["id"]] = alias
        self.player_alias = {s: {} for s in scenario["sources"]}
        for p in scenario["players"]:
            for src, alias in p["aliases"].items():
                self.player_alias[src][p["id"]] = alias

    def team(self, source_id: str, team_id: Optional[str]) -> Optional[str]:
        if team_id is None:
            return None
        return self.team_alias[source_id][team_id]

    def player(self, source_id: str, player_id: Optional[str]) -> Optional[str]:
        if player_id is None:
            return None
        return self.player_alias[source_id][player_id]

    def build(self, source_id: str, item: dict, seq: int, ts_ms: int,
              backfilled: bool = False) -> dict:
        """item: 调度器展开后的单源事件 dict（含 event_type/原生码等）。"""
        fn = {
            "A": self._build_a,
            "B": self._build_b,
            "C": self._build_c,
        }[source_id]
        return fn(item, seq, ts_ms, backfilled)

    # ---------------- A：官方 JSON 信封 ----------------

    _A_CODES = {
        "goal": "GOAL", "penalty_goal": "PENALTY_GOAL", "penalty_miss": "PENALTY_MISS",
        "own_goal": "OWN_GOAL", "yellow_card": "YELLOW_CARD", "red_card": "RED_CARD",
        "substitution": "SUBSTITUTION", "kickoff": "KICKOFF", "halftime": "HALFTIME",
        "fulltime": "FULLTIME",
    }

    def _build_a(self, item, seq, ts_ms, backfilled):
        payload = {"event_code": self._A_CODES[item["event_type"]], "minute": item.get("minute")}
        if item.get("team_id"):
            payload["team"] = self.team("A", item["team_id"])
        if item.get("player_id"):
            payload["player"] = self.player("A", item["player_id"])
        if item.get("score_after"):
            payload["score"] = list(item["score_after"])
        env = {
            "event_id": item["source_event_id"],
            "match_id": self.m["match_id"],
            "action": "RETRACT" if item["retraction"] else "POST",
            "payload": payload,
            "ts_ms": ts_ms,
            "seq": seq,
        }
        if backfilled:
            env["backfilled"] = True
        return env

    # ---------------- B：紧凑短码信封 ----------------

    _B_CODES = {
        "goal": "G", "penalty_goal": "PG", "penalty_miss": "PM", "own_goal": "OG",
        "yellow_card": "Y", "red_card": "R", "substitution": "SUB",
        "kickoff": "KO", "halftime": "HT", "fulltime": "FT",
    }

    def _build_b(self, item, seq, ts_ms, backfilled):
        env = {
            "msg_type": "RETRACT" if item["retraction"] else "EVENT",
            "eid": item["source_event_id"],
            "mid": self.m["match_id"],
            "ev": self._B_CODES[item["event_type"]],
            "min": item.get("minute"),
            "side": self.team("B", item.get("team_id")),
            "who": self.player("B", item.get("player_id")),
            "t": ts_ms,
            "seq": seq,
        }
        if item.get("player_on_id"):
            env["on"] = self.player("B", item["player_on_id"])
        if item.get("player_off_id"):
            env["off"] = self.player("B", item["player_off_id"])
        if item.get("score_after"):
            env["sh"], env["sa"] = item["score_after"]
        if backfilled:
            env["bf"] = True
        return env

    # ---------------- C：中文长轮询信封 ----------------

    _C_CODES = {
        "goal": "进球", "penalty_goal": "点球进", "penalty_miss": "点球未进",
        "own_goal": "乌龙球", "yellow_card": "黄牌", "red_card": "红牌",
        "substitution": "换人", "kickoff": "开球", "halftime": "半场", "fulltime": "完场",
    }

    def _build_c(self, item, seq, ts_ms, backfilled):
        env = {
            "kind": "RETRACT" if item["retraction"] else "EVENT",
            "uid": item["source_event_id"],
            "match": self.m["match_id"],
            "etype": self._C_CODES[item["event_type"]],
            "minute": item.get("minute"),
            "team_name": self.team("C", item.get("team_id")),
            "player_name": self.player("C", item.get("player_id")),
            "emitted_at": ts_ms,
            "seq": seq,
        }
        if item.get("player_on_id"):
            env["player_in"] = self.player("C", item["player_on_id"])
        if item.get("player_off_id"):
            env["player_out"] = self.player("C", item["player_off_id"])
        if item.get("score_after"):
            env["score"] = list(item["score_after"])
        if backfilled:
            env["replayed"] = True
        return env
