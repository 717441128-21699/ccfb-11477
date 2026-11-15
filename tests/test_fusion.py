"""融合引擎测试：多数确认、撤回、官方比分权威、待确认标记。"""
import asyncio
import unittest

from fusion.engine import FusionEngine
from fusion.models import EventStatus, Report


def rep(source, eid, etype, minute, team="T-H", player="P1",
        score=None, retraction=False, seq=None):
    return Report(
        source_id=source, source_event_id=eid, event_type=etype, match_id="M1",
        minute=minute, team_id=team, player_id=player,
        score_home=score[0] if score else None,
        score_away=score[1] if score else None,
        retraction=retraction, seq=seq,
    )


class FusionEngineTests(unittest.TestCase):
    def setUp(self):
        self.engine = FusionEngine(official_source="A", majority=2, minute_window=1)
        m = self.engine.register_match({
            "match_id": "M1", "competition": "t",
            "home_team": {"id": "T-H", "canonical": "主", "aliases": {}},
            "away_team": {"id": "T-A", "canonical": "客", "aliases": {}},
            "sources": {"A": {}, "B": {}, "C": {}},
        })

    def run(self, result=None):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            super().run(result)
        finally:
            loop.close()

    async def _feed(self, *reports):
        for r in reports:
            await self.engine.handle_report(r)

    def test_pending_then_majority_confirm(self):
        asyncio.get_event_loop().run_until_complete(self._feed(
            rep("B", "e1-B", "goal", 23, score=[1, 0]),
        ))
        m = self.engine.get_match("M1")
        ev = m.events[0]
        self.assertEqual(ev.status, EventStatus.PENDING)
        # 官方未确认：只进待确认比分
        self.assertEqual(m.pending_score, [1, 0])
        self.assertEqual(m.authoritative_score, [0, 0])

        asyncio.get_event_loop().run_until_complete(self._feed(
            rep("C", "e1-C", "goal", 23, score=[1, 0]),
        ))
        self.assertEqual(ev.status, EventStatus.CONFIRMED)
        # 官方源仍未报：权威比分依旧 0:0，待确认 1:0
        self.assertEqual(m.authoritative_score, [0, 0])
        self.assertEqual(m.pending_score, [1, 0])

        asyncio.get_event_loop().run_until_complete(self._feed(
            rep("A", "e1-A", "goal", 23, score=[1, 0]),
        ))
        self.assertEqual(m.authoritative_score, [1, 0])
        self.assertEqual(m.pending_score, [1, 0])

    def test_official_vote_confirms_alone(self):
        asyncio.get_event_loop().run_until_complete(self._feed(
            rep("A", "e2-A", "yellow_card", 10),
        ))
        ev = self.engine.get_match("M1").events[0]
        self.assertEqual(ev.status, EventStatus.CONFIRMED)

    def test_minute_window_matching(self):
        asyncio.get_event_loop().run_until_complete(self._feed(
            rep("B", "e3-B", "goal", 30),
            rep("C", "e3-C", "goal", 31),  # 分钟差 1，应并入同一事件
        ))
        m = self.engine.get_match("M1")
        self.assertEqual(len(m.events), 1)
        self.assertEqual(set(m.events[0].votes), {"B", "C"})

    def test_full_retraction_marks_event_retracted(self):
        asyncio.get_event_loop().run_until_complete(self._feed(
            rep("B", "f1-B", "goal", 43, team="T-A", player="P9", score=[0, 1]),
            rep("B", "f1-B", "goal", 43, team="T-A", player="P9", retraction=True),
        ))
        m = self.engine.get_match("M1")
        ev = m.events[0]
        self.assertEqual(ev.status, EventStatus.RETRACTED)
        self.assertEqual(ev.active_votes, [])
        self.assertTrue(any(c.kind == "RETRACT" for c in m.corrections))
        self.assertEqual(m.authoritative_score, [0, 0])
        self.assertEqual(m.pending_score, [0, 0])

    def test_partial_retraction_keeps_other_votes(self):
        asyncio.get_event_loop().run_until_complete(self._feed(
            rep("B", "g-B", "goal", 50, score=[1, 0]),
            rep("C", "g-C", "goal", 50, score=[1, 0]),
            rep("B", "g-B", "goal", 50, retraction=True),
        ))
        ev = self.engine.get_match("M1").events[0]
        self.assertEqual(ev.sources, ["C"])
        self.assertEqual(ev.status, EventStatus.PENDING)  # 只剩非官方单票

    def test_official_retraction_rolls_back_authoritative_score(self):
        asyncio.get_event_loop().run_until_complete(self._feed(
            rep("A", "og-A", "goal", 79, score=[5, 2]),
            rep("B", "og-B", "goal", 79, score=[5, 2]),
            rep("C", "og-C", "goal", 79, score=[5, 2]),
        ))
        m = self.engine.get_match("M1")
        self.assertEqual(m.authoritative_score, [1, 0])
        asyncio.get_event_loop().run_until_complete(self._feed(
            rep("A", "og-A", "goal", 79, retraction=True),
        ))
        # 官方撤回：权威比分回退，B/C 的票仍以待确认保留
        self.assertEqual(m.authoritative_score, [0, 0])
        self.assertEqual(m.pending_score, [1, 0])
        self.assertTrue(any(c.kind == "SCORE_ROLLBACK" for c in m.corrections))

    def test_duplicate_delivery_is_idempotent(self):
        asyncio.get_event_loop().run_until_complete(self._feed(
            rep("B", "d-B", "goal", 60, seq=5),
            rep("B", "d-B", "goal", 60, seq=5),  # 补传重叠窗口重复送达
        ))
        ev = self.engine.get_match("M1").events[0]
        self.assertEqual(len(ev.votes), 1)

    def test_own_goal_counts_for_opponent(self):
        asyncio.get_event_loop().run_until_complete(self._feed(
            rep("A", "og-A", "own_goal", 75, team="T-A", player="P2"),
        ))
        m = self.engine.get_match("M1")
        self.assertEqual(m.authoritative_score, [1, 0])  # 客队乌龙 -> 主队得分

    def test_control_phase_transitions(self):
        asyncio.get_event_loop().run_until_complete(
            self.engine.apply_control(rep("A", "ko-A", "kickoff", 0)))
        self.assertEqual(self.engine.get_match("M1").phase, "LIVE")
        asyncio.get_event_loop().run_until_complete(
            self.engine.apply_control(rep("A", "ft-A", "fulltime", 90)))
        self.assertEqual(self.engine.get_match("M1").phase, "FINISHED")


if __name__ == "__main__":
    unittest.main()
