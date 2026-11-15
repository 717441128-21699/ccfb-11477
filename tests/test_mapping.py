"""映射层测试：三家原生报文 -> 统一编码。"""
import json
import unittest
from pathlib import Path

from fusion.mapping import (AdapterA, AdapterB, AdapterC, MappingCatalog,
                            MappingError)

SCENARIO = json.loads(Path("scenarios/sample_match.json").read_text(encoding="utf-8"))


class MappingTests(unittest.TestCase):
    def setUp(self):
        self.catalog = MappingCatalog.from_scenario(SCENARIO)

    def test_adapter_a_native_envelope(self):
        env = {
            "event_id": "EV-G1-A", "match_id": "M1001", "action": "POST",
            "payload": {"event_code": "GOAL", "minute": 23,
                        "team": "Arsenal FC", "player": "Bukayo Saka", "score": [1, 0]},
            "ts_ms": 1000, "seq": 3,
        }
        r = AdapterA(self.catalog).parse(env)
        self.assertEqual(r.event_type, "goal")
        self.assertEqual(r.team_id, "T-ARS")
        self.assertEqual(r.player_id, "P-1001")
        self.assertEqual(r.score_home, 1)
        self.assertFalse(r.retraction)

    def test_adapter_a_retraction(self):
        env = {"event_id": "EV-FG1-B", "match_id": "M1001", "action": "RETRACT",
               "payload": {"event_code": "GOAL", "minute": 43, "team": "Chelsea FC",
                           "player": "Nicolas Jackson"}, "ts_ms": 1, "seq": 9}
        r = AdapterA(self.catalog).parse(env)
        self.assertTrue(r.retraction)
        self.assertEqual(r.event_type, "goal")

    def test_adapter_b_compact_codes_and_substitution(self):
        env = {"msg_type": "EVENT", "eid": "EV-S1-B", "mid": "M1001", "ev": "SUB",
               "min": 40, "side": "Arsenal", "off": "G.Jesus", "on": "M.Odegaard",
               "t": 2, "seq": 11}
        r = AdapterB(self.catalog).parse(env)
        self.assertEqual(r.event_type, "substitution")
        self.assertEqual(r.player_off_id, "P-1004")
        self.assertEqual(r.player_on_id, "P-1002")

    def test_adapter_c_chinese_envelope(self):
        env = {"kind": "EVENT", "uid": "EV-Y1-C", "match": "M1001", "etype": "黄牌",
               "minute": 12, "team_name": "车路士", "player_name": "卡斯度",
               "emitted_at": 3, "seq": 2}
        r = AdapterC(self.catalog).parse(env)
        self.assertEqual(r.event_type, "yellow_card")
        self.assertEqual(r.team_id, "T-CHE")
        self.assertEqual(r.player_id, "P-2005")

    def test_adapter_c_retract_kind(self):
        env = {"kind": "RETRACT", "uid": "EV-FR1-B", "match": "M1001", "etype": "红牌",
               "minute": 59, "team_name": "阿仙奴", "player_name": "拉耶",
               "emitted_at": 4, "seq": 20}
        r = AdapterC(self.catalog).parse(env)
        self.assertTrue(r.retraction)
        self.assertEqual(r.event_type, "red_card")

    def test_unknown_team_raises(self):
        with self.assertRaises(MappingError):
            self.catalog.map_team("A", "火星联队")

    def test_unknown_event_code_raises(self):
        with self.assertRaises(MappingError):
            self.catalog.map_event_code("B", "XXXX")


if __name__ == "__main__":
    unittest.main()
