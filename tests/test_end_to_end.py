"""端到端集成测试：模拟器 + 连接器 + 引擎，跑完整个剧本并逐事件校验。"""
import asyncio
import json
import unittest
from pathlib import Path

from validation.replay import run_replay
from validation.validator import validate

SCENARIO_PATH = "scenarios/sample_match.json"


class EndToEndTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.result = asyncio.run(run_replay(SCENARIO_PATH, time_scale=120.0, timeout_s=120))

    def test_all_checks_pass(self):
        for failure in self.result["failures"]:
            self.fail(failure)
        self.assertTrue(self.result["ok"])

    def test_final_score_matches_script(self):
        self.assertEqual(self.result["final_authoritative_score"], [4, 3])

    def test_phase_finished(self):
        self.assertEqual(self.result["phase"], "FINISHED")

    def test_false_events_retracted(self):
        report = self.result["report"]
        retracted = [e for e in report["events"] if e["status"] == "RETRACTED"]
        # 剧本误报：EV-FY1 / EV-FG1 / EV-FR1 / EV-FGO1 共 4 个
        self.assertEqual(len(retracted), 4)
        for e in retracted:
            self.assertEqual(e["sources"], [])
            self.assertTrue(e["retracted_sources"])

    def test_backfill_observed_after_outages(self):
        m_transport = self.result["report"]["per_source"]
        self.assertGreaterEqual(m_transport["B"]["backfilled"], 1)
        self.assertGreaterEqual(m_transport["C"]["backfilled"], 1)
        self.assertGreaterEqual(m_transport["B"]["disconnect_count"], 1)
        self.assertGreaterEqual(m_transport["C"]["disconnect_count"], 1)

    def test_report_rankings_cover_three_sources(self):
        ids = [r["source_id"] for r in self.result["report"]["rankings"]]
        self.assertEqual(sorted(ids), ["A", "B", "C"])

    def test_truth_event_count(self):
        scenario = json.loads(Path(SCENARIO_PATH).read_text(encoding="utf-8"))
        report = self.result["report"]
        truth_ids = {n["truth_id"] for n in scenario["timeline"]
                     if not n["type"].startswith("false_")}
        got = {e["truth_id"] for e in report["events"] if e["truth_id"]}
        missing = truth_ids - got
        self.assertFalse(missing, f"剧本事件未被融合: {missing}")


if __name__ == "__main__":
    unittest.main()
