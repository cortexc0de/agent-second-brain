from __future__ import annotations

import unittest
from datetime import datetime, timezone

from d_brain.services.decision_models import DecisionRecord, PatternRecord, PatternStatus
from d_brain.services.pattern_detector import detect_patterns


class PatternDetectorTests(unittest.TestCase):
    def test_detects_focus_fragmentation_and_analysis_paralysis(self) -> None:
        patterns = detect_patterns(
            "У меня много направлений и вариантов, не понимаю что выбрать из этого хаоса",
        )

        names = {pattern.name for pattern in patterns}
        self.assertIn("focus_fragmentation", names)
        self.assertIn("analysis_paralysis", names)

    def test_detects_premature_pivot_from_recent_records(self) -> None:
        now = datetime(2026, 3, 20, tzinfo=timezone.utc)
        recent_records = [
            DecisionRecord(
                id=1,
                workspace_id="42",
                decision_run_id=1,
                title="A",
                decision_type="prioritize",
                decision_summary="",
                chosen_option="Direction A",
                rejected_options=[],
                why="",
                risks="",
                expected_signals=[],
                time_horizon_days=14,
                review_date=now,
                confidence=0.4,
                created_at=now,
                updated_at=now,
            ),
            DecisionRecord(
                id=2,
                workspace_id="42",
                decision_run_id=2,
                title="B",
                decision_type="prioritize",
                decision_summary="",
                chosen_option="Direction B",
                rejected_options=[],
                why="",
                risks="",
                expected_signals=[],
                time_horizon_days=14,
                review_date=now,
                confidence=0.5,
                created_at=now,
                updated_at=now,
            ),
        ]
        existing_patterns = [
            PatternRecord(
                id=1,
                workspace_id="42",
                name="premature_pivot",
                category="failure_loop",
                description="Existing description",
                evidence=["Seen before."],
                confidence=0.8,
                status=PatternStatus.WATCH,
                last_seen_at=now,
                created_at=now,
                updated_at=now,
            )
        ]

        patterns = detect_patterns(
            "Я снова думаю сменить фокус",
            recent_records=recent_records,
            existing_patterns=existing_patterns,
        )

        pivot = next(pattern for pattern in patterns if pattern.name == "premature_pivot")
        self.assertEqual(pivot.status, PatternStatus.WATCH)
        self.assertGreaterEqual(pivot.confidence, 0.8)
        self.assertIn("уже встречался", " ".join(pivot.evidence))


if __name__ == "__main__":
    unittest.main()
