from __future__ import annotations

import unittest
from datetime import datetime, timezone

from d_brain.services.decision_models import DecisionOutcomeStatus, PatternRecord, PatternStatus
from d_brain.services.review_pattern_feedback import build_pattern_feedback


class ReviewPatternFeedbackTests(unittest.TestCase):
    def setUp(self) -> None:
        self.reviewed_at = datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc)
        self.pattern = PatternRecord(
            id=1,
            workspace_id="42",
            name="focus_fragmentation",
            category="decision_pattern",
            description="Keep focus narrow.",
            evidence=["Existing evidence"],
            confidence=0.60,
            status=PatternStatus.WATCH,
            last_seen_at=self.reviewed_at,
            created_at=self.reviewed_at,
            updated_at=self.reviewed_at,
        )

    def test_builds_confirmed_feedback(self) -> None:
        update = build_pattern_feedback(
            self.pattern,
            DecisionOutcomeStatus.CONFIRMED,
            review_id=7,
            outcome_summary="Aктивации выросли, фокус подтвердился",
        )

        self.assertEqual(update.category, self.pattern.category)
        self.assertEqual(update.status, PatternStatus.WATCH)
        self.assertEqual(update.confidence, 0.60)
        self.assertIn("Confirmed by review 7", update.description)
        self.assertIn("Review 7 confirmed decision outcome", " ".join(update.evidence))
        self.assertEqual(update.evidence[0], "Existing evidence")

    def test_builds_mixed_feedback(self) -> None:
        update = build_pattern_feedback(
            self.pattern,
            DecisionOutcomeStatus.MIXED,
            review_id=7,
            outcome_summary="Часть сигналов подтвердилась, часть нет",
        )

        self.assertEqual(update.status, PatternStatus.WATCH)
        self.assertEqual(update.confidence, 0.70)
        self.assertIn("Mixed review 7", update.description)
        self.assertIn("Review 7 produced mixed decision outcome", " ".join(update.evidence))

    def test_builds_invalidated_feedback(self) -> None:
        update = build_pattern_feedback(
            self.pattern,
            DecisionOutcomeStatus.INVALIDATED,
            review_id=7,
            outcome_summary="Активации не выросли, решение не сработало",
        )

        self.assertEqual(update.status, PatternStatus.ACTIVE)
        self.assertEqual(update.confidence, 0.85)
        self.assertIn("Not confirmed by review 7", update.description)
        self.assertIn("Review 7 invalidated decision outcome", " ".join(update.evidence))

    def test_rejects_unknown_outcome_status(self) -> None:
        with self.assertRaises(ValueError):
            build_pattern_feedback(
                self.pattern,
                DecisionOutcomeStatus.UNKNOWN,
                review_id=7,
                outcome_summary="неважно",
            )


if __name__ == "__main__":
    unittest.main()
