from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from d_brain.services.decision_models import ReviewStatus
from d_brain.services.decision_store import DecisionStore


class ReviewServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "decision-store.sqlite3"
        self.current_time = datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc)
        self.store = DecisionStore(self.db_path, clock=self.clock)

    def tearDown(self) -> None:
        self.store.close()
        self.tmpdir.cleanup()

    def clock(self) -> datetime:
        return self.current_time

    def _seed_due_review(self):
        run = self.store.persist_run(
            "workspace-1",
            "Should I focus on onboarding?",
            decision_type="prioritize",
        )
        record = self.store.persist_decision(
            "workspace-1",
            decision_run_id=run.id,
            title="Focus on onboarding",
            decision_summary="Freeze experiments and focus on onboarding.",
            chosen_option="Onboarding",
            rejected_options=["Landing page"],
            why="It has the clearest signal.",
            risks="Signal may be small.",
            expected_signals=["more activations", "fewer drop-offs"],
            decision_type="prioritize",
            time_horizon_days=14,
            confidence=0.8,
        )
        review = self.store.create_review(
            workspace_id="workspace-1",
            decision_record_id=record.id,
            due_at=self.current_time - timedelta(hours=1),
            expected_outcome="Onboarding conversion should improve.",
        )
        return record, review

    def test_lists_due_reviews_and_formats_prompt(self) -> None:
        from d_brain.services.review_service import ReviewService

        record, review = self._seed_due_review()
        service = ReviewService(self.store, clock=self.clock)

        due_reviews = service.list_due_reviews()
        self.assertEqual([item.id for item in due_reviews], [review.id])

        prompt = service.format_review_prompt(review)
        self.assertIn("Review check-in", prompt)
        self.assertIn("Focus on onboarding", prompt)
        self.assertIn(record.chosen_option, prompt)
        self.assertIn("Expected signals", prompt)

    def test_completes_and_skips_reviews(self) -> None:
        from d_brain.services.review_service import ReviewService

        _, review = self._seed_due_review()
        service = ReviewService(self.store, clock=self.clock)

        self.current_time = self.current_time + timedelta(minutes=10)
        completed = service.mark_completed(
            review.id,
            actual_outcome="Conversion improved after narrowing focus.",
            user_response="Done.",
            agent_assessment="Validated.",
        )

        self.assertEqual(completed.status, ReviewStatus.COMPLETED)
        self.assertEqual(completed.completed_at, self.current_time)

        self.current_time = self.current_time + timedelta(minutes=10)
        skipped = service.mark_skipped(review.id, reason="Not enough signal yet.")

        self.assertEqual(skipped.status, ReviewStatus.SKIPPED)
        self.assertEqual(skipped.completed_at, self.current_time)


if __name__ == "__main__":
    unittest.main()
