from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from d_brain.services.decision_models import DecisionRunStatus, ReviewStatus
from d_brain.services.decision_store import DecisionStore


class DecisionStoreTests(unittest.TestCase):
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

    def test_persists_runs_records_and_reviews(self) -> None:
        run = self.store.persist_run(
            1,
            "What should I focus on next?",
            verdict=None,
            status=DecisionRunStatus.RECEIVED,
            decision_type="prioritization",
            source_message_id=77,
            time_horizon_days=14,
        )

        self.assertEqual(run.id, 1)
        self.assertEqual(run.status, DecisionRunStatus.RECEIVED)
        self.assertEqual(run.created_at, self.current_time)

        self.current_time = self.current_time + timedelta(minutes=5)
        updated_run = self.store.update_run_status(
            run.id,
            DecisionRunStatus.REASONING,
            final_verdict="Focus on the onboarding flow.",
        )

        self.assertEqual(updated_run.status, DecisionRunStatus.REASONING)
        self.assertEqual(updated_run.final_verdict, "Focus on the onboarding flow.")
        self.assertEqual(updated_run.updated_at, self.current_time)

        self.current_time = self.current_time + timedelta(minutes=5)
        record = self.store.persist_decision(
            1,
            decision_run_id=run.id,
            title="Focus on onboarding",
            decision_summary="Freeze side experiments and focus on onboarding.",
            chosen_option="Onboarding",
            rejected_options=["Landing page tweak", "New feature"],
            why="It has the clearest evidence of user pain.",
            risks="May overfit to current sample size.",
            expected_signals=["more activations", "fewer drop-offs"],
            decision_type="prioritization",
            confidence=0.83,
        )

        self.assertEqual(record.id, 1)
        self.assertEqual(record.rejected_options, ["Landing page tweak", "New feature"])
        self.assertEqual(record.expected_signals, ["more activations", "fewer drop-offs"])
        self.assertEqual(record.review_date, self.current_time + timedelta(days=14))

        self.current_time = self.current_time + timedelta(minutes=5)
        review = self.store.create_review(
            workspace_id="workspace-1",
            decision_record_id=record.id,
            due_at=self.current_time + timedelta(days=14),
            expected_outcome="Onboarding conversion should improve.",
        )

        self.assertEqual(review.id, 1)
        self.assertEqual(review.status, ReviewStatus.SCHEDULED)
        self.assertEqual(review.due_at, self.current_time + timedelta(days=14))

        self.current_time = self.current_time + timedelta(days=15)
        due_reviews = self.store.list_due_reviews(self.current_time)
        self.assertEqual([item.id for item in due_reviews], [review.id])

        self.current_time = self.current_time + timedelta(minutes=10)
        completed_review = self.store.update_review(
            review.id,
            ReviewStatus.COMPLETED,
            user_response="It worked. The onboarding flow is now the focus.",
            actual_outcome="Conversion improved and support tickets dropped.",
            agent_assessment="Recommendation validated.",
        )

        self.assertEqual(completed_review.status, ReviewStatus.COMPLETED)
        self.assertEqual(completed_review.completed_at, self.current_time)
        self.assertEqual(completed_review.actual_outcome, "Conversion improved and support tickets dropped.")

        self.store.close()
        self.store = DecisionStore(self.db_path, clock=self.clock)

        loaded_run = self.store.get_run(run.id)
        loaded_record = self.store.get_record(record.id)
        loaded_review = self.store.get_review(review.id)

        self.assertEqual(loaded_run.final_verdict, "Focus on the onboarding flow.")
        self.assertEqual(loaded_record.chosen_option, "Onboarding")
        self.assertEqual(loaded_review.status, ReviewStatus.COMPLETED)
        self.assertEqual(loaded_review.actual_outcome, "Conversion improved and support tickets dropped.")

        recent_runs = self.store.list_recent(1)
        self.assertEqual([item.id for item in recent_runs], [run.id])

    def test_filters_due_reviews_by_status_and_due_date(self) -> None:
        record = self.store.persist_decision(
            "workspace-1",
            decision_run_id=self.store.persist_run(
                "workspace-1",
                "Should I cut this project?",
                decision_type="cut",
            ).id,
            title="Cut the project",
            decision_summary="Stop the low-signal project.",
            chosen_option="Cut",
            rejected_options=["Keep"],
            why="No evidence of traction.",
            risks="Could miss a delayed signal.",
            expected_signals=["less context switching"],
            decision_type="cut",
            time_horizon_days=14,
            confidence=0.9,
        )

        due_review = self.store.create_review(
            workspace_id="workspace-1",
            decision_record_id=record.id,
            due_at=self.current_time - timedelta(days=1),
            expected_outcome="Should confirm the cut was beneficial.",
        )
        future_review = self.store.create_review(
            workspace_id="workspace-1",
            decision_record_id=record.id,
            due_at=self.current_time + timedelta(days=7),
            expected_outcome="Should remain scheduled.",
        )
        completed_review = self.store.create_review(
            workspace_id="workspace-1",
            decision_record_id=record.id,
            due_at=self.current_time - timedelta(days=2),
            expected_outcome="Should be ignored once completed.",
        )

        self.store.update_review(completed_review.id, ReviewStatus.COMPLETED)

        due_reviews = self.store.list_due_reviews(self.current_time)

        self.assertEqual([item.id for item in due_reviews], [due_review.id])
        self.assertNotIn(future_review.id, [item.id for item in due_reviews])


if __name__ == "__main__":
    unittest.main()
