from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from d_brain.services.decision_models import (
    DecisionOutcomeStatus,
    DecisionRunStatus,
    PatternStatus,
    ReviewStatus,
)
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
        self.assertEqual(record.linked_pattern_names, [])
        self.assertEqual(record.outcome_status, DecisionOutcomeStatus.UNKNOWN)
        self.assertIsNone(record.outcome_summary)
        self.assertFalse(record.needs_follow_up)

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
        self.assertIsNone(due_reviews[0].notified_at)

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
        self.assertEqual(loaded_record.linked_pattern_names, [])
        self.assertEqual(loaded_record.outcome_status, DecisionOutcomeStatus.UNKNOWN)
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

    def test_mark_review_notified_persists_and_hides_review_from_delivery_query(self) -> None:
        record = self.store.persist_decision(
            "workspace-1",
            decision_run_id=self.store.persist_run(
                "workspace-1",
                "What should I review?",
            ).id,
            title="Focus the team",
            decision_summary="Focus on one initiative.",
            chosen_option="Initiative A",
            rejected_options=["Initiative B"],
            why="Best evidence.",
            risks="Might miss other opportunities.",
            expected_signals=["more activations"],
            confidence=0.8,
        )
        review = self.store.create_review(
            workspace_id="workspace-1",
            decision_record_id=record.id,
            due_at=self.current_time - timedelta(days=1),
            expected_outcome="More activations",
        )

        pending = self.store.list_pending_review_notifications(self.current_time)
        self.assertEqual([item.id for item in pending], [review.id])
        self.assertIsNone(pending[0].notified_at)

        self.current_time = self.current_time + timedelta(minutes=5)
        updated = self.store.mark_review_notified(review.id)

        self.assertEqual(updated.status, ReviewStatus.DUE)
        self.assertEqual(updated.notified_at, self.current_time)
        self.assertEqual(self.store.list_pending_review_notifications(self.current_time), [])

        loaded = self.store.get_review(review.id)
        self.assertEqual(loaded.notified_at, self.current_time)

    def test_persists_and_lists_patterns(self) -> None:
        pattern = self.store.persist_pattern(
            "workspace-1",
            name="Premature pivot",
            category="bias",
            description="Switches direction before enough signal.",
            evidence=["3 pivots in 2 weeks", "no validation window"],
            confidence=0.91,
            status=PatternStatus.WATCH,
            last_seen_at=self.current_time,
        )

        self.assertEqual(pattern.id, 1)
        self.assertEqual(pattern.workspace_id, "workspace-1")
        self.assertEqual(pattern.status, PatternStatus.WATCH)
        self.assertEqual(pattern.evidence, ["3 pivots in 2 weeks", "no validation window"])
        self.assertEqual(pattern.last_seen_at, self.current_time)

        self.current_time = self.current_time + timedelta(minutes=1)
        second = self.store.persist_pattern(
            1,
            name="Risk avoidance",
            category="thinking_style",
            description="Prefers safe work over the high-leverage option.",
            evidence=["chooses easier path under pressure"],
            confidence=0.77,
        )

        self.assertEqual(second.id, 2)
        self.assertEqual(second.status, PatternStatus.ACTIVE)

        loaded = self.store.get_pattern(pattern.id)
        self.assertEqual(loaded.description, "Switches direction before enough signal.")
        self.assertEqual(loaded.evidence[0], "3 pivots in 2 weeks")

        active_patterns = self.store.list_patterns("1", status=PatternStatus.ACTIVE)
        watched_patterns = self.store.list_patterns("workspace-1", status=PatternStatus.WATCH)

        self.assertEqual([item.id for item in active_patterns], [second.id])
        self.assertEqual([item.id for item in watched_patterns], [pattern.id])

        self.store.close()
        self.store = DecisionStore(self.db_path, clock=self.clock)

        reloaded = self.store.get_pattern(pattern.id)
        self.assertEqual(reloaded.status, PatternStatus.WATCH)
        self.assertEqual(reloaded.confidence, 0.91)

    def test_persist_pattern_upserts_existing_pattern_by_workspace_and_name(self) -> None:
        original = self.store.persist_pattern(
            "workspace-1",
            name="focus_fragmentation",
            category="decision_pattern",
            description="Old description",
            evidence=["Seen once."],
            confidence=0.55,
            status=PatternStatus.WATCH,
            last_seen_at=self.current_time,
        )

        self.current_time = self.current_time + timedelta(days=1)
        updated = self.store.persist_pattern(
            "workspace-1",
            name="focus_fragmentation",
            category="decision_pattern",
            description="New description",
            evidence=["Seen again.", "Seen once."],
            confidence=0.81,
            status=PatternStatus.ACTIVE,
            last_seen_at=self.current_time,
        )

        self.assertEqual(updated.id, original.id)
        self.assertEqual(updated.description, "New description")
        self.assertEqual(updated.status, PatternStatus.ACTIVE)
        self.assertEqual(updated.confidence, 0.81)
        self.assertEqual(updated.last_seen_at, self.current_time)
        self.assertEqual(updated.evidence, ["Seen once.", "Seen again."])
        self.assertEqual(len(self.store.list_patterns("workspace-1")), 1)

    def test_updates_decision_outcome_and_persists_review_learning(self) -> None:
        run = self.store.persist_run("workspace-1", "Should I double down?")
        record = self.store.persist_decision(
            "workspace-1",
            decision_run_id=run.id,
            title="Double down on onboarding",
            decision_summary="Ignore side paths and focus on onboarding",
            chosen_option="Onboarding",
            rejected_options=["New feature"],
            why="Best signal",
            risks="Could be noise",
            expected_signals=["activation up"],
        )

        self.current_time = self.current_time + timedelta(days=14)
        updated_record = self.store.update_record_outcome(
            record.id,
            outcome_status=DecisionOutcomeStatus.INVALIDATED,
            outcome_summary="Активация не выросла, фокус не подтвердился.",
            needs_follow_up=True,
        )

        self.assertEqual(updated_record.outcome_status, DecisionOutcomeStatus.INVALIDATED)
        self.assertEqual(updated_record.outcome_summary, "Активация не выросла, фокус не подтвердился.")
        self.assertEqual(updated_record.last_reviewed_at, self.current_time)
        self.assertTrue(updated_record.needs_follow_up)

        self.store.close()
        self.store = DecisionStore(self.db_path, clock=self.clock)

        reloaded = self.store.get_record(record.id)
        self.assertEqual(reloaded.outcome_status, DecisionOutcomeStatus.INVALIDATED)
        self.assertEqual(reloaded.outcome_summary, "Активация не выросла, фокус не подтвердился.")
        self.assertEqual(reloaded.last_reviewed_at, self.current_time)
        self.assertTrue(reloaded.needs_follow_up)

    def test_persist_decision_stores_linked_pattern_names(self) -> None:
        run = self.store.persist_run("workspace-1", "Should I double down?")
        record = self.store.persist_decision(
            "workspace-1",
            decision_run_id=run.id,
            title="Double down on onboarding",
            decision_summary="Ignore side paths and focus on onboarding",
            chosen_option="Onboarding",
            rejected_options=["New feature"],
            why="Best signal",
            risks="Could be noise",
            expected_signals=["activation up"],
            linked_pattern_names=["focus_fragmentation", "analysis_paralysis"],
        )

        self.assertEqual(record.linked_pattern_names, ["focus_fragmentation", "analysis_paralysis"])

        self.store.close()
        self.store = DecisionStore(self.db_path, clock=self.clock)

        reloaded = self.store.get_record(record.id)
        self.assertEqual(reloaded.linked_pattern_names, ["focus_fragmentation", "analysis_paralysis"])


if __name__ == "__main__":
    unittest.main()
