from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from d_brain.services.decision_models import DecisionOutcomeStatus, PatternStatus
from d_brain.services.decision_store import DecisionStore
from d_brain.services.review_service import ReviewService, ReviewServiceError


class ReviewServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "decision-store.sqlite3"
        self.current_time = datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc)
        self.store = DecisionStore(self.db_path, clock=self.clock)
        self.service = ReviewService(store=self.store, clock=self.clock)

    def tearDown(self) -> None:
        self.store.close()
        self.tmpdir.cleanup()

    def clock(self) -> datetime:
        return self.current_time

    def _seed_due_review(self, workspace_id: str = "42") -> int:
        run = self.store.persist_run(workspace_id, "What should I focus on?")
        record = self.store.persist_decision(
            workspace_id,
            decision_run_id=run.id,
            title="Focus on onboarding",
            decision_summary="Freeze experiments and focus on onboarding.",
            chosen_option="Onboarding",
            rejected_options=["New feature"],
            why="Highest evidence.",
            risks="Sample too small.",
            expected_signals=["more activations"],
            linked_pattern_names=["focus_fragmentation"],
        )
        review = self.store.create_review(
            workspace_id=workspace_id,
            decision_record_id=record.id,
            due_at=self.current_time - timedelta(days=1),
            expected_outcome="more activations",
        )
        self.store.persist_pattern(
            workspace_id,
            name="focus_fragmentation",
            category="decision_pattern",
            description="Ты снова пытаешься удерживать слишком много направлений одновременно.",
            evidence=["В запросе есть признаки перегрузки и множественного выбора."],
            confidence=0.6,
            status=PatternStatus.WATCH,
            last_seen_at=self.current_time - timedelta(days=2),
        )
        return review.id

    def test_render_review_overview_shows_due_review_and_commands(self) -> None:
        review_id = self._seed_due_review()

        rendered = self.service.render_review_overview(42)

        self.assertIn("Пора проверить решение", rendered)
        self.assertIn(f"<code>{review_id}</code>", rendered)
        self.assertIn("/review_done", rendered)
        self.assertIn("/review_skip", rendered)

        updated = self.store.get_review(review_id)
        self.assertEqual(updated.status.value, "due")

    def test_complete_review_updates_status_and_outcome(self) -> None:
        review_id = self._seed_due_review()

        rendered = self.service.complete_review(42, review_id, "Активации выросли, фокус подтвердился")

        self.assertIn("Review закрыт", rendered)
        self.assertIn("confirmed", rendered)
        updated = self.store.get_review(review_id)
        self.assertEqual(updated.status.value, "completed")
        self.assertEqual(updated.actual_outcome, "Активации выросли, фокус подтвердился")
        record = self.store.get_record(updated.decision_record_id)
        self.assertEqual(record.outcome_status, DecisionOutcomeStatus.CONFIRMED)
        self.assertEqual(record.outcome_summary, "Активации выросли, фокус подтвердился")
        self.assertFalse(record.needs_follow_up)

    def test_complete_review_marks_invalidated_decision_for_follow_up(self) -> None:
        review_id = self._seed_due_review()

        rendered = self.service.complete_review(42, review_id, "Активации не выросли, решение не сработало")

        self.assertIn("invalidated", rendered)
        updated = self.store.get_review(review_id)
        record = self.store.get_record(updated.decision_record_id)
        self.assertEqual(record.outcome_status, DecisionOutcomeStatus.INVALIDATED)
        self.assertTrue(record.needs_follow_up)
        patterns = self.store.list_patterns("42")
        focus_pattern = next(pattern for pattern in patterns if pattern.name == "focus_fragmentation")
        self.assertEqual(focus_pattern.status, PatternStatus.ACTIVE)
        self.assertGreaterEqual(focus_pattern.confidence, 0.85)
        self.assertIn("invalidated", " ".join(focus_pattern.evidence))

    def test_skip_review_updates_status(self) -> None:
        review_id = self._seed_due_review()

        rendered = self.service.skip_review(42, review_id)

        self.assertIn("Review пропущен", rendered)
        updated = self.store.get_review(review_id)
        self.assertEqual(updated.status.value, "skipped")

    def test_complete_review_softens_linked_pattern_when_decision_is_confirmed(self) -> None:
        review_id = self._seed_due_review()

        self.service.complete_review(42, review_id, "Активации выросли, фокус подтвердился")

        patterns = self.store.list_patterns("42")
        focus_pattern = next(pattern for pattern in patterns if pattern.name == "focus_fragmentation")
        self.assertEqual(focus_pattern.status, PatternStatus.WATCH)
        self.assertIn("confirmed", " ".join(focus_pattern.evidence))

    def test_skip_review_rejects_foreign_review(self) -> None:
        review_id = self._seed_due_review("workspace-1")

        with self.assertRaises(ReviewServiceError):
            self.service.skip_review(42, review_id)

    def test_complete_review_rejects_foreign_review(self) -> None:
        review_id = self._seed_due_review("workspace-1")

        with self.assertRaises(ReviewServiceError):
            self.service.complete_review(42, review_id, "Не должен иметь доступ")

    def test_complete_review_rejects_already_completed_review(self) -> None:
        review_id = self._seed_due_review()
        self.service.complete_review(42, review_id, "Активации выросли, фокус подтвердился")

        with self.assertRaises(ReviewServiceError):
            self.service.complete_review(42, review_id, "Вторая попытка")

    def test_skip_review_rejects_already_completed_review(self) -> None:
        review_id = self._seed_due_review()
        self.service.complete_review(42, review_id, "Активации выросли, фокус подтвердился")

        with self.assertRaises(ReviewServiceError):
            self.service.skip_review(42, review_id)

    def test_complete_review_rejects_already_skipped_review(self) -> None:
        review_id = self._seed_due_review()
        self.service.skip_review(42, review_id)

        with self.assertRaises(ReviewServiceError):
            self.service.complete_review(42, review_id, "Поздний итог")


if __name__ == "__main__":
    unittest.main()
