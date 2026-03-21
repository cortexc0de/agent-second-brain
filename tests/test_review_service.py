from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from d_brain.services.decision_models import DecisionOutcomeStatus, PatternStatus, ReviewDeliveryEventType
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

    def _seed_due_review(
        self,
        workspace_id: str = "42",
        *,
        title: str = "Focus on onboarding",
        chosen_option: str = "Onboarding",
        expected_outcome: str = "more activations",
        due_at: datetime | None = None,
    ) -> int:
        run = self.store.persist_run(workspace_id, "What should I focus on?")
        record = self.store.persist_decision(
            workspace_id,
            decision_run_id=run.id,
            title=title,
            decision_summary="Freeze experiments and focus on onboarding.",
            chosen_option=chosen_option,
            rejected_options=["New feature"],
            why="Highest evidence.",
            risks="Sample too small.",
            expected_signals=["more activations"],
            linked_pattern_names=["focus_fragmentation"],
        )
        review = self.store.create_review(
            workspace_id=workspace_id,
            decision_record_id=record.id,
            due_at=due_at or (self.current_time - timedelta(days=1)),
            expected_outcome=expected_outcome,
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

    def test_render_review_overview_shows_latest_delivery_status_when_trace_exists(self) -> None:
        review_id = self._seed_due_review()
        review = self.store.get_review(review_id)
        self.store.append_review_delivery_event(
            review_id=review_id,
            workspace_id=review.workspace_id,
            event_type=ReviewDeliveryEventType.CLAIMED,
            worker_id="worker-a",
        )
        self.current_time = self.current_time + timedelta(seconds=1)
        self.store.append_review_delivery_event(
            review_id=review_id,
            workspace_id=review.workspace_id,
            event_type=ReviewDeliveryEventType.FAILED,
            worker_id="worker-a",
            error_code="RuntimeError",
            error_message="boom",
        )

        rendered = self.service.render_review_overview(42)

        self.assertIn("Последняя доставка", rendered)
        self.assertIn("Попыток доставки", rendered)
        self.assertIn("1", rendered)
        self.assertIn("Сбоев доставки", rendered)
        self.assertIn("failed (RuntimeError)", rendered)
        self.assertIn(f"/review_trace {review_id}", rendered)
        self.assertIn("Следующий шаг", rendered)
        self.assertIn("ретрай уже запланирован", rendered)

    def test_render_review_overview_shows_empty_delivery_status_when_trace_is_missing(self) -> None:
        review_id = self._seed_due_review()
        review = self.store.get_review(review_id)
        self.current_time = review.due_at + timedelta(hours=1)

        rendered = self.service.render_review_overview(42)

        self.assertIn("Последняя доставка", rendered)
        self.assertNotIn("Попыток доставки", rendered)
        self.assertIn("trace пока пустой", rendered)
        self.assertIn(f"/review_trace {review_id}", rendered)
        self.assertIn("Следующий шаг", rendered)
        self.assertIn("проверь trace", rendered)

    def test_render_review_overview_shows_delivery_attempt_summary(self) -> None:
        review_id = self._seed_due_review()
        review = self.store.get_review(review_id)
        self.store.append_review_delivery_event(
            review_id=review_id,
            workspace_id=review.workspace_id,
            event_type=ReviewDeliveryEventType.CLAIMED,
            worker_id="worker-a",
        )
        self.current_time = self.current_time + timedelta(seconds=1)
        self.store.append_review_delivery_event(
            review_id=review_id,
            workspace_id=review.workspace_id,
            event_type=ReviewDeliveryEventType.FAILED,
            worker_id="worker-a",
            error_code="RuntimeError",
            error_message="boom",
        )
        self.current_time = self.current_time + timedelta(seconds=1)
        self.store.append_review_delivery_event(
            review_id=review_id,
            workspace_id=review.workspace_id,
            event_type=ReviewDeliveryEventType.RELEASED,
            worker_id="worker-a",
        )
        self.current_time = self.current_time + timedelta(seconds=1)
        self.store.append_review_delivery_event(
            review_id=review_id,
            workspace_id=review.workspace_id,
            event_type=ReviewDeliveryEventType.CLAIMED,
            worker_id="worker-b",
        )
        self.current_time = self.current_time + timedelta(seconds=1)
        self.store.append_review_delivery_event(
            review_id=review_id,
            workspace_id=review.workspace_id,
            event_type=ReviewDeliveryEventType.DELIVERED,
            worker_id="worker-b",
        )

        rendered = self.service.render_review_overview(42)

        self.assertIn("Попыток доставки", rendered)
        self.assertIn("2", rendered)
        self.assertIn("Сбоев доставки", rendered)
        self.assertIn("1", rendered)

    def test_render_review_overview_escalates_stale_failed_delivery(self) -> None:
        review_id = self._seed_due_review()
        review = self.store.get_review(review_id)
        self.store.append_review_delivery_event(
            review_id=review_id,
            workspace_id=review.workspace_id,
            event_type=ReviewDeliveryEventType.CLAIMED,
            worker_id="worker-a",
        )
        self.current_time = self.current_time + timedelta(seconds=1)
        self.store.append_review_delivery_event(
            review_id=review_id,
            workspace_id=review.workspace_id,
            event_type=ReviewDeliveryEventType.FAILED,
            worker_id="worker-a",
            error_code="RuntimeError",
            error_message="boom",
        )
        self.current_time = self.current_time + timedelta(hours=7)

        rendered = self.service.render_review_overview(42)

        self.assertIn("Следующий шаг", rendered)
        self.assertIn("завис слишком долго", rendered)
        self.assertIn("проверь worker", rendered)

    def test_render_review_overview_escalates_empty_trace_when_review_is_stale(self) -> None:
        review_id = self._seed_due_review()
        self.current_time = self.current_time + timedelta(hours=7)

        rendered = self.service.render_review_overview(42)

        self.assertIn(f"/review_trace {review_id}", rendered)
        self.assertIn("Следующий шаг", rendered)
        self.assertIn("trace так и не появился", rendered)
        self.assertIn("scheduler", rendered)

    def test_render_review_overview_shows_compact_queue_for_additional_due_reviews(self) -> None:
        first_review_id = self._seed_due_review(title="Focus on onboarding")
        second_review_id = self._seed_due_review(
            title="Cut side experiments",
            chosen_option="Pause experiments",
            expected_outcome="fewer distractions",
            due_at=self.current_time - timedelta(hours=12),
        )
        third_review_id = self._seed_due_review(
            title="Talk to five users",
            chosen_option="Customer interviews",
            expected_outcome="clearer signals",
            due_at=self.current_time - timedelta(hours=6),
        )

        rendered = self.service.render_review_overview(42)

        self.assertIn(f"<code>{first_review_id}</code>", rendered)
        self.assertIn("<b>Ещё в очереди:</b>", rendered)
        self.assertIn(f"<code>{second_review_id}</code>", rendered)
        self.assertIn("Cut side experiments", rendered)
        self.assertIn(f"/review_trace {second_review_id}", rendered)
        self.assertIn(f"/review_done {second_review_id}", rendered)
        self.assertIn(f"<code>{third_review_id}</code>", rendered)
        self.assertIn("Talk to five users", rendered)

    def test_render_review_overview_omits_queue_section_when_only_one_due_review(self) -> None:
        self._seed_due_review()

        rendered = self.service.render_review_overview(42)

        self.assertNotIn("<b>Ещё в очереди:</b>", rendered)

    def test_render_review_overview_shows_remaining_hidden_count_when_due_queue_exceeds_preview(
        self,
    ) -> None:
        self._seed_due_review(title="Focus on onboarding")
        self._seed_due_review(title="Cut side experiments", due_at=self.current_time - timedelta(hours=12))
        self._seed_due_review(title="Talk to five users", due_at=self.current_time - timedelta(hours=6))
        self._seed_due_review(title="Narrow ICP", due_at=self.current_time - timedelta(hours=3))
        self._seed_due_review(title="Fix activation", due_at=self.current_time - timedelta(hours=2))

        rendered = self.service.render_review_overview(42)

        self.assertIn("<b>Ещё в очереди:</b>", rendered)
        self.assertIn("и ещё 1", rendered)
        self.assertIn("/review [limit]", rendered)

    def test_render_review_overview_keeps_primary_due_review_actions_intact_with_queue_present(
        self,
    ) -> None:
        first_review_id = self._seed_due_review(title="Focus on onboarding")
        self._seed_due_review(title="Cut side experiments", due_at=self.current_time - timedelta(hours=12))

        rendered = self.service.render_review_overview(42)

        self.assertIn(f"/review_done {first_review_id} что получилось", rendered)
        self.assertIn(f"/review_skip {first_review_id}", rendered)

    def test_render_review_overview_respects_due_review_limit(self) -> None:
        first_review_id = self._seed_due_review(title="Focus on onboarding")
        second_review_id = self._seed_due_review(
            title="Cut side experiments",
            due_at=self.current_time - timedelta(hours=12),
        )
        third_review_id = self._seed_due_review(
            title="Talk to five users",
            due_at=self.current_time - timedelta(hours=6),
        )

        rendered = self.service.render_review_overview(42, limit=2)

        self.assertIn(f"<code>{first_review_id}</code>", rendered)
        self.assertIn(f"<code>{second_review_id}</code>", rendered)
        self.assertIn("Cut side experiments", rendered)
        self.assertIn(f"<code>{third_review_id}</code>", rendered)
        self.assertIn("Talk to five users", rendered)
        self.assertNotIn("и ещё", rendered)

    def test_render_review_overview_respects_queue_preview_limit(self) -> None:
        self._seed_due_review(title="Focus on onboarding")
        second_review_id = self._seed_due_review(
            title="Cut side experiments",
            due_at=self.current_time - timedelta(hours=12),
        )
        third_review_id = self._seed_due_review(
            title="Talk to five users",
            due_at=self.current_time - timedelta(hours=6),
        )
        fourth_review_id = self._seed_due_review(
            title="Narrow ICP",
            due_at=self.current_time - timedelta(hours=3),
        )

        rendered = self.service.render_review_overview(42, limit=1)

        self.assertIn(f"<code>{second_review_id}</code>", rendered)
        self.assertNotIn(f"<code>{third_review_id}</code>", rendered)
        self.assertNotIn(f"<code>{fourth_review_id}</code>", rendered)
        self.assertIn("и ещё 2", rendered)
        self.assertIn("/review [limit]", rendered)

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

    def test_render_review_trace_shows_delivery_event_history(self) -> None:
        review_id = self._seed_due_review()
        review = self.store.get_review(review_id)
        self.store.append_review_delivery_event(
            review_id=review_id,
            workspace_id=review.workspace_id,
            event_type=ReviewDeliveryEventType.CLAIMED,
            worker_id="worker-a",
            metadata={"claim_expires_at": self.current_time.isoformat()},
        )
        self.current_time = self.current_time + timedelta(seconds=1)
        self.store.append_review_delivery_event(
            review_id=review_id,
            workspace_id=review.workspace_id,
            event_type=ReviewDeliveryEventType.FAILED,
            worker_id="worker-a",
            error_code="RuntimeError",
            error_message="boom",
            metadata={"chat_id": 42},
        )
        self.current_time = self.current_time + timedelta(seconds=1)
        self.store.append_review_delivery_event(
            review_id=review_id,
            workspace_id=review.workspace_id,
            event_type=ReviewDeliveryEventType.RELEASED,
            worker_id="worker-a",
            metadata={"reason": "send_failed"},
        )

        rendered = self.service.render_review_trace(42, review_id)

        self.assertIn("Delivery Trace", rendered)
        self.assertIn(f"<code>{review_id}</code>", rendered)
        self.assertIn("claimed", rendered)
        self.assertIn("failed", rendered)
        self.assertIn("released", rendered)
        self.assertIn("RuntimeError", rendered)
        self.assertIn("worker-a", rendered)

    def test_render_review_trace_rejects_foreign_review(self) -> None:
        review_id = self._seed_due_review("workspace-1")

        with self.assertRaises(ReviewServiceError):
            self.service.render_review_trace(42, review_id)

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
