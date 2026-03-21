from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from d_brain.services.decision_store import DecisionStore
from d_brain.services.decision_models import ReviewDeliveryEventType
from d_brain.services.due_review_worker import DueReviewWorker


class DueReviewWorkerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "decision-store.sqlite3"
        self.current_time = datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc)
        self.store = DecisionStore(self.db_path, clock=self.clock)
        self.worker = DueReviewWorker(store=self.store, clock=self.clock, worker_id="worker-a", lease_seconds=300)

    def tearDown(self) -> None:
        self.store.close()
        self.tmpdir.cleanup()

    def clock(self) -> datetime:
        return self.current_time

    def _seed_review(self, workspace_id: str, *, due_at: datetime) -> int:
        run = self.store.persist_run(workspace_id, "What should I focus on?")
        record = self.store.persist_decision(
            workspace_id,
            decision_run_id=run.id,
            title=f"Focus for {workspace_id}",
            decision_summary="Freeze experiments and focus on onboarding.",
            chosen_option="Onboarding",
            rejected_options=["New feature"],
            why="Highest evidence.",
            risks="Sample too small.",
            expected_signals=["more activations"],
        )
        review = self.store.create_review(
            workspace_id=workspace_id,
            decision_record_id=record.id,
            due_at=due_at,
            expected_outcome="more activations",
        )
        return review.id

    def test_collect_due_prompts_returns_sorted_due_reviews_without_acknowledging_them(self) -> None:
        first_review_id = self._seed_review("42", due_at=self.current_time - timedelta(days=2))
        second_review_id = self._seed_review("84", due_at=self.current_time - timedelta(days=1))
        self._seed_review("168", due_at=self.current_time + timedelta(days=1))

        prompts = self.worker.collect_due_prompts()

        self.assertEqual([prompt.review_id for prompt in prompts], [first_review_id, second_review_id])
        self.assertEqual([prompt.workspace_id for prompt in prompts], ["42", "84"])
        self.assertIn("/review_done", prompts[0].message)
        self.assertIn("/review_skip", prompts[0].message)

        self.assertEqual(self.store.get_review(first_review_id).status.value, "scheduled")
        self.assertEqual(self.store.get_review(second_review_id).status.value, "scheduled")
        self.assertIsNone(self.store.get_review(first_review_id).notified_at)

    def test_collect_due_prompts_keeps_claimed_review_hidden_until_release_or_expiry(self) -> None:
        review_id = self._seed_review("42", due_at=self.current_time - timedelta(days=1))

        first_batch = self.worker.collect_due_prompts()
        second_batch = self.worker.collect_due_prompts()

        self.assertEqual(len(first_batch), 1)
        self.assertEqual(second_batch, [])
        self.assertEqual(first_batch[0].review_id, review_id)
        loaded = self.store.get_review(review_id)
        self.assertEqual(loaded.claimed_by, "worker-a")

    def test_release_prompt_delivery_claim_makes_review_retryable_immediately(self) -> None:
        review_id = self._seed_review("42", due_at=self.current_time - timedelta(days=1))

        first_batch = self.worker.collect_due_prompts()
        self.worker.release_prompt_delivery(review_id)
        second_batch = self.worker.collect_due_prompts()

        self.assertEqual(len(first_batch), 1)
        self.assertEqual(len(second_batch), 1)
        self.assertEqual(second_batch[0].review_id, review_id)

    def test_collect_due_prompts_skips_already_acknowledged_review(self) -> None:
        review_id = self._seed_review("42", due_at=self.current_time - timedelta(days=1))

        prompts = self.worker.collect_due_prompts()
        self.worker.acknowledge_prompt_delivery(review_id)
        next_batch = self.worker.collect_due_prompts()

        self.assertEqual(len(prompts), 1)
        self.assertEqual(next_batch, [])
        self.assertEqual(self.store.get_review(review_id).status.value, "due")
        self.assertEqual(self.store.get_review(review_id).notified_at, self.current_time)

    def test_collect_due_prompts_claims_reviews_for_this_worker(self) -> None:
        review_id = self._seed_review("42", due_at=self.current_time - timedelta(days=1))

        prompts = self.worker.collect_due_prompts()

        self.assertEqual(len(prompts), 1)
        loaded = self.store.get_review(review_id)
        self.assertEqual(loaded.claimed_by, "worker-a")
        self.assertEqual(loaded.claim_expires_at, self.current_time + timedelta(minutes=5))
        events = self.store.list_review_delivery_events(review_id)
        self.assertEqual([event.event_type for event in events], [ReviewDeliveryEventType.CLAIMED])

    def test_collect_due_prompts_does_not_create_duplicate_claim_trace_for_already_claimed_review(self) -> None:
        review_id = self._seed_review("42", due_at=self.current_time - timedelta(days=1))

        self.worker.collect_due_prompts()
        self.worker.collect_due_prompts()

        events = self.store.list_review_delivery_events(review_id)
        self.assertEqual([event.event_type for event in events], [ReviewDeliveryEventType.CLAIMED])

    def test_collect_due_prompts_does_not_pick_review_claimed_by_other_worker_until_lease_expires(self) -> None:
        review_id = self._seed_review("42", due_at=self.current_time - timedelta(days=1))
        other_worker = DueReviewWorker(store=self.store, clock=self.clock, worker_id="worker-b", lease_seconds=300)

        first_batch = self.worker.collect_due_prompts()
        second_batch = other_worker.collect_due_prompts()
        self.current_time = self.current_time + timedelta(minutes=6)
        third_batch = other_worker.collect_due_prompts()

        self.assertEqual([item.review_id for item in first_batch], [review_id])
        self.assertEqual(second_batch, [])
        self.assertEqual([item.review_id for item in third_batch], [review_id])

    def test_collect_due_prompts_respects_limit(self) -> None:
        self._seed_review("42", due_at=self.current_time - timedelta(days=3))
        self._seed_review("84", due_at=self.current_time - timedelta(days=2))
        self._seed_review("126", due_at=self.current_time - timedelta(days=1))

        prompts = self.worker.collect_due_prompts(limit=2)

        self.assertEqual(len(prompts), 2)


if __name__ == "__main__":
    unittest.main()
