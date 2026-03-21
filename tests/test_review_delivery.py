from __future__ import annotations

import asyncio
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from d_brain.bot import review_delivery
from d_brain.services.decision_store import DecisionStore
from d_brain.services.due_review_worker import DueReviewWorker


class ReviewDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_deliver_due_reviews_sends_all_prompts_and_returns_count(self) -> None:
        worker = MagicMock()
        worker.collect_due_prompts.return_value = [
            SimpleNamespace(review_id=1, user_id=42, message="first"),
            SimpleNamespace(review_id=2, user_id=84, message="second"),
        ]
        bot = SimpleNamespace(send_message=AsyncMock())

        sent_count = await review_delivery.deliver_due_reviews(bot, worker=worker, batch_limit=7)

        self.assertEqual(sent_count, 2)
        worker.collect_due_prompts.assert_called_once_with(limit=7)
        worker.acknowledge_prompt_delivery.assert_has_calls(
            [unittest.mock.call(1), unittest.mock.call(2)]
        )
        bot.send_message.assert_has_awaits(
            [
                unittest.mock.call(chat_id=42, text="first"),
                unittest.mock.call(chat_id=84, text="second"),
            ]
        )

    async def test_deliver_due_reviews_continues_after_send_failure(self) -> None:
        worker = MagicMock()
        worker.collect_due_prompts.return_value = [
            SimpleNamespace(review_id=1, user_id=42, message="first"),
            SimpleNamespace(review_id=2, user_id=84, message="second"),
        ]
        bot = SimpleNamespace(
            send_message=AsyncMock(side_effect=[RuntimeError("boom"), None]),
        )

        with patch.object(review_delivery, "logger") as logger:
            sent_count = await review_delivery.deliver_due_reviews(bot, worker=worker, batch_limit=20)

        self.assertEqual(sent_count, 1)
        self.assertEqual(bot.send_message.await_count, 2)
        worker.acknowledge_prompt_delivery.assert_called_once_with(2)
        logger.exception.assert_called_once()

    async def test_deliver_due_reviews_acknowledges_only_successful_prompts_in_partial_failure_batch(self) -> None:
        worker = MagicMock()
        worker.collect_due_prompts.return_value = [
            SimpleNamespace(review_id=10, user_id=42, message="first"),
            SimpleNamespace(review_id=11, user_id=84, message="second"),
            SimpleNamespace(review_id=12, user_id=126, message="third"),
        ]
        bot = SimpleNamespace(
            send_message=AsyncMock(side_effect=[None, RuntimeError("boom"), None]),
        )

        with patch.object(review_delivery, "logger"):
            sent_count = await review_delivery.deliver_due_reviews(bot, worker=worker, batch_limit=20)

        self.assertEqual(sent_count, 2)
        worker.acknowledge_prompt_delivery.assert_has_calls(
            [unittest.mock.call(10), unittest.mock.call(12)]
        )
        self.assertEqual(worker.acknowledge_prompt_delivery.call_count, 2)

    async def test_run_due_review_delivery_loop_polls_until_cancelled(self) -> None:
        worker = MagicMock()
        bot = SimpleNamespace()
        delivered_batches: list[int] = []
        expected_worker = worker

        async def fake_deliver_due_reviews(bot_arg, *, worker, batch_limit: int) -> int:
            self.assertIs(bot_arg, bot)
            self.assertIs(worker, expected_worker)
            delivered_batches.append(batch_limit)
            return 1

        async def fake_sleep(seconds: float) -> None:
            self.assertEqual(seconds, 15)
            raise asyncio.CancelledError

        with patch.object(review_delivery, "deliver_due_reviews", new=fake_deliver_due_reviews):
            with self.assertRaises(asyncio.CancelledError):
                await review_delivery.run_due_review_delivery_loop(
                    bot,
                    worker=worker,
                    poll_interval_seconds=15,
                    batch_limit=3,
                    sleep=fake_sleep,
                )

        self.assertEqual(delivered_batches, [3])

    async def test_deliver_due_reviews_retries_failed_prompt_then_stops_after_ack(self) -> None:
        current_time = datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc)

        def clock() -> datetime:
            return current_time

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decision-store.sqlite3"
            store = DecisionStore(db_path, clock=clock)
            try:
                run = store.persist_run("42", "What should I focus on next?")
                record = store.persist_decision(
                    "42",
                    decision_run_id=run.id,
                    title="Focus on onboarding",
                    decision_summary="Freeze side quests.",
                    chosen_option="Onboarding",
                    rejected_options=["New feature"],
                    why="Strongest signal.",
                    risks="Sample too small.",
                    expected_signals=["more activations"],
                )
                store.create_review(
                    workspace_id="42",
                    decision_record_id=record.id,
                    due_at=current_time - timedelta(days=1),
                    expected_outcome="more activations",
                )
                worker = DueReviewWorker(store=store, clock=clock)
                bot = SimpleNamespace(
                    send_message=AsyncMock(side_effect=[RuntimeError("boom"), None]),
                )

                with patch.object(review_delivery, "logger"):
                    first_sent = await review_delivery.deliver_due_reviews(bot, worker=worker)
                    second_sent = await review_delivery.deliver_due_reviews(bot, worker=worker)
                    third_sent = await review_delivery.deliver_due_reviews(bot, worker=worker)

                self.assertEqual(first_sent, 0)
                self.assertEqual(second_sent, 1)
                self.assertEqual(third_sent, 0)
                self.assertEqual(bot.send_message.await_count, 2)
                review = store.get_review(1)
                self.assertEqual(review.status.value, "due")
                self.assertEqual(review.notified_at, current_time)
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
