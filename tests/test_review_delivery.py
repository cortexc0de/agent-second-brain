from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from d_brain.bot import review_delivery


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
        logger.exception.assert_called_once()

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


if __name__ == "__main__":
    unittest.main()
