"""Transport-level delivery loop for proactive due-review prompts."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from d_brain.services.due_review_worker import DueReviewWorker

logger = logging.getLogger(__name__)


class MessageBot(Protocol):
    """Minimal async bot contract used by due-review delivery."""

    async def send_message(self, *, chat_id: int, text: str) -> Any:
        """Send a formatted message to a Telegram chat."""


async def deliver_due_reviews(
    bot: MessageBot,
    *,
    worker: DueReviewWorker,
    batch_limit: int = 20,
) -> int:
    """Deliver all currently due review prompts and return success count."""
    sent_count = 0
    prompts = worker.collect_due_prompts(limit=batch_limit)
    for prompt in prompts:
        try:
            await bot.send_message(chat_id=prompt.user_id, text=prompt.message)
            worker.acknowledge_prompt_delivery(prompt.review_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            try:
                worker.record_failed_prompt_delivery(prompt.review_id, chat_id=prompt.user_id, error=exc)
            except Exception:
                logger.exception("Due review failure trace persistence failed for review %s", prompt.review_id)
            try:
                worker.release_prompt_delivery(prompt.review_id, reason="send_failed")
            except Exception:
                logger.exception("Due review claim release failed for review %s", prompt.review_id)
            logger.exception("Due review delivery failed for review %s", prompt.review_id)
        else:
            sent_count += 1
    return sent_count


async def run_due_review_delivery_loop(
    bot: MessageBot,
    *,
    worker: DueReviewWorker,
    poll_interval_seconds: float = 60.0,
    batch_limit: int = 20,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """Poll for due reviews forever and push them through the bot transport."""
    while True:
        try:
            await deliver_due_reviews(bot, worker=worker, batch_limit=batch_limit)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Due review polling failed")
        await sleep(poll_interval_seconds)
