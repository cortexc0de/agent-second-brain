"""Deterministic worker-facing service for due review prompts."""

from __future__ import annotations

import html
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from d_brain.services.decision_models import ReviewDeliveryEventType
from d_brain.services.decision_store import DecisionStore


@dataclass(slots=True)
class DueReviewPrompt:
    """Prepared due-review prompt for downstream delivery."""

    workspace_id: str
    user_id: int
    review_id: int
    decision_record_id: int
    message: str


class DueReviewWorker:
    """Collect due reviews and acknowledge proactive delivery explicitly."""

    def __init__(
        self,
        *,
        store: Any | None = None,
        store_path: str | Path | None = None,
        clock: callable | None = None,
        worker_id: str | None = None,
        lease_seconds: int = 300,
    ) -> None:
        self.store = store
        self.store_path = store_path
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.worker_id = worker_id or uuid.uuid4().hex
        self.lease_seconds = lease_seconds

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    def _open_store(self) -> tuple[Any, bool]:
        if self.store is not None:
            return self.store, False
        if self.store_path is None:
            raise RuntimeError("Review store is not configured")
        return DecisionStore(self.store_path), True

    def _lease_expires_at(self) -> datetime:
        return self._now() + timedelta(seconds=self.lease_seconds)

    def collect_due_prompts(self, limit: int = 20) -> list[DueReviewPrompt]:
        """Collect due reviews that still need proactive delivery."""
        store, created_store = self._open_store()
        try:
            reviews = store.claim_due_review_notifications(
                claimer_id=self.worker_id,
                when=self._now(),
                lease_expires_at=self._lease_expires_at(),
                limit=limit,
            )
            prompts: list[DueReviewPrompt] = []
            for review in reviews:
                record = store.get_record(review.decision_record_id)
                prompts.append(
                    DueReviewPrompt(
                        workspace_id=review.workspace_id,
                        user_id=int(review.workspace_id),
                        review_id=review.id,
                        decision_record_id=record.id,
                        message=self._render_prompt(review.id, record.title, record.chosen_option, review.expected_outcome),
                    )
                )
            return prompts
        finally:
            if created_store and isinstance(store, DecisionStore):
                store.close()

    def acknowledge_prompt_delivery(self, review_id: int) -> None:
        """Persist successful proactive delivery for a review prompt."""
        store, created_store = self._open_store()
        try:
            with store.transaction():
                updated = store.mark_review_notified(review_id, notified_at=self._now(), claimer_id=self.worker_id)
                store.append_review_delivery_event(
                    review_id=updated.id,
                    workspace_id=updated.workspace_id,
                    event_type=ReviewDeliveryEventType.DELIVERED,
                    worker_id=self.worker_id,
                )
        finally:
            if created_store and isinstance(store, DecisionStore):
                store.close()

    def record_failed_prompt_delivery(
        self,
        review_id: int,
        *,
        chat_id: int | None = None,
        error: Exception,
    ) -> None:
        """Persist a failed proactive delivery attempt."""
        store, created_store = self._open_store()
        try:
            review = store.get_review(review_id)
            metadata: dict[str, object] = {}
            if chat_id is not None:
                metadata["chat_id"] = chat_id
            store.append_review_delivery_event(
                review_id=review.id,
                workspace_id=review.workspace_id,
                event_type=ReviewDeliveryEventType.FAILED,
                worker_id=self.worker_id,
                error_code=error.__class__.__name__,
                error_message=str(error),
                metadata=metadata,
            )
        finally:
            if created_store and isinstance(store, DecisionStore):
                store.close()

    def release_prompt_delivery(self, review_id: int, *, reason: str = "send_failed") -> None:
        """Release a failed proactive delivery claim for retry."""
        store, created_store = self._open_store()
        try:
            with store.transaction():
                released = store.release_review_claim(review_id, claimer_id=self.worker_id)
                store.append_review_delivery_event(
                    review_id=released.id,
                    workspace_id=released.workspace_id,
                    event_type=ReviewDeliveryEventType.RELEASED,
                    worker_id=self.worker_id,
                    metadata={"reason": reason},
                )
        finally:
            if created_store and isinstance(store, DecisionStore):
                store.close()

    @staticmethod
    def _render_prompt(review_id: int, title: str, chosen_option: str, expected_outcome: str) -> str:
        return "\n".join(
            [
                "🔁 <b>Пора проверить решение</b>",
                "",
                f"<b>ID:</b> <code>{review_id}</code>",
                f"<b>Решение:</b> {html.escape(title)}",
                f"<b>Что выбрали:</b> {html.escape(chosen_option)}",
                f"<b>Что ожидали:</b> {html.escape(expected_outcome)}",
                "",
                f"<b>Завершить:</b> <code>/review_done {review_id} что получилось</code>",
                f"<b>Пропустить:</b> <code>/review_skip {review_id}</code>",
            ]
        )
