"""Deterministic worker-facing service for due review prompts."""

from __future__ import annotations

import html
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from d_brain.services.decision_models import ReviewStatus
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
    """Collect due reviews and render ready-to-send prompts exactly once."""

    def __init__(
        self,
        *,
        store: Any | None = None,
        store_path: str | Path | None = None,
        clock: callable | None = None,
    ) -> None:
        self.store = store
        self.store_path = store_path
        self._clock = clock or (lambda: datetime.now(timezone.utc))

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

    def collect_due_prompts(self, limit: int = 20) -> list[DueReviewPrompt]:
        """Collect scheduled due reviews, mark them due, and render one-off prompts."""
        store, created_store = self._open_store()
        try:
            reviews = store.list_due_reviews(self._now())[:limit]
            prompts: list[DueReviewPrompt] = []
            for review in reviews:
                updated = store.update_review(review.id, ReviewStatus.DUE)
                record = store.get_record(updated.decision_record_id)
                prompts.append(
                    DueReviewPrompt(
                        workspace_id=updated.workspace_id,
                        user_id=int(updated.workspace_id),
                        review_id=updated.id,
                        decision_record_id=record.id,
                        message=self._render_prompt(updated.id, record.title, record.chosen_option, updated.expected_outcome),
                    )
                )
            return prompts
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
