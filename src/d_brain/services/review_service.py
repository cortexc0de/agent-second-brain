"""Deterministic review-loop service for Founder Decision Partner V1."""

from __future__ import annotations

import html
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from d_brain.services.decision_models import DecisionOutcomeStatus, ReviewRecord, ReviewStatus
from d_brain.services.decision_store import DecisionStore, DecisionStoreError
from d_brain.services.review_outcome_analyzer import (
    ReviewOutcomeStatus,
    analyze_review_outcome,
)


class ReviewServiceError(RuntimeError):
    """Raised when a review operation is invalid."""


class ReviewService:
    """Service for listing and closing review checkpoints."""

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
            raise ReviewServiceError("Review store is not configured")
        return DecisionStore(self.store_path), True

    @staticmethod
    def _ensure_owner(review: ReviewRecord, user_id: int) -> None:
        if review.workspace_id != str(user_id):
            raise ReviewServiceError("Review does not belong to this user")

    @staticmethod
    def _ensure_open(review: ReviewRecord) -> None:
        if review.status in {ReviewStatus.COMPLETED, ReviewStatus.SKIPPED}:
            raise ReviewServiceError(f"Review {review.id} is already closed")

    @staticmethod
    def _map_outcome_status(status: ReviewOutcomeStatus) -> DecisionOutcomeStatus:
        if status is ReviewOutcomeStatus.CONFIRMED:
            return DecisionOutcomeStatus.CONFIRMED
        if status is ReviewOutcomeStatus.INVALIDATED:
            return DecisionOutcomeStatus.INVALIDATED
        return DecisionOutcomeStatus.MIXED

    def list_due_reviews(self, user_id: int, limit: int = 5) -> list[ReviewRecord]:
        """List due reviews for a user and mark scheduled ones as due."""
        store, created_store = self._open_store()
        try:
            reviews = store.list_reviews(str(user_id))
            due_reviews: list[ReviewRecord] = []
            for review in reviews:
                if review.status in {ReviewStatus.SCHEDULED, ReviewStatus.DUE} and review.due_at <= self._now():
                    if review.status == ReviewStatus.SCHEDULED:
                        review = store.update_review(review.id, ReviewStatus.DUE)
                    due_reviews.append(review)
            return due_reviews[:limit]
        finally:
            if created_store and isinstance(store, DecisionStore):
                store.close()

    def render_review_overview(self, user_id: int) -> str:
        """Render an overview of due reviews and recent review history."""
        store, created_store = self._open_store()
        try:
            due_reviews = self.list_due_reviews(user_id)
            recent_reviews = store.list_reviews(str(user_id), limit=5)

            if due_reviews:
                first = due_reviews[0]
                record = store.get_record(first.decision_record_id)
                parts = [
                    "🔁 <b>Пора проверить решение</b>",
                    "",
                    f"<b>ID:</b> <code>{first.id}</code>",
                    f"<b>Решение:</b> {html.escape(record.title)}",
                    f"<b>Что выбрали:</b> {html.escape(record.chosen_option)}",
                    f"<b>Что ожидали:</b> {html.escape(first.expected_outcome)}",
                    f"<b>Дедлайн ревью:</b> {html.escape(first.due_at.date().isoformat())}",
                    "",
                    f"<b>Завершить:</b> <code>/review_done {first.id} что получилось</code>",
                    f"<b>Пропустить:</b> <code>/review_skip {first.id}</code>",
                ]
                if len(due_reviews) > 1:
                    parts.extend(
                        [
                            "",
                            f"<i>Ещё due reviews: {len(due_reviews) - 1}</i>",
                        ]
                    )
                return "\n".join(parts)

            parts = ["✅ <b>Сейчас нет due reviews</b>"]
            if recent_reviews:
                parts.extend(["", "<b>Последние review:</b>"])
                for review in recent_reviews[:3]:
                    status = review.status.value
                    record = store.get_record(review.decision_record_id)
                    parts.append(
                        f"• <code>{review.id}</code> {html.escape(record.title)} — {html.escape(status)}"
                    )
            else:
                parts.extend(["", "<i>Когда появятся решения с review date, они будут видны здесь.</i>"])
            return "\n".join(parts)
        finally:
            if created_store and isinstance(store, DecisionStore):
                store.close()

    def complete_review(self, user_id: int, review_id: int, outcome: str) -> str:
        """Mark a review as completed and return a confirmation message."""
        if not outcome.strip():
            raise ReviewServiceError("Outcome text is required")

        store, created_store = self._open_store()
        try:
            review = store.get_review(review_id)
            self._ensure_owner(review, user_id)
            self._ensure_open(review)
            record = store.get_record(review.decision_record_id)
            assessment = analyze_review_outcome(review.expected_outcome, outcome.strip())
            updated = store.update_review(
                review_id,
                ReviewStatus.COMPLETED,
                user_response=outcome.strip(),
                actual_outcome=outcome.strip(),
                agent_assessment=assessment.assessment,
            )
            store.update_record_outcome(
                record.id,
                outcome_status=self._map_outcome_status(assessment.status),
                outcome_summary=outcome.strip(),
                needs_follow_up=assessment.needs_follow_up,
            )
            follow_up_line = "Да" if assessment.needs_follow_up else "Нет"
            return (
                "✅ <b>Review закрыт</b>\n\n"
                f"<b>ID:</b> <code>{updated.id}</code>\n"
                f"<b>Решение:</b> {html.escape(record.title)}\n"
                f"<b>Статус:</b> {html.escape(assessment.status.value)}\n"
                f"<b>Нужен follow-up:</b> {follow_up_line}\n"
                f"<b>Итог:</b> {html.escape(outcome.strip())}"
            )
        except DecisionStoreError as exc:
            raise ReviewServiceError(str(exc)) from exc
        finally:
            if created_store and isinstance(store, DecisionStore):
                store.close()

    def skip_review(self, user_id: int, review_id: int) -> str:
        """Skip a review checkpoint."""
        store, created_store = self._open_store()
        try:
            review = store.get_review(review_id)
            self._ensure_owner(review, user_id)
            self._ensure_open(review)
            record = store.get_record(review.decision_record_id)
            updated = store.update_review(
                review_id,
                ReviewStatus.SKIPPED,
                agent_assessment="Review skipped via Telegram command.",
            )
            return (
                "⏭️ <b>Review пропущен</b>\n\n"
                f"<b>ID:</b> <code>{updated.id}</code>\n"
                f"<b>Решение:</b> {html.escape(record.title)}"
            )
        except DecisionStoreError as exc:
            raise ReviewServiceError(str(exc)) from exc
        finally:
            if created_store and isinstance(store, DecisionStore):
                store.close()
