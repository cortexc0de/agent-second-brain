"""Deterministic review-loop service for Founder Decision Partner V1."""

from __future__ import annotations

import html
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from d_brain.services.decision_models import DecisionOutcomeStatus, ReviewRecord, ReviewStatus
from d_brain.services.decision_store import DecisionStore, DecisionStoreError
from d_brain.services.review_pattern_feedback import build_pattern_feedback
from d_brain.services.review_outcome_analyzer import (
    ReviewOutcomeStatus,
    analyze_review_outcome,
)

logger = logging.getLogger(__name__)


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

    @staticmethod
    def _render_latest_delivery_status(review_id: int, latest_event: Any | None) -> list[str]:
        if latest_event is None:
            return [
                "<b>Последняя доставка:</b> trace пока пустой",
                f"<b>Trace целиком:</b> <code>/review_trace {review_id}</code>",
            ]

        status = latest_event.event_type.value
        if latest_event.event_type.value == "failed" and latest_event.error_code:
            status = f"{status} ({latest_event.error_code})"
        elif latest_event.event_type.value == "claimed" and latest_event.worker_id:
            status = f"{status} by {latest_event.worker_id}"

        return [
            f"<b>Последняя доставка:</b> {html.escape(status)}",
            f"<b>Trace целиком:</b> <code>/review_trace {review_id}</code>",
        ]

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
                latest_events = store.list_review_delivery_events(first.id, limit=10)
                latest_event = latest_events[-1] if latest_events else None
                parts = [
                    "🔁 <b>Пора проверить решение</b>",
                    "",
                    f"<b>ID:</b> <code>{first.id}</code>",
                    f"<b>Решение:</b> {html.escape(record.title)}",
                    f"<b>Что выбрали:</b> {html.escape(record.chosen_option)}",
                    f"<b>Что ожидали:</b> {html.escape(first.expected_outcome)}",
                    f"<b>Дедлайн ревью:</b> {html.escape(first.due_at.date().isoformat())}",
                    *self._render_latest_delivery_status(first.id, latest_event),
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

    def render_review_trace(self, user_id: int, review_id: int, limit: int = 10) -> str:
        """Render proactive delivery trace for a specific review."""
        store, created_store = self._open_store()
        try:
            review = store.get_review(review_id)
            self._ensure_owner(review, user_id)
            record = store.get_record(review.decision_record_id)
            events = store.list_review_delivery_events(review_id, limit=limit)

            parts = [
                "🔎 <b>Delivery Trace</b>",
                "",
                f"<b>ID:</b> <code>{review.id}</code>",
                f"<b>Решение:</b> {html.escape(record.title)}",
                f"<b>Статус review:</b> {html.escape(review.status.value)}",
            ]
            if review.notified_at is not None:
                parts.append(f"<b>Notified at:</b> {html.escape(review.notified_at.isoformat())}")
            if review.claimed_by and review.claim_expires_at is not None:
                parts.append(
                    f"<b>Активный claim:</b> {html.escape(review.claimed_by)} до "
                    f"{html.escape(review.claim_expires_at.isoformat())}"
                )

            parts.extend(["", "<b>Timeline:</b>"])
            if not events:
                parts.append("<i>Trace пока пустой.</i>")
                return "\n".join(parts)

            for event in events:
                line = [
                    html.escape(event.created_at.isoformat()),
                    html.escape(event.event_type.value),
                ]
                if event.worker_id:
                    line.append(f"worker={html.escape(event.worker_id)}")
                if event.error_code:
                    line.append(html.escape(event.error_code))
                if event.error_message:
                    line.append(html.escape(event.error_message))
                if event.metadata:
                    metadata = ", ".join(
                        f"{html.escape(str(key))}={html.escape(str(value))}"
                        for key, value in sorted(event.metadata.items())
                    )
                    if metadata:
                        line.append(metadata)
                parts.append(f"• {' — '.join(line)}")
            return "\n".join(parts)
        except DecisionStoreError as exc:
            raise ReviewServiceError(str(exc)) from exc
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
            outcome_status = self._map_outcome_status(assessment.status)
            if record.linked_pattern_names:
                existing_patterns = {
                    pattern.name: pattern
                    for pattern in store.list_patterns(str(user_id), limit=50)
                }
                for pattern_name in record.linked_pattern_names:
                    pattern = existing_patterns.get(pattern_name)
                    if pattern is None:
                        continue
                    feedback = build_pattern_feedback(
                        pattern,
                        outcome_status,
                        review_id=updated.id,
                        outcome_summary=outcome.strip(),
                    )
                    try:
                        store.persist_pattern(
                            user_id,
                            name=pattern.name,
                            category=feedback.category,
                            description=feedback.description,
                            evidence=feedback.evidence,
                            confidence=feedback.confidence,
                            status=feedback.status,
                            last_seen_at=self._now(),
                        )
                    except Exception:
                        logger.warning("Pattern feedback persistence failed", exc_info=True)
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
