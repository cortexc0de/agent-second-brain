"""Review loop service for scheduled decision follow-ups."""

from __future__ import annotations

from datetime import datetime
from typing import Callable

from d_brain.services.decision_models import DecisionRecord, ReviewRecord, ReviewStatus
from d_brain.services.decision_store import DecisionStore

Clock = Callable[[], datetime]


class ReviewService:
    """Deterministic service for review due lists and completion flows."""

    def __init__(self, store: DecisionStore, clock: Clock | None = None) -> None:
        self.store = store
        self._clock = clock

    def _now(self) -> datetime:
        if self._clock is not None:
            return self._clock()
        return datetime.now()

    def list_due_reviews(
        self,
        workspace_id: str | int | None = None,
        *,
        when: datetime | None = None,
        limit: int | None = None,
    ) -> list[ReviewRecord]:
        """List scheduled reviews that are due now or before a threshold."""
        threshold = when or self._now()
        reviews = self.store.list_due_reviews(threshold)
        if workspace_id is not None:
            workspace_key = str(workspace_id)
            reviews = [review for review in reviews if review.workspace_id == workspace_key]
        if limit is not None:
            reviews = reviews[:limit]
        return reviews

    def format_review_prompt(self, review: ReviewRecord) -> str:
        """Format a deterministic review prompt for Telegram or chat output."""
        decision = self.store.get_record(review.decision_record_id)
        return _format_review_prompt(review, decision)

    def mark_completed(
        self,
        review_id: int,
        *,
        actual_outcome: str | None = None,
        user_response: str | None = None,
        agent_assessment: str | None = None,
    ) -> ReviewRecord:
        """Mark a review as completed."""
        return self.store.update_review(
            review_id,
            ReviewStatus.COMPLETED,
            actual_outcome=actual_outcome,
            user_response=user_response,
            agent_assessment=agent_assessment,
        )

    def mark_skipped(self, review_id: int, reason: str | None = None) -> ReviewRecord:
        """Mark a review as skipped."""
        return self.store.update_review(
            review_id,
            ReviewStatus.SKIPPED,
            agent_assessment=reason,
        )


def _format_review_prompt(review: ReviewRecord, decision: DecisionRecord) -> str:
    """Create a compact prompt that asks whether the prior recommendation held."""
    expected_signals = decision.expected_signals or [review.expected_outcome]

    lines = [
        "🧭 Review check-in",
        "",
        f"Decision: {decision.title}",
        f"Chosen option: {decision.chosen_option}",
        f"Decision summary: {decision.decision_summary}",
        "",
        "Expected signals:",
    ]
    lines.extend(f"- {signal}" for signal in expected_signals)
    lines.extend(
        [
            "",
            f"Review due: {review.due_at.isoformat()}",
            f"Expected outcome: {review.expected_outcome}",
            "",
            "Question: Did the expected signals appear? If not, what happened instead?",
        ]
    )
    return "\n".join(lines)
