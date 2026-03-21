"""Deterministic review-pattern feedback for decision patterns."""

from __future__ import annotations

from dataclasses import dataclass

from d_brain.services.decision_models import DecisionOutcomeStatus, PatternRecord, PatternStatus


@dataclass(slots=True)
class PatternFeedbackUpdate:
    """Updated pattern fields derived from a review outcome."""

    category: str
    description: str
    evidence: list[str]
    confidence: float
    status: PatternStatus


def build_pattern_feedback(
    pattern: PatternRecord,
    outcome_status: DecisionOutcomeStatus,
    *,
    review_id: int,
    outcome_summary: str,
) -> PatternFeedbackUpdate:
    """Translate a review outcome into deterministic pattern-memory updates."""
    summary = outcome_summary.strip()

    if outcome_status is DecisionOutcomeStatus.CONFIRMED:
        return PatternFeedbackUpdate(
            category=pattern.category,
            description=f"{pattern.description} Confirmed by review {review_id}: {summary}",
            evidence=_merge_unique(
                pattern.evidence,
                [f"Review {review_id} confirmed decision outcome: {summary}"],
            ),
            confidence=pattern.confidence,
            status=PatternStatus.WATCH,
        )

    if outcome_status is DecisionOutcomeStatus.MIXED:
        return PatternFeedbackUpdate(
            category=pattern.category,
            description=f"{pattern.description} Mixed review {review_id}: {summary}",
            evidence=_merge_unique(
                pattern.evidence,
                [f"Review {review_id} produced mixed decision outcome: {summary}"],
            ),
            confidence=max(pattern.confidence, 0.7),
            status=PatternStatus.WATCH,
        )

    if outcome_status is DecisionOutcomeStatus.INVALIDATED:
        return PatternFeedbackUpdate(
            category=pattern.category,
            description=f"{pattern.description} Not confirmed by review {review_id}: {summary}",
            evidence=_merge_unique(
                pattern.evidence,
                [f"Review {review_id} invalidated decision outcome: {summary}"],
            ),
            confidence=max(pattern.confidence, 0.85),
            status=PatternStatus.ACTIVE,
        )

    raise ValueError(f"Unsupported review outcome: {outcome_status.value}")


def _merge_unique(existing: list[str], incoming: list[str]) -> list[str]:
    merged: list[str] = []
    for item in [*existing, *incoming]:
        text = str(item).strip()
        if text and text not in merged:
            merged.append(text)
    return merged
