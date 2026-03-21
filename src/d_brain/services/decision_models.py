"""Domain models for decision storage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class DecisionRunStatus(str, Enum):
    """Lifecycle for a decision run."""

    RECEIVED = "received"
    REASONING = "reasoning"
    COMPLETED = "completed"
    FAILED = "failed"


class ReviewStatus(str, Enum):
    """Lifecycle for a review checkpoint."""

    SCHEDULED = "scheduled"
    DUE = "due"
    COMPLETED = "completed"
    SKIPPED = "skipped"


@dataclass(slots=True)
class DecisionRun:
    """Stored decision request and processing state."""

    id: int
    workspace_id: str
    source_message_id: int | None
    request_text: str
    decision_type: str
    time_horizon_days: int
    status: DecisionRunStatus
    final_verdict: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class DecisionRecord:
    """Stored decision outcome and rationale."""

    id: int
    workspace_id: str
    decision_run_id: int
    title: str
    decision_type: str
    decision_summary: str
    chosen_option: str
    rejected_options: list[str]
    why: str
    risks: str
    expected_signals: list[str]
    time_horizon_days: int
    review_date: datetime
    confidence: float
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class ReviewRecord:
    """Scheduled follow-up for a decision record."""

    id: int
    workspace_id: str
    decision_record_id: int
    due_at: datetime
    status: ReviewStatus
    expected_outcome: str
    actual_outcome: str | None
    user_response: str | None
    agent_assessment: str | None
    created_at: datetime
    completed_at: datetime | None
