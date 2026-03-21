"""Domain models for decision storage."""

from __future__ import annotations

from dataclasses import dataclass, field
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


class DecisionOutcomeStatus(str, Enum):
    """Observed result after a review checkpoint."""

    UNKNOWN = "unknown"
    CONFIRMED = "confirmed"
    MIXED = "mixed"
    INVALIDATED = "invalidated"


class PatternStatus(str, Enum):
    """Lifecycle for a behavioral pattern."""

    ACTIVE = "active"
    WATCH = "watch"
    ARCHIVED = "archived"


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
    outcome_status: DecisionOutcomeStatus = DecisionOutcomeStatus.UNKNOWN
    outcome_summary: str | None = None
    last_reviewed_at: datetime | None = None
    needs_follow_up: bool = False
    linked_pattern_names: list[str] = field(default_factory=list)


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
    notified_at: datetime | None = None
    claimed_by: str | None = None
    claim_expires_at: datetime | None = None


@dataclass(slots=True)
class PatternRecord:
    """Stored decision pattern or bias signal."""

    id: int
    workspace_id: str
    name: str
    category: str
    description: str
    evidence: list[str]
    confidence: float
    status: PatternStatus
    last_seen_at: datetime
    created_at: datetime
    updated_at: datetime
