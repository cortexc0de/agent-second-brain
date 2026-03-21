"""SQLite-backed persistence for decision runs, records, and reviews."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from enum import Enum
from typing import Callable, Iterable

from d_brain.services.decision_models import (
    DecisionRecord,
    DecisionOutcomeStatus,
    DecisionRun,
    DecisionRunStatus,
    PatternRecord,
    PatternStatus,
    ReviewDeliveryEvent,
    ReviewDeliveryEventType,
    ReviewRecord,
    ReviewStatus,
)

Clock = Callable[[], datetime]


class DecisionStoreError(RuntimeError):
    """Raised when a decision record is missing or invalid."""


class DecisionStore:
    """Deterministic local persistence for decision workflows."""

    def __init__(self, database_path: Path | str, clock: Clock | None = None) -> None:
        self.database_path = str(database_path)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._conn = sqlite3.connect(self.database_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._transaction_depth = 0
        self._init_schema()

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        self._conn.close()

    def __enter__(self) -> DecisionStore:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _init_schema(self) -> None:
        with self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS decision_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    workspace_id TEXT NOT NULL,
                    source_message_id INTEGER,
                    request_text TEXT NOT NULL,
                    decision_type TEXT NOT NULL,
                    time_horizon_days INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    final_verdict TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS decision_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    workspace_id TEXT NOT NULL,
                    decision_run_id INTEGER NOT NULL REFERENCES decision_runs(id) ON DELETE CASCADE,
                    title TEXT NOT NULL,
                    decision_type TEXT NOT NULL,
                    decision_summary TEXT NOT NULL,
                    chosen_option TEXT NOT NULL,
                    rejected_options_json TEXT NOT NULL,
                    why TEXT NOT NULL,
                    risks TEXT NOT NULL,
                    expected_signals_json TEXT NOT NULL,
                    linked_pattern_names_json TEXT NOT NULL DEFAULT '[]',
                    time_horizon_days INTEGER NOT NULL,
                    review_date TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    outcome_status TEXT NOT NULL DEFAULT 'unknown',
                    outcome_summary TEXT,
                    last_reviewed_at TEXT,
                    needs_follow_up INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS review_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    workspace_id TEXT NOT NULL,
                    decision_record_id INTEGER NOT NULL REFERENCES decision_records(id) ON DELETE CASCADE,
                    due_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    expected_outcome TEXT NOT NULL,
                    actual_outcome TEXT,
                    user_response TEXT,
                    agent_assessment TEXT,
                    created_at TEXT NOT NULL,
                    completed_at TEXT,
                    notified_at TEXT,
                    claimed_by TEXT,
                    claim_expires_at TEXT
                );

                CREATE TABLE IF NOT EXISTS pattern_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    workspace_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    description TEXT NOT NULL,
                    evidence_json TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    status TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS review_delivery_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    review_id INTEGER NOT NULL REFERENCES review_records(id) ON DELETE CASCADE,
                    workspace_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    worker_id TEXT,
                    error_code TEXT,
                    error_message TEXT,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_runs_workspace
                    ON decision_runs(workspace_id, id);
                CREATE INDEX IF NOT EXISTS idx_records_workspace
                    ON decision_records(workspace_id, id);
                CREATE INDEX IF NOT EXISTS idx_reviews_due
                    ON review_records(status, due_at, id);
                CREATE INDEX IF NOT EXISTS idx_patterns_workspace
                    ON pattern_records(workspace_id, id);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_patterns_workspace_name
                    ON pattern_records(workspace_id, name);
                CREATE INDEX IF NOT EXISTS idx_review_delivery_events_review
                    ON review_delivery_events(review_id, id);
                CREATE INDEX IF NOT EXISTS idx_review_delivery_events_workspace
                    ON review_delivery_events(workspace_id, created_at, id);
                """
            )
        self._migrate_schema()

    def _migrate_schema(self) -> None:
        columns = {
            row["name"] for row in self._conn.execute("PRAGMA table_info(decision_records)").fetchall()
        }
        migrations = {
            "outcome_status": "ALTER TABLE decision_records ADD COLUMN outcome_status TEXT NOT NULL DEFAULT 'unknown'",
            "outcome_summary": "ALTER TABLE decision_records ADD COLUMN outcome_summary TEXT",
            "last_reviewed_at": "ALTER TABLE decision_records ADD COLUMN last_reviewed_at TEXT",
            "needs_follow_up": "ALTER TABLE decision_records ADD COLUMN needs_follow_up INTEGER NOT NULL DEFAULT 0",
            "linked_pattern_names_json": "ALTER TABLE decision_records ADD COLUMN linked_pattern_names_json TEXT NOT NULL DEFAULT '[]'",
        }
        with self._conn:
            for column, statement in migrations.items():
                if column not in columns:
                    self._conn.execute(statement)
        review_columns = {
            row["name"] for row in self._conn.execute("PRAGMA table_info(review_records)").fetchall()
        }
        review_migrations = {
            "notified_at": "ALTER TABLE review_records ADD COLUMN notified_at TEXT",
            "claimed_by": "ALTER TABLE review_records ADD COLUMN claimed_by TEXT",
            "claim_expires_at": "ALTER TABLE review_records ADD COLUMN claim_expires_at TEXT",
        }
        with self._conn:
            for column, statement in review_migrations.items():
                if column not in review_columns:
                    self._conn.execute(statement)

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    @contextmanager
    def transaction(self):
        """Run multiple store writes in a single atomic SQLite transaction."""
        if self._transaction_depth > 0:
            self._transaction_depth += 1
            try:
                yield
            finally:
                self._transaction_depth -= 1
            return

        self._transaction_depth = 1
        try:
            with self._conn:
                yield
        finally:
            self._transaction_depth = 0

    @contextmanager
    def _write(self):
        if self._transaction_depth > 0:
            yield
            return

        with self._conn:
            yield

    @staticmethod
    def _workspace_key(workspace_id_or_user_id: str | int) -> str:
        return str(workspace_id_or_user_id)

    @staticmethod
    def _serialize_datetime(value: datetime) -> str:
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()

    @staticmethod
    def _parse_datetime(value: str) -> datetime:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed

    @staticmethod
    def _dump_json_list(values: Iterable[str]) -> str:
        return json.dumps(list(values), ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def _load_json_list(value: str) -> list[str]:
        data = json.loads(value)
        if not isinstance(data, list):
            raise DecisionStoreError("Expected JSON array in store")
        return [str(item) for item in data]

    @staticmethod
    def _dump_json_object(value: dict[str, object] | None) -> str:
        return json.dumps(value or {}, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def _load_json_object(value: str) -> dict[str, object]:
        data = json.loads(value)
        if not isinstance(data, dict):
            raise DecisionStoreError("Expected JSON object in store")
        return data

    @staticmethod
    def _status_value(status: DecisionRunStatus | ReviewStatus | str) -> str:
        if isinstance(status, Enum):
            return status.value
        return str(status)

    @staticmethod
    def _pattern_status_value(status: PatternStatus | str) -> str:
        if isinstance(status, Enum):
            return status.value
        return str(status)

    def persist_run(
        self,
        workspace_id_or_user_id: str | int,
        request_text: str,
        *,
        verdict: str | None = None,
        status: DecisionRunStatus = DecisionRunStatus.RECEIVED,
        decision_type: str = "decision",
        source_message_id: int | None = None,
        time_horizon_days: int = 14,
    ) -> DecisionRun:
        """Persist a request-level decision run."""
        return self.create_run(
            workspace_id=self._workspace_key(workspace_id_or_user_id),
            source_message_id=source_message_id,
            request_text=request_text,
            decision_type=decision_type,
            time_horizon_days=time_horizon_days,
            status=status,
            final_verdict=verdict,
        )

    def list_recent(
        self,
        workspace_id_or_user_id: str | int | None = None,
        limit: int = 20,
    ) -> list[DecisionRun]:
        """List the most recent runs, newest first."""
        query = "SELECT * FROM decision_runs"
        params: list[object] = []
        if workspace_id_or_user_id is not None:
            query += " WHERE workspace_id = ?"
            params.append(self._workspace_key(workspace_id_or_user_id))
        query += " ORDER BY created_at DESC, id DESC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_run(row) for row in rows]

    def persist_decision(
        self,
        workspace_id_or_user_id: str | int,
        *,
        decision_run_id: int,
        title: str,
        decision_summary: str,
        chosen_option: str,
        rejected_options: list[str],
        why: str,
        risks: str,
        expected_signals: list[str],
        linked_pattern_names: list[str] | None = None,
        decision_type: str = "decision",
        time_horizon_days: int = 14,
        review_date: datetime | None = None,
        confidence: float = 0.0,
    ) -> DecisionRecord:
        """Persist the durable decision and schedule the default review date."""
        due_date = review_date or (self._now() + timedelta(days=14))
        return self.create_record(
            workspace_id=self._workspace_key(workspace_id_or_user_id),
            decision_run_id=decision_run_id,
            title=title,
            decision_type=decision_type,
            decision_summary=decision_summary,
            chosen_option=chosen_option,
            rejected_options=rejected_options,
            why=why,
            risks=risks,
            expected_signals=expected_signals,
            linked_pattern_names=linked_pattern_names or [],
            time_horizon_days=time_horizon_days,
            review_date=due_date,
            confidence=confidence,
        )

    def create_run(
        self,
        *,
        workspace_id: str,
        source_message_id: int | None,
        request_text: str,
        decision_type: str,
        time_horizon_days: int,
        status: DecisionRunStatus = DecisionRunStatus.RECEIVED,
        final_verdict: str | None = None,
    ) -> DecisionRun:
        now = self._now()
        with self._write():
            cursor = self._conn.execute(
                """
                INSERT INTO decision_runs (
                    workspace_id,
                    source_message_id,
                    request_text,
                    decision_type,
                    time_horizon_days,
                    status,
                    final_verdict,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    workspace_id,
                    source_message_id,
                    request_text,
                    decision_type,
                    time_horizon_days,
                    self._status_value(status),
                    final_verdict,
                    self._serialize_datetime(now),
                    self._serialize_datetime(now),
                ),
            )
        return self.get_run(cursor.lastrowid)

    def get_run(self, run_id: int) -> DecisionRun:
        row = self._conn.execute(
            "SELECT * FROM decision_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            raise DecisionStoreError(f"Decision run {run_id} not found")
        return self._row_to_run(row)

    def list_runs(self, workspace_id: str | None = None, limit: int | None = None) -> list[DecisionRun]:
        query = "SELECT * FROM decision_runs"
        params: list[object] = []
        if workspace_id is not None:
            query += " WHERE workspace_id = ?"
            params.append(workspace_id)
        query += " ORDER BY id"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_run(row) for row in rows]

    def update_run_status(
        self,
        run_id: int,
        status: DecisionRunStatus,
        *,
        final_verdict: str | None = None,
    ) -> DecisionRun:
        now = self._now()
        run = self.get_run(run_id)
        with self._conn:
            self._conn.execute(
                """
                UPDATE decision_runs
                SET status = ?, final_verdict = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    self._status_value(status),
                    final_verdict if final_verdict is not None else run.final_verdict,
                    self._serialize_datetime(now),
                    run_id,
                ),
            )
        return self.get_run(run_id)

    def create_record(
        self,
        *,
        workspace_id: str,
        decision_run_id: int,
        title: str,
        decision_type: str,
        decision_summary: str,
        chosen_option: str,
        rejected_options: list[str],
        why: str,
        risks: str,
        expected_signals: list[str],
        linked_pattern_names: list[str],
        time_horizon_days: int,
        review_date: datetime,
        confidence: float,
    ) -> DecisionRecord:
        now = self._now()
        with self._write():
            cursor = self._conn.execute(
                """
                INSERT INTO decision_records (
                    workspace_id,
                    decision_run_id,
                    title,
                    decision_type,
                    decision_summary,
                    chosen_option,
                    rejected_options_json,
                    why,
                    risks,
                    expected_signals_json,
                    linked_pattern_names_json,
                    time_horizon_days,
                    review_date,
                    confidence,
                    outcome_status,
                    outcome_summary,
                    last_reviewed_at,
                    needs_follow_up,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    workspace_id,
                    decision_run_id,
                    title,
                    decision_type,
                    decision_summary,
                    chosen_option,
                    self._dump_json_list(rejected_options),
                    why,
                    risks,
                    self._dump_json_list(expected_signals),
                    self._dump_json_list(linked_pattern_names),
                    time_horizon_days,
                    self._serialize_datetime(review_date),
                    confidence,
                    DecisionOutcomeStatus.UNKNOWN.value,
                    None,
                    None,
                    0,
                    self._serialize_datetime(now),
                    self._serialize_datetime(now),
                ),
            )
        return self.get_record(cursor.lastrowid)

    def get_record(self, record_id: int) -> DecisionRecord:
        row = self._conn.execute(
            "SELECT * FROM decision_records WHERE id = ?",
            (record_id,),
        ).fetchone()
        if row is None:
            raise DecisionStoreError(f"Decision record {record_id} not found")
        return self._row_to_record(row)

    def list_records(
        self,
        workspace_id: str | None = None,
        limit: int | None = None,
    ) -> list[DecisionRecord]:
        query = "SELECT * FROM decision_records"
        params: list[object] = []
        if workspace_id is not None:
            query += " WHERE workspace_id = ?"
            params.append(workspace_id)
        query += " ORDER BY id"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_record(row) for row in rows]

    def create_review(
        self,
        *,
        workspace_id: str,
        decision_record_id: int,
        due_at: datetime,
        expected_outcome: str,
        status: ReviewStatus = ReviewStatus.SCHEDULED,
        actual_outcome: str | None = None,
        user_response: str | None = None,
        agent_assessment: str | None = None,
    ) -> ReviewRecord:
        now = self._now()
        with self._write():
            cursor = self._conn.execute(
                """
                INSERT INTO review_records (
                    workspace_id,
                    decision_record_id,
                    due_at,
                    status,
                    expected_outcome,
                    actual_outcome,
                    user_response,
                    agent_assessment,
                    created_at,
                    completed_at,
                    notified_at,
                    claimed_by,
                    claim_expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    workspace_id,
                    decision_record_id,
                    self._serialize_datetime(due_at),
                    self._status_value(status),
                    expected_outcome,
                    actual_outcome,
                    user_response,
                    agent_assessment,
                    self._serialize_datetime(now),
                    None,
                    None,
                    None,
                    None,
                ),
            )
        return self.get_review(cursor.lastrowid)

    def get_review(self, review_id: int) -> ReviewRecord:
        row = self._conn.execute(
            "SELECT * FROM review_records WHERE id = ?",
            (review_id,),
        ).fetchone()
        if row is None:
            raise DecisionStoreError(f"Review record {review_id} not found")
        return self._row_to_review(row)

    def list_reviews(
        self,
        workspace_id: str | None = None,
        status: ReviewStatus | None = None,
        limit: int | None = None,
    ) -> list[ReviewRecord]:
        query = "SELECT * FROM review_records"
        params: list[object] = []
        clauses: list[str] = []
        if workspace_id is not None:
            clauses.append("workspace_id = ?")
            params.append(workspace_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(self._status_value(status))
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY due_at, id"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_review(row) for row in rows]

    def list_due_reviews(self, when: datetime | None = None) -> list[ReviewRecord]:
        threshold = when or self._now()
        rows = self._conn.execute(
            """
            SELECT * FROM review_records
            WHERE status IN (?, ?) AND due_at <= ?
            ORDER BY due_at, id
            """,
            (
                self._status_value(ReviewStatus.SCHEDULED),
                self._status_value(ReviewStatus.DUE),
                self._serialize_datetime(threshold),
            ),
        ).fetchall()
        return [self._row_to_review(row) for row in rows]

    def list_pending_review_notifications(
        self,
        when: datetime | None = None,
        limit: int | None = None,
    ) -> list[ReviewRecord]:
        threshold = when or self._now()
        query = """
            SELECT * FROM review_records
            WHERE status IN (?, ?)
              AND due_at <= ?
              AND notified_at IS NULL
              AND (claim_expires_at IS NULL OR claim_expires_at <= ?)
            ORDER BY due_at, id
        """
        params: list[object] = [
            self._status_value(ReviewStatus.SCHEDULED),
            self._status_value(ReviewStatus.DUE),
            self._serialize_datetime(threshold),
            self._serialize_datetime(threshold),
        ]
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_review(row) for row in rows]

    def claim_due_review_notifications(
        self,
        *,
        claimer_id: str,
        when: datetime | None = None,
        lease_expires_at: datetime | None = None,
        limit: int = 20,
    ) -> list[ReviewRecord]:
        threshold = when or self._now()
        lease_until = lease_expires_at or (threshold + timedelta(minutes=5))
        claimed_ids: list[int] = []
        with self.transaction():
            candidate_rows = self._conn.execute(
                """
                SELECT id FROM review_records
                WHERE status IN (?, ?)
                  AND due_at <= ?
                  AND notified_at IS NULL
                  AND (claim_expires_at IS NULL OR claim_expires_at <= ?)
                ORDER BY due_at, id
                LIMIT ?
                """,
                (
                    self._status_value(ReviewStatus.SCHEDULED),
                    self._status_value(ReviewStatus.DUE),
                    self._serialize_datetime(threshold),
                    self._serialize_datetime(threshold),
                    limit,
                ),
            ).fetchall()
            for row in candidate_rows:
                cursor = self._conn.execute(
                    """
                    UPDATE review_records
                    SET claimed_by = ?,
                        claim_expires_at = ?
                    WHERE id = ?
                      AND notified_at IS NULL
                      AND (claim_expires_at IS NULL OR claim_expires_at <= ?)
                    """,
                    (
                        claimer_id,
                        self._serialize_datetime(lease_until),
                        row["id"],
                        self._serialize_datetime(threshold),
                    ),
                )
                if cursor.rowcount == 1:
                    claimed_ids.append(int(row["id"]))
            claimed_reviews = [self.get_review(review_id) for review_id in claimed_ids]
            for review in claimed_reviews:
                self._conn.execute(
                    """
                    INSERT INTO review_delivery_events (
                        review_id,
                        workspace_id,
                        event_type,
                        worker_id,
                        error_code,
                        error_message,
                        metadata_json,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        review.id,
                        review.workspace_id,
                        ReviewDeliveryEventType.CLAIMED.value,
                        claimer_id,
                        None,
                        None,
                        self._dump_json_object(
                            {
                                "claim_expires_at": self._serialize_datetime(lease_until),
                            }
                        ),
                        self._serialize_datetime(threshold),
                    ),
                )
        return claimed_reviews

    def update_review(
        self,
        review_id: int,
        status: ReviewStatus,
        *,
        actual_outcome: str | None = None,
        user_response: str | None = None,
        agent_assessment: str | None = None,
    ) -> ReviewRecord:
        review = self.get_review(review_id)
        completed_at = review.completed_at
        if status in {ReviewStatus.COMPLETED, ReviewStatus.SKIPPED}:
            completed_at = self._now()
        with self._write():
            self._conn.execute(
                """
                UPDATE review_records
                SET status = ?,
                    actual_outcome = COALESCE(?, actual_outcome),
                    user_response = COALESCE(?, user_response),
                    agent_assessment = COALESCE(?, agent_assessment),
                    completed_at = ?
                WHERE id = ?
                """,
                (
                    self._status_value(status),
                    actual_outcome,
                    user_response,
                    agent_assessment,
                    self._serialize_datetime(completed_at) if completed_at else None,
                    review_id,
                ),
            )
        return self.get_review(review_id)

    def mark_review_notified(
        self,
        review_id: int,
        *,
        notified_at: datetime | None = None,
        claimer_id: str | None = None,
    ) -> ReviewRecord:
        timestamp = notified_at or self._now()
        with self._write():
            cursor = self._conn.execute(
                """
                UPDATE review_records
                SET status = ?,
                    notified_at = ?,
                    claimed_by = NULL,
                    claim_expires_at = NULL
                WHERE id = ?
                  AND (? IS NULL OR claimed_by = ?)
                """,
                (
                    self._status_value(ReviewStatus.DUE),
                    self._serialize_datetime(timestamp),
                    review_id,
                    claimer_id,
                    claimer_id,
                ),
            )
        if cursor.rowcount != 1:
            raise DecisionStoreError(f"Review record {review_id} is not claimed by {claimer_id}")
        return self.get_review(review_id)

    def release_review_claim(
        self,
        review_id: int,
        *,
        claimer_id: str | None = None,
    ) -> ReviewRecord:
        with self._write():
            cursor = self._conn.execute(
                """
                UPDATE review_records
                SET claimed_by = NULL,
                    claim_expires_at = NULL
                WHERE id = ?
                  AND (? IS NULL OR claimed_by = ?)
                """,
                (
                    review_id,
                    claimer_id,
                    claimer_id,
                ),
            )
        if cursor.rowcount != 1:
            raise DecisionStoreError(f"Review record {review_id} is not claimed by {claimer_id}")
        return self.get_review(review_id)

    def append_review_delivery_event(
        self,
        *,
        review_id: int,
        workspace_id: str,
        event_type: ReviewDeliveryEventType,
        worker_id: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
        metadata: dict[str, object] | None = None,
        created_at: datetime | None = None,
    ) -> ReviewDeliveryEvent:
        event_time = created_at or self._now()
        with self._write():
            cursor = self._conn.execute(
                """
                INSERT INTO review_delivery_events (
                    review_id,
                    workspace_id,
                    event_type,
                    worker_id,
                    error_code,
                    error_message,
                    metadata_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    review_id,
                    workspace_id,
                    event_type.value,
                    worker_id,
                    error_code,
                    error_message,
                    self._dump_json_object(metadata),
                    self._serialize_datetime(event_time),
                ),
            )
        row = self._conn.execute(
            "SELECT * FROM review_delivery_events WHERE id = ?",
            (cursor.lastrowid,),
        ).fetchone()
        if row is None:
            raise DecisionStoreError("Review delivery event insert failed")
        return self._row_to_review_delivery_event(row)

    def list_review_delivery_events(
        self,
        review_id: int,
        limit: int | None = None,
    ) -> list[ReviewDeliveryEvent]:
        query = "SELECT * FROM review_delivery_events WHERE review_id = ? ORDER BY id"
        params: list[object] = [review_id]
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_review_delivery_event(row) for row in rows]

    def update_record_outcome(
        self,
        record_id: int,
        *,
        outcome_status: DecisionOutcomeStatus,
        outcome_summary: str,
        needs_follow_up: bool,
        last_reviewed_at: datetime | None = None,
    ) -> DecisionRecord:
        reviewed_at = last_reviewed_at or self._now()
        record = self.get_record(record_id)
        with self._write():
            self._conn.execute(
                """
                UPDATE decision_records
                SET outcome_status = ?,
                    outcome_summary = ?,
                    last_reviewed_at = ?,
                    needs_follow_up = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    outcome_status.value,
                    outcome_summary.strip() or record.outcome_summary,
                    self._serialize_datetime(reviewed_at),
                    1 if needs_follow_up else 0,
                    self._serialize_datetime(reviewed_at),
                    record_id,
                ),
            )
        return self.get_record(record_id)

    def persist_pattern(
        self,
        workspace_id_or_user_id: str | int,
        *,
        name: str,
        category: str,
        description: str,
        evidence: list[str],
        confidence: float,
        status: PatternStatus = PatternStatus.ACTIVE,
        last_seen_at: datetime | None = None,
    ) -> PatternRecord:
        """Persist a deterministic pattern record."""
        workspace_id = self._workspace_key(workspace_id_or_user_id)
        existing = self._conn.execute(
            "SELECT id FROM pattern_records WHERE workspace_id = ? AND name = ?",
            (workspace_id, name),
        ).fetchone()

        if existing is None:
            return self.create_pattern(
                workspace_id=workspace_id,
                name=name,
                category=category,
                description=description,
                evidence=evidence,
                confidence=confidence,
                status=status,
                last_seen_at=last_seen_at or self._now(),
            )

        return self.update_pattern(
            int(existing["id"]),
            category=category,
            description=description,
            evidence=evidence,
            confidence=confidence,
            status=status,
            last_seen_at=last_seen_at or self._now(),
        )

    def create_pattern(
        self,
        *,
        workspace_id: str,
        name: str,
        category: str,
        description: str,
        evidence: list[str],
        confidence: float,
        status: PatternStatus = PatternStatus.ACTIVE,
        last_seen_at: datetime,
    ) -> PatternRecord:
        now = self._now()
        with self._write():
            cursor = self._conn.execute(
                """
                INSERT INTO pattern_records (
                    workspace_id,
                    name,
                    category,
                    description,
                    evidence_json,
                    confidence,
                    status,
                    last_seen_at,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    workspace_id,
                    name,
                    category,
                    description,
                    self._dump_json_list(evidence),
                    confidence,
                    self._pattern_status_value(status),
                    self._serialize_datetime(last_seen_at),
                    self._serialize_datetime(now),
                    self._serialize_datetime(now),
                ),
            )
        return self.get_pattern(cursor.lastrowid)

    def get_pattern(self, pattern_id: int) -> PatternRecord:
        row = self._conn.execute(
            "SELECT * FROM pattern_records WHERE id = ?",
            (pattern_id,),
        ).fetchone()
        if row is None:
            raise DecisionStoreError(f"Pattern record {pattern_id} not found")
        return self._row_to_pattern(row)

    def update_pattern(
        self,
        pattern_id: int,
        *,
        category: str,
        description: str,
        evidence: list[str],
        confidence: float,
        status: PatternStatus = PatternStatus.ACTIVE,
        last_seen_at: datetime | None = None,
    ) -> PatternRecord:
        pattern = self.get_pattern(pattern_id)
        merged_evidence = self._merge_evidence(pattern.evidence, evidence)
        updated_at = self._now()
        seen_at = last_seen_at or updated_at

        with self._write():
            self._conn.execute(
                """
                UPDATE pattern_records
                SET category = ?,
                    description = ?,
                    evidence_json = ?,
                    confidence = ?,
                    status = ?,
                    last_seen_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    category,
                    description,
                    self._dump_json_list(merged_evidence),
                    max(pattern.confidence, confidence),
                    self._pattern_status_value(status),
                    self._serialize_datetime(seen_at),
                    self._serialize_datetime(updated_at),
                    pattern_id,
                ),
            )
        return self.get_pattern(pattern_id)

    def list_patterns(
        self,
        workspace_id: str | None = None,
        status: PatternStatus | None = None,
        limit: int | None = None,
    ) -> list[PatternRecord]:
        query = "SELECT * FROM pattern_records"
        params: list[object] = []
        clauses: list[str] = []
        if workspace_id is not None:
            clauses.append("workspace_id = ?")
            params.append(workspace_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(self._pattern_status_value(status))
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY last_seen_at DESC, id DESC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        rows = self._conn.execute(query, params).fetchall()
        return [self._row_to_pattern(row) for row in rows]

    @staticmethod
    def _merge_evidence(existing: list[str], incoming: list[str]) -> list[str]:
        merged: list[str] = []
        for item in [*existing, *incoming]:
            text = str(item).strip()
            if text and text not in merged:
                merged.append(text)
        return merged

    @staticmethod
    def _row_to_run(row: sqlite3.Row) -> DecisionRun:
        return DecisionRun(
            id=row["id"],
            workspace_id=row["workspace_id"],
            source_message_id=row["source_message_id"],
            request_text=row["request_text"],
            decision_type=row["decision_type"],
            time_horizon_days=row["time_horizon_days"],
            status=DecisionRunStatus(row["status"]),
            final_verdict=row["final_verdict"],
            created_at=DecisionStore._parse_datetime(row["created_at"]),
            updated_at=DecisionStore._parse_datetime(row["updated_at"]),
        )

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> DecisionRecord:
        return DecisionRecord(
            id=row["id"],
            workspace_id=row["workspace_id"],
            decision_run_id=row["decision_run_id"],
            title=row["title"],
            decision_type=row["decision_type"],
            decision_summary=row["decision_summary"],
            chosen_option=row["chosen_option"],
            rejected_options=DecisionStore._load_json_list(row["rejected_options_json"]),
            why=row["why"],
            risks=row["risks"],
            expected_signals=DecisionStore._load_json_list(row["expected_signals_json"]),
            time_horizon_days=row["time_horizon_days"],
            review_date=DecisionStore._parse_datetime(row["review_date"]),
            confidence=row["confidence"],
            created_at=DecisionStore._parse_datetime(row["created_at"]),
            updated_at=DecisionStore._parse_datetime(row["updated_at"]),
            outcome_status=DecisionOutcomeStatus(row["outcome_status"] or DecisionOutcomeStatus.UNKNOWN.value),
            outcome_summary=row["outcome_summary"],
            last_reviewed_at=(
                DecisionStore._parse_datetime(row["last_reviewed_at"])
                if row["last_reviewed_at"] is not None
                else None
            ),
            needs_follow_up=bool(row["needs_follow_up"]),
            linked_pattern_names=DecisionStore._load_json_list(row["linked_pattern_names_json"]),
        )

    @staticmethod
    def _row_to_review(row: sqlite3.Row) -> ReviewRecord:
        return ReviewRecord(
            id=row["id"],
            workspace_id=row["workspace_id"],
            decision_record_id=row["decision_record_id"],
            due_at=DecisionStore._parse_datetime(row["due_at"]),
            status=ReviewStatus(row["status"]),
            expected_outcome=row["expected_outcome"],
            actual_outcome=row["actual_outcome"],
            user_response=row["user_response"],
            agent_assessment=row["agent_assessment"],
            created_at=DecisionStore._parse_datetime(row["created_at"]),
            completed_at=(
                DecisionStore._parse_datetime(row["completed_at"])
                if row["completed_at"] is not None
                else None
            ),
            notified_at=(
                DecisionStore._parse_datetime(row["notified_at"])
                if row["notified_at"] is not None
                else None
            ),
            claimed_by=row["claimed_by"],
            claim_expires_at=(
                DecisionStore._parse_datetime(row["claim_expires_at"])
                if row["claim_expires_at"] is not None
                else None
            ),
        )

    @staticmethod
    def _row_to_review_delivery_event(row: sqlite3.Row) -> ReviewDeliveryEvent:
        return ReviewDeliveryEvent(
            id=row["id"],
            review_id=row["review_id"],
            workspace_id=row["workspace_id"],
            event_type=ReviewDeliveryEventType(row["event_type"]),
            worker_id=row["worker_id"],
            error_code=row["error_code"],
            error_message=row["error_message"],
            metadata=DecisionStore._load_json_object(row["metadata_json"]),
            created_at=DecisionStore._parse_datetime(row["created_at"]),
        )

    @staticmethod
    def _row_to_pattern(row: sqlite3.Row) -> PatternRecord:
        return PatternRecord(
            id=row["id"],
            workspace_id=row["workspace_id"],
            name=row["name"],
            category=row["category"],
            description=row["description"],
            evidence=DecisionStore._load_json_list(row["evidence_json"]),
            confidence=row["confidence"],
            status=PatternStatus(row["status"]),
            last_seen_at=DecisionStore._parse_datetime(row["last_seen_at"]),
            created_at=DecisionStore._parse_datetime(row["created_at"]),
            updated_at=DecisionStore._parse_datetime(row["updated_at"]),
        )
