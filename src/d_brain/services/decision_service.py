"""Decision-focused service for Founder Decision Partner V1."""

from __future__ import annotations

import html
import itertools
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from d_brain.services.decision_models import DecisionRunStatus
from d_brain.services.decision_store import DecisionStore, DecisionStoreError
from d_brain.services.pattern_detector import detect_patterns
from d_brain.services.processor import ClaudeProcessor

DEFAULT_DECISION_HORIZON_DAYS = 14
logger = logging.getLogger(__name__)


class DecisionServiceError(RuntimeError):
    """Raised when a decision trace operation is invalid."""


def _normalize_lines(value: Any) -> list[str]:
    """Convert Claude JSON values into a clean list of non-empty lines."""
    if isinstance(value, str):
        items = [value]
    elif isinstance(value, list):
        items = [str(item) for item in value]
    else:
        items = []

    return [item.strip() for item in items if str(item).strip()]


def format_decision_html(decision: dict[str, Any]) -> str:
    """Build a deterministic Telegram HTML response from decision data."""
    verdict = html.escape(str(decision.get("verdict", "")).strip() or "Нужно уточнить запрос.")
    why = _normalize_lines(decision.get("why"))
    do_not_do = _normalize_lines(decision.get("do_not_do"))
    risks = _normalize_lines(decision.get("risks"))
    check_signals = _normalize_lines(decision.get("check_in_signals"))
    patterns = _normalize_lines(decision.get("patterns"))
    counter_argument = html.escape(
        str(decision.get("counter_argument", "")).strip()
    )
    horizon_days = int(decision.get("check_in_days") or DEFAULT_DECISION_HORIZON_DAYS)
    trace_run_id = decision.get("trace_run_id")

    parts = [f"🎯 <b>Решение на {horizon_days} дней</b>", "", f"<b>Вердикт:</b> {verdict}"]

    if why:
        parts.extend(["", "<b>Почему:</b>"])
        parts.extend(f"• {html.escape(item)}" for item in why)

    if do_not_do:
        parts.extend(["", "<b>Не делай:</b>"])
        parts.extend(f"• {html.escape(item)}" for item in do_not_do)

    if risks:
        parts.extend(["", "<b>Риски:</b>"])
        parts.extend(f"• {html.escape(item)}" for item in risks)

    if check_signals:
        parts.extend(["", f"<b>Что проверить через {horizon_days} дней:</b>"])
        parts.extend(f"• {html.escape(item)}" for item in check_signals)

    if counter_argument:
        parts.extend(["", f"<b>Что может это опровергнуть:</b> {counter_argument}"])

    if patterns:
        parts.extend(["", "<b>Что у тебя повторяется:</b>"])
        parts.extend(f"• {html.escape(item)}" for item in patterns)

    if trace_run_id:
        parts.extend(["", f"<b>Trace:</b> <code>/decide_trace {int(trace_run_id)}</code>"])

    return "\n".join(parts)


class DecisionService:
    """High-level service for decision support requests."""

    REVIEW_SOON_WINDOW = timedelta(days=2)

    def __init__(
        self,
        vault_path: str | Path,
        todoist_api_key: str = "",
        *,
        horizon_days: int = DEFAULT_DECISION_HORIZON_DAYS,
        processor: ClaudeProcessor | None = None,
        store: Any | None = None,
        store_path: str | Path | None = None,
        clock: callable | None = None,
    ) -> None:
        self.processor = processor or ClaudeProcessor(vault_path, todoist_api_key)
        self.horizon_days = horizon_days
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
            raise DecisionServiceError("Decision store is not configured")
        return DecisionStore(self.store_path), True

    @staticmethod
    def _ensure_owner(run: Any, user_id: int) -> None:
        if str(run.workspace_id) != str(user_id):
            raise DecisionServiceError("Decision trace does not belong to this user")

    def _decision_attention_rank(self, record: Any, review: Any | None) -> tuple[int, int]:
        if review is not None and review.status.value in {"scheduled", "due"}:
            if review.due_at <= self._now():
                return (0, -record.decision_run_id)
            if review.due_at <= self._now() + self.REVIEW_SOON_WINDOW:
                return (1, -record.decision_run_id)
        if record.needs_follow_up:
            return (2, -record.decision_run_id)
        status = record.outcome_status.value
        order = {
            "invalidated": 3,
            "mixed": 4,
            "unknown": 5,
            "confirmed": 6,
        }
        return (order.get(status, 5), -record.decision_run_id)

    def _decision_section_key(self, record: Any, review: Any | None) -> int:
        if record.needs_follow_up:
            return 0
        if review is not None and review.status.value in {"scheduled", "due"}:
            if review.due_at <= self._now():
                return 0
            if review.due_at <= self._now() + self.REVIEW_SOON_WINDOW:
                return 1
        if record.outcome_status.value in {"invalidated", "mixed"}:
            return 0
        return 2

    @staticmethod
    def _section_label(section_key: int) -> str:
        labels = {
            0: "🚨 <b>Needs Attention</b>",
            1: "⏳ <b>Due Soon</b>",
            2: "✅ <b>Stable</b>",
        }
        return labels.get(section_key, "✅ <b>Stable</b>")

    @staticmethod
    def _render_outcome_label(record: Any) -> str:
        status = record.outcome_status.value
        labels = {
            "invalidated": "Не подтвердилось",
            "mixed": "🟡 Частично подтвердилось",
            "unknown": "⚪ Без итога",
            "confirmed": "🟢 Подтвердилось",
        }
        if record.needs_follow_up:
            suffix = labels.get(status)
            if suffix and status != "unknown":
                return f"🔴 Требует внимания · {suffix}"
            return "🔴 Требует внимания"
        return labels.get(status, f"⚪ {html.escape(status)}")

    def _render_review_timing_label(self, review: Any | None) -> str | None:
        if review is None or review.status.value not in {"scheduled", "due"}:
            return None
        if review.due_at <= self._now():
            return "🔴 Review просрочен"
        if review.due_at <= self._now() + self.REVIEW_SOON_WINDOW:
            return "🟡 Review скоро"
        return None

    def _render_next_action(self, run: Any, record: Any, review: Any | None) -> str:
        if record.needs_follow_up:
            return (
                f"<b>Следующее действие:</b> 🔴 Пересмотри решение — "
                f"<code>/decide_trace {run.id}</code>"
            )
        if review is not None and review.status.value in {"scheduled", "due"}:
            if review.due_at <= self._now():
                return (
                    f"<b>Следующее действие:</b> 🔴 Закрой review — "
                    f"<code>/review_done {review.id} что получилось</code>"
                )
            if review.due_at <= self._now() + self.REVIEW_SOON_WINDOW:
                return (
                    f"<b>Следующее действие:</b> 🟡 Подготовь review — "
                    f"<code>/review_trace {review.id}</code>"
                )
        status = record.outcome_status.value
        if status == "invalidated":
            return (
                f"<b>Следующее действие:</b> 🔴 Пересобери решение — "
                f"<code>/decide_trace {run.id}</code>"
            )
        if status == "mixed":
            return (
                f"<b>Следующее действие:</b> 🟡 Разбери, что сработало — "
                f"<code>/decide_trace {run.id}</code>"
            )
        if status == "unknown":
            if review is not None:
                return (
                    f"<b>Следующее действие:</b> ⚪ Держи в фокусе — "
                    f"<code>/review_trace {review.id}</code>"
                )
            return (
                f"<b>Следующее действие:</b> ⚪ Держи в фокусе — "
                f"<code>/decide_trace {run.id}</code>"
            )
        if status == "confirmed":
            if review is not None:
                return (
                    f"<b>Следующее действие:</b> 🟢 Наблюдай — "
                    f"<code>/review_trace {review.id}</code>"
                )
            return (
                f"<b>Следующее действие:</b> 🟢 Наблюдай — "
                f"<code>/decide_trace {run.id}</code>"
            )
        return ""

    def render_recent_decisions(self, user_id: int, limit: int = 5) -> str:
        """Render a compact overview of latest persisted decisions."""
        store, created_store = self._open_store()
        try:
            reviews_by_record_id = {
                review.decision_record_id: review
                for review in store.list_reviews(str(user_id))
            }
            records = sorted(
                store.list_records(str(user_id)),
                key=lambda record: (
                    self._decision_section_key(record, reviews_by_record_id.get(record.id)),
                    *self._decision_attention_rank(
                        record,
                        reviews_by_record_id.get(record.id),
                    ),
                ),
            )[:limit]
            if not records:
                return "🗂️ <b>Пока нет сохранённых решений</b>"

            parts = ["🗂️ <b>Последние решения</b>"]

            grouped_records = itertools.groupby(
                records,
                key=lambda record: self._decision_section_key(
                    record,
                    reviews_by_record_id.get(record.id),
                ),
            )

            for section_key, section_records in grouped_records:
                parts.extend(["", self._section_label(section_key)])
                for record in section_records:
                    run = store.get_run(record.decision_run_id)
                    self._ensure_owner(run, user_id)
                    review = reviews_by_record_id.get(record.id)
                    review_timing_label = self._render_review_timing_label(review)
                    parts.extend(
                        [
                            "",
                            f"<b>Run:</b> <code>{run.id}</code>",
                            f"<b>Запрос:</b> {html.escape(run.request_text)}",
                            f"<b>Вердикт:</b> {html.escape(record.chosen_option)}",
                            f"<b>Итог:</b> {self._render_outcome_label(record)}",
                        ]
                    )
                    if review_timing_label:
                        parts.append(f"<b>Review-сигнал:</b> {review_timing_label}")
                    if review is not None:
                        parts.append(
                            f"<b>Review:</b> <code>{review.id}</code> ({html.escape(review.status.value)})"
                        )
                        parts.append(
                            f"<code>/review_trace {review.id}</code> · "
                            f"<code>/review_done {review.id} что получилось</code>"
                        )
                    next_action = self._render_next_action(run, record, review)
                    if next_action:
                        parts.append(next_action)
                    parts.append(f"<code>/decide_trace {run.id}</code>")

            return "\n".join(parts)
        except DecisionStoreError as exc:
            raise DecisionServiceError(str(exc)) from exc
        finally:
            if created_store and isinstance(store, DecisionStore):
                store.close()

    def render_decision_trace(self, user_id: int, run_id: int) -> str:
        """Render a decision trace from existing run/record/review state."""
        store, created_store = self._open_store()
        try:
            run = store.get_run(run_id)
            self._ensure_owner(run, user_id)

            record = next(
                (item for item in store.list_records(str(user_id)) if item.decision_run_id == run.id),
                None,
            )
            review = None
            if record is not None:
                review = next(
                    (
                        item
                        for item in store.list_reviews(str(user_id))
                        if item.decision_record_id == record.id
                    ),
                    None,
                )

            parts = [
                "🔎 <b>Decision Trace</b>",
                "",
                f"<b>ID:</b> <code>{run.id}</code>",
                f"<b>Тип:</b> {html.escape(run.decision_type)}",
                f"<b>Статус:</b> {html.escape(run.status.value)}",
                f"<b>Горизонт:</b> {run.time_horizon_days} дней",
                "",
                f"<b>Запрос:</b> {html.escape(run.request_text)}",
            ]

            if run.final_verdict:
                parts.append(f"<b>Вердикт:</b> {html.escape(run.final_verdict)}")

            if record is None:
                parts.extend(["", "<i>Decision record пока не зафиксирован.</i>"])
                return "\n".join(parts)

            parts.extend(
                [
                    "",
                    f"<b>Заголовок:</b> {html.escape(record.title)}",
                    f"<b>Summary:</b> {html.escape(record.decision_summary)}",
                    f"<b>Почему:</b> {html.escape(record.why)}",
                    f"<b>Риски:</b> {html.escape(record.risks)}",
                ]
            )

            if record.rejected_options:
                parts.append(
                    f"<b>Не делай:</b> {html.escape('; '.join(record.rejected_options))}"
                )
            if record.expected_signals:
                parts.append(
                    f"<b>Что проверить:</b> {html.escape('; '.join(record.expected_signals))}"
                )
            if record.linked_pattern_names:
                parts.append(
                    f"<b>Паттерны:</b> {html.escape(', '.join(record.linked_pattern_names))}"
                )
            if review is not None:
                parts.append(
                    f"<b>Связанный review:</b> <code>{review.id}</code> "
                    f"({html.escape(review.status.value)}) — "
                    f"<code>/review_trace {review.id}</code>"
                )
                parts.append(
                    f"<b>Закрыть цикл:</b> <code>/review_done {review.id} что получилось</code>"
                )
            return "\n".join(parts)
        except DecisionStoreError as exc:
            raise DecisionServiceError(str(exc)) from exc
        finally:
            if created_store and isinstance(store, DecisionStore):
                store.close()

    def decide(self, prompt: str, user_id: int = 0) -> dict[str, Any]:
        """Run a decision-specific reasoning flow and persist the result if possible."""
        result = self.processor.execute_decision(prompt, user_id, self.horizon_days)
        if "error" in result:
            return result

        decision = result.get("decision")
        if not isinstance(decision, dict):
            return {"error": "Decision payload missing or invalid", "processed_entries": 0}

        store = self.store
        created_store = False
        if store is None and self.store_path is not None:
            store = DecisionStore(self.store_path)
            created_store = True

        run = None
        record = None
        review = None
        try:
            recent_records = store.list_records(str(user_id), limit=5) if store is not None else []
            existing_patterns = store.list_patterns(str(user_id), limit=10) if store is not None else []
            detected_patterns = detect_patterns(prompt, recent_records, existing_patterns)

            if store is not None:
                create_review = getattr(store, "create_review", None)
                persist_pattern = getattr(store, "persist_pattern", None)
                persist_run = getattr(store, "persist_run", None)
                persist_decision = getattr(store, "persist_decision", None)
                transaction = getattr(store, "transaction", None)

                why_lines = _normalize_lines(decision.get("why"))
                risk_lines = _normalize_lines(decision.get("risks"))
                check_signals = _normalize_lines(decision.get("check_in_signals"))
                try:
                    if callable(transaction):
                        with transaction():
                            if callable(persist_run):
                                run = persist_run(
                                    user_id,
                                    prompt,
                                    verdict=str(decision.get("verdict", "")),
                                    status=DecisionRunStatus.COMPLETED,
                                    decision_type=str(decision.get("decision_type", "decision")),
                                    time_horizon_days=int(decision.get("check_in_days") or self.horizon_days),
                                )
                            if callable(persist_decision) and run is not None:
                                record = persist_decision(
                                    user_id,
                                    decision_run_id=run.id,
                                    title=str(decision.get("title", "Decision")),
                                    decision_type=str(decision.get("decision_type", "decision")),
                                    decision_summary=str(decision.get("summary", "")),
                                    chosen_option=str(decision.get("verdict", "")),
                                    rejected_options=_normalize_lines(decision.get("do_not_do")),
                                    why="\n".join(why_lines),
                                    risks="\n".join(risk_lines),
                                    expected_signals=check_signals,
                                    linked_pattern_names=[pattern.name for pattern in detected_patterns],
                                    time_horizon_days=int(decision.get("check_in_days") or self.horizon_days),
                                    confidence=float(decision.get("confidence") or 0.0),
                                )
                            if callable(create_review) and record is not None:
                                expected_outcome = "; ".join(check_signals) or str(decision.get("summary", ""))
                                review = create_review(
                                    workspace_id=str(user_id),
                                    decision_record_id=record.id,
                                    due_at=record.review_date,
                                    expected_outcome=expected_outcome,
                                )
                    else:
                        if callable(persist_run):
                            run = persist_run(
                                user_id,
                                prompt,
                                verdict=str(decision.get("verdict", "")),
                                status=DecisionRunStatus.COMPLETED,
                                decision_type=str(decision.get("decision_type", "decision")),
                                time_horizon_days=int(decision.get("check_in_days") or self.horizon_days),
                            )
                        if callable(persist_decision) and run is not None:
                            record = persist_decision(
                                user_id,
                                decision_run_id=run.id,
                                title=str(decision.get("title", "Decision")),
                                decision_type=str(decision.get("decision_type", "decision")),
                                decision_summary=str(decision.get("summary", "")),
                                chosen_option=str(decision.get("verdict", "")),
                                rejected_options=_normalize_lines(decision.get("do_not_do")),
                                why="\n".join(why_lines),
                                risks="\n".join(risk_lines),
                                expected_signals=check_signals,
                                linked_pattern_names=[pattern.name for pattern in detected_patterns],
                                time_horizon_days=int(decision.get("check_in_days") or self.horizon_days),
                                confidence=float(decision.get("confidence") or 0.0),
                            )
                        if callable(create_review) and record is not None:
                            expected_outcome = "; ".join(check_signals) or str(decision.get("summary", ""))
                            review = create_review(
                                workspace_id=str(user_id),
                                decision_record_id=record.id,
                                due_at=record.review_date,
                                expected_outcome=expected_outcome,
                            )
                except Exception as exc:
                    return {"error": f"Failed to persist decision: {exc}", "processed_entries": 0}

                if callable(persist_pattern):
                    for pattern in detected_patterns:
                        try:
                            persist_pattern(
                                user_id,
                                name=pattern.name,
                                category=pattern.category,
                                description=pattern.description,
                                evidence=pattern.evidence,
                                confidence=pattern.confidence,
                                status=pattern.status,
                            )
                        except Exception:
                            logger.warning("Pattern persistence failed", exc_info=True)
        finally:
            if created_store and isinstance(store, DecisionStore):
                store.close()

        if run is not None:
            decision["trace_run_id"] = run.id
        if review is not None:
            decision["review_id"] = review.id
        if detected_patterns:
            decision["patterns"] = [pattern.description for pattern in detected_patterns[:2]]

        return {
            "report": format_decision_html(decision),
            "decision": decision,
            "processed_entries": 1,
        }
