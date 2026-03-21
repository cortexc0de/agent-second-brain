"""Decision-focused service for Founder Decision Partner V1."""

from __future__ import annotations

import html
from pathlib import Path
from typing import Any

from d_brain.services.decision_models import DecisionRunStatus
from d_brain.services.decision_store import DecisionStore
from d_brain.services.pattern_detector import detect_patterns
from d_brain.services.processor import ClaudeProcessor

DEFAULT_DECISION_HORIZON_DAYS = 14


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

    return "\n".join(parts)


class DecisionService:
    """High-level service for decision support requests."""

    def __init__(
        self,
        vault_path: str | Path,
        todoist_api_key: str = "",
        *,
        horizon_days: int = DEFAULT_DECISION_HORIZON_DAYS,
        processor: ClaudeProcessor | None = None,
        store: Any | None = None,
        store_path: str | Path | None = None,
    ) -> None:
        self.processor = processor or ClaudeProcessor(vault_path, todoist_api_key)
        self.horizon_days = horizon_days
        self.store = store
        self.store_path = store_path

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

        try:
            recent_records = store.list_records(str(user_id), limit=5) if store is not None else []
            existing_patterns = store.list_patterns(str(user_id), limit=10) if store is not None else []
            detected_patterns = detect_patterns(prompt, recent_records, existing_patterns)

            if store is not None:
                create_review = getattr(store, "create_review", None)
                persist_pattern = getattr(store, "persist_pattern", None)
                persist_run = getattr(store, "persist_run", None)
                persist_decision = getattr(store, "persist_decision", None)

                why_lines = _normalize_lines(decision.get("why"))
                risk_lines = _normalize_lines(decision.get("risks"))
                check_signals = _normalize_lines(decision.get("check_in_signals"))
                run = None
                record = None

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
                        time_horizon_days=int(decision.get("check_in_days") or self.horizon_days),
                        confidence=float(decision.get("confidence") or 0.0),
                    )
                if callable(create_review) and record is not None:
                    expected_outcome = "; ".join(check_signals) or str(decision.get("summary", ""))
                    create_review(
                        workspace_id=str(user_id),
                        decision_record_id=record.id,
                        due_at=record.review_date,
                        expected_outcome=expected_outcome,
                    )
                if callable(persist_pattern):
                    for pattern in detected_patterns:
                        persist_pattern(
                            user_id,
                            name=pattern.name,
                            category=pattern.category,
                            description=pattern.description,
                            evidence=pattern.evidence,
                            confidence=pattern.confidence,
                            status=pattern.status,
                        )
        finally:
            if created_store and isinstance(store, DecisionStore):
                store.close()

        if detected_patterns:
            decision["patterns"] = [pattern.description for pattern in detected_patterns[:2]]

        return {
            "report": format_decision_html(decision),
            "decision": decision,
            "processed_entries": 1,
        }
