"""Deterministic pattern extraction for decision support."""

from __future__ import annotations

from dataclasses import dataclass

from d_brain.services.decision_models import DecisionRecord, PatternRecord, PatternStatus


@dataclass(slots=True)
class PatternCandidate:
    """Candidate pattern derived from the current prompt and recent history."""

    name: str
    category: str
    description: str
    evidence: list[str]
    confidence: float
    status: PatternStatus = PatternStatus.ACTIVE


MULTI_DIRECTION_MARKERS = (
    "несколько",
    "много",
    "хаос",
    "распыл",
    "вариант",
    "направлен",
    "оставить",
    "выбрать",
)

ANALYSIS_MARKERS = (
    "не понимаю",
    "не знаю",
    "сомнева",
    "никак не могу",
    "думаю",
)


def _contains_any(text: str, markers: tuple[str, ...]) -> bool:
    return any(marker in text for marker in markers)


def detect_patterns(
    request_text: str,
    recent_records: list[DecisionRecord] | None = None,
    existing_patterns: list[PatternRecord] | None = None,
) -> list[PatternCandidate]:
    """Extract a small set of deterministic patterns."""
    text = request_text.lower()
    recent_records = recent_records or []
    existing_patterns = existing_patterns or []
    detected: dict[str, PatternCandidate] = {}

    if _contains_any(text, MULTI_DIRECTION_MARKERS):
        detected["focus_fragmentation"] = PatternCandidate(
            name="focus_fragmentation",
            category="decision_pattern",
            description="Ты снова пытаешься удерживать слишком много направлений одновременно.",
            evidence=["В запросе есть признаки перегрузки и множественного выбора."],
            confidence=0.76,
        )

    if _contains_any(text, ANALYSIS_MARKERS):
        detected["analysis_paralysis"] = PatternCandidate(
            name="analysis_paralysis",
            category="bias",
            description="Похоже, ты близок к циклу размышления без жёсткого коммита.",
            evidence=["В формулировке есть явная неуверенность и стопор."],
            confidence=0.68,
        )

    recent_choices = [record.chosen_option.strip().lower() for record in recent_records if record.chosen_option.strip()]
    if len(recent_choices) >= 2 and len(set(recent_choices)) >= 2:
        detected["premature_pivot"] = PatternCandidate(
            name="premature_pivot",
            category="failure_loop",
            description="Недавние решения уже меняли фокус слишком рано, до окна проверки.",
            evidence=["В истории решений есть несколько разных приоритетов за короткий цикл."],
            confidence=0.74,
        )

    for pattern in existing_patterns:
        if pattern.name in detected:
            detected[pattern.name].evidence.append("Этот паттерн уже встречался раньше.")
            detected[pattern.name].confidence = max(pattern.confidence, detected[pattern.name].confidence)
            detected[pattern.name].status = pattern.status

    return list(detected.values())
