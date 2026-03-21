"""Deterministic analyzer for review outcomes."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum


class ReviewOutcomeStatus(str, Enum):
    """Outcome classification for a completed review."""

    CONFIRMED = "confirmed"
    PARTIAL = "partial"
    INVALIDATED = "invalidated"
    INCONCLUSIVE = "inconclusive"


@dataclass(slots=True)
class ReviewOutcomeAnalysis:
    """Structured deterministic result for a review outcome."""

    status: ReviewOutcomeStatus
    matched_signals: list[str]
    missed_signals: list[str]
    needs_follow_up: bool
    assessment: str


_SPLIT_PATTERN = re.compile(r"[;\n]+")
_WORD_PATTERN = re.compile(r"[a-zа-я0-9-]+", re.IGNORECASE)
_STOP_WORDS = {
    "и",
    "с",
    "на",
    "по",
    "в",
    "во",
    "к",
    "но",
    "или",
    "a",
    "an",
    "the",
    "and",
    "or",
    "to",
    "of",
    "for",
}
_POSITIVE_MARKERS = (
    "вырос",
    "выросл",
    "сниз",
    "уменьш",
    "подтверд",
    "получ",
    "сдел",
    "провел",
    "провел",
    "done",
    "grew",
    "improv",
    "reduc",
    "confirm",
    "achiev",
    "complet",
)
_NEGATIVE_MARKERS = (
    "не ",
    "нет ",
    "нет,",
    "нет.",
    "не вырос",
    "не выросл",
    "не сниз",
    "не сработ",
    "не подтверд",
    "не получ",
    "не случ",
    "без результат",
    "failed",
    "didn't",
    "didnt",
    "no ",
    "not ",
)
_VAGUE_MARKERS = (
    "непонят",
    "данных мало",
    "пока рано",
    "пока сложно",
    "пока неясно",
    "unclear",
    "too early",
    "not enough data",
)


def analyze_review_outcome(expected_outcome: str, actual_outcome: str) -> ReviewOutcomeAnalysis:
    """Analyze completed review text against expected outcome signals."""
    signals = _split_signals(expected_outcome)
    outcome_text = (actual_outcome or "").strip()
    overall_polarity = _classify_clause(outcome_text)

    if not outcome_text:
        return _build_analysis(
            status=ReviewOutcomeStatus.INCONCLUSIVE,
            matched_signals=[],
            missed_signals=signals,
            needs_follow_up=True,
        )

    clauses = _split_clauses(outcome_text)
    matched_signals: list[str] = []
    missed_signals: list[str] = []

    for signal in signals:
        signal_tokens = _meaningful_tokens(signal)
        best_clause = _select_best_clause(signal_tokens, clauses)
        clause_polarity = _classify_clause(best_clause)

        if best_clause and clause_polarity == "positive":
            matched_signals.append(signal)
        elif best_clause and clause_polarity == "negative":
            missed_signals.append(signal)
        elif overall_polarity == "positive":
            matched_signals.append(signal)
        elif _is_vague(outcome_text):
            missed_signals.append(signal)
        elif overall_polarity == "negative":
            missed_signals.append(signal)
        else:
            missed_signals.append(signal)

    if matched_signals and not missed_signals:
        return _build_analysis(
            status=ReviewOutcomeStatus.CONFIRMED,
            matched_signals=matched_signals,
            missed_signals=[],
            needs_follow_up=False,
        )

    if matched_signals and missed_signals:
        return _build_analysis(
            status=ReviewOutcomeStatus.PARTIAL,
            matched_signals=matched_signals,
            missed_signals=missed_signals,
            needs_follow_up=True,
        )

    if _is_vague(outcome_text):
        return _build_analysis(
            status=ReviewOutcomeStatus.INCONCLUSIVE,
            matched_signals=[],
            missed_signals=missed_signals,
            needs_follow_up=True,
        )

    return _build_analysis(
        status=ReviewOutcomeStatus.INVALIDATED,
        matched_signals=[],
        missed_signals=missed_signals,
        needs_follow_up=True,
    )


def _build_analysis(
    *,
    status: ReviewOutcomeStatus,
    matched_signals: list[str],
    missed_signals: list[str],
    needs_follow_up: bool,
) -> ReviewOutcomeAnalysis:
    total = len(matched_signals) + len(missed_signals)
    if status is ReviewOutcomeStatus.CONFIRMED:
        assessment = f"Outcome confirmed: {len(matched_signals)}/{total} expected signals observed."
    elif status is ReviewOutcomeStatus.PARTIAL:
        assessment = f"Outcome partial: {len(matched_signals)}/{total} expected signals observed; follow-up required."
    elif status is ReviewOutcomeStatus.INVALIDATED:
        assessment = f"Outcome invalidated: {len(matched_signals)}/{total} expected signals observed."
    else:
        assessment = "Outcome inconclusive: evidence is too weak to validate the decision."

    return ReviewOutcomeAnalysis(
        status=status,
        matched_signals=matched_signals,
        missed_signals=missed_signals,
        needs_follow_up=needs_follow_up,
        assessment=assessment,
    )


def _split_signals(expected_outcome: str) -> list[str]:
    cleaned = (expected_outcome or "").strip()
    if not cleaned:
        return []
    signals = [part.strip(" •-\t") for part in _SPLIT_PATTERN.split(cleaned)]
    return [signal for signal in signals if signal]


def _split_clauses(text: str) -> list[str]:
    chunks = re.split(r"[,.!?;]+|\s+но\s+|\s+but\s+", text)
    return [chunk.strip() for chunk in chunks if chunk.strip()]


def _select_best_clause(signal_tokens: set[str], clauses: list[str]) -> str:
    best_clause = ""
    best_score = 0
    for clause in clauses:
        clause_tokens = _meaningful_tokens(clause)
        score = _token_overlap_score(signal_tokens, clause_tokens)
        if score > best_score:
            best_score = score
            best_clause = clause
    return best_clause


def _token_overlap_score(left: set[str], right: set[str]) -> int:
    score = 0
    used_right: set[str] = set()
    for left_token in left:
        for right_token in right:
            if right_token in used_right:
                continue
            if _tokens_match(left_token, right_token):
                score += 1
                used_right.add(right_token)
                break
    return score


def _tokens_match(left: str, right: str) -> bool:
    return left == right or left.startswith(right) or right.startswith(left)


def _classify_clause(text: str) -> str:
    normalized = _normalize_text(text)
    negative_hits = sum(marker in normalized for marker in _NEGATIVE_MARKERS)
    positive_hits = sum(marker in normalized for marker in _POSITIVE_MARKERS)

    if negative_hits and not positive_hits:
        return "negative"
    if positive_hits and not negative_hits:
        return "positive"
    if negative_hits and positive_hits:
        return "negative"
    return "neutral"


def _is_vague(text: str) -> bool:
    normalized = _normalize_text(text)
    return any(marker in normalized for marker in _VAGUE_MARKERS)


def _meaningful_tokens(text: str) -> set[str]:
    stems: set[str] = set()
    for raw_token in _WORD_PATTERN.findall(_normalize_text(text)):
        if raw_token in _STOP_WORDS:
            continue
        stem = _stem_token(raw_token)
        if len(stem) >= 3:
            stems.add(stem)
    return stems


def _normalize_text(text: str) -> str:
    return f" {text.casefold().replace('ё', 'е')} "


def _stem_token(token: str) -> str:
    value = token.casefold().replace("ё", "е")
    if value.isdigit():
        return value

    for suffix in (
        "иями",
        "ями",
        "ами",
        "иях",
        "ого",
        "ему",
        "ыми",
        "ими",
        "ий",
        "ый",
        "ой",
        "ая",
        "яя",
        "ое",
        "ее",
        "ые",
        "ие",
        "ов",
        "ев",
        "ом",
        "ем",
        "ах",
        "ях",
        "ам",
        "ям",
        "а",
        "я",
        "ы",
        "и",
        "е",
        "о",
        "у",
        "ing",
        "ed",
        "es",
        "s",
    ):
        if len(value) > len(suffix) + 2 and value.endswith(suffix):
            return value[: -len(suffix)]
    return value
