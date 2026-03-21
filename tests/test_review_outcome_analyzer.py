from __future__ import annotations

import unittest

from d_brain.services.review_outcome_analyzer import (
    ReviewOutcomeStatus,
    analyze_review_outcome,
)


class ReviewOutcomeAnalyzerTests(unittest.TestCase):
    def test_marks_review_confirmed_when_expected_signal_is_met(self) -> None:
        analysis = analyze_review_outcome(
            expected_outcome="рост активаций; меньше drop-off",
            actual_outcome="Активации выросли, drop-off снизился, фокус подтвердился.",
        )

        self.assertEqual(analysis.status, ReviewOutcomeStatus.CONFIRMED)
        self.assertFalse(analysis.needs_follow_up)
        self.assertEqual(
            analysis.matched_signals,
            ["рост активаций", "меньше drop-off"],
        )
        self.assertEqual(analysis.missed_signals, [])
        self.assertIn("confirmed", analysis.assessment)

    def test_marks_review_invalidated_when_expected_signal_is_missed(self) -> None:
        analysis = analyze_review_outcome(
            expected_outcome="рост активаций",
            actual_outcome="Активации не выросли, решение не сработало.",
        )

        self.assertEqual(analysis.status, ReviewOutcomeStatus.INVALIDATED)
        self.assertTrue(analysis.needs_follow_up)
        self.assertEqual(analysis.matched_signals, [])
        self.assertEqual(analysis.missed_signals, ["рост активаций"])
        self.assertIn("invalidated", analysis.assessment)

    def test_marks_review_partial_when_signals_are_mixed(self) -> None:
        analysis = analyze_review_outcome(
            expected_outcome="рост активаций; 3 разговора с ICP",
            actual_outcome="Три разговора с ICP провёл, но активации пока не выросли.",
        )

        self.assertEqual(analysis.status, ReviewOutcomeStatus.PARTIAL)
        self.assertTrue(analysis.needs_follow_up)
        self.assertEqual(analysis.matched_signals, ["3 разговора с ICP"])
        self.assertEqual(analysis.missed_signals, ["рост активаций"])
        self.assertIn("partial", analysis.assessment)

    def test_marks_review_inconclusive_when_outcome_is_too_vague(self) -> None:
        analysis = analyze_review_outcome(
            expected_outcome="рост активаций",
            actual_outcome="Пока непонятно, данных мало.",
        )

        self.assertEqual(analysis.status, ReviewOutcomeStatus.INCONCLUSIVE)
        self.assertTrue(analysis.needs_follow_up)
        self.assertEqual(analysis.matched_signals, [])
        self.assertEqual(analysis.missed_signals, ["рост активаций"])
        self.assertIn("inconclusive", analysis.assessment)


if __name__ == "__main__":
    unittest.main()
