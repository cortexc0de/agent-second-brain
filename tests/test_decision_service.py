from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from d_brain.services.decision_service import DecisionService, format_decision_html
from d_brain.services.decision_store import DecisionStore


class FakeProcessor:
    def __init__(self, decision: dict[str, object]) -> None:
        self.decision = decision
        self.calls: list[tuple[str, int, int]] = []

    def execute_decision(self, prompt: str, user_id: int, horizon_days: int) -> dict[str, object]:
        self.calls.append((prompt, user_id, horizon_days))
        return {"decision": self.decision, "processed_entries": 1}


class DecisionServiceTests(unittest.TestCase):
    def test_format_decision_html_renders_required_sections(self) -> None:
        rendered = format_decision_html(
            {
                "verdict": "Делай X",
                "why": ["есть сигнал", "меньше распыления"],
                "do_not_do": ["не делай Y"],
                "risks": ["сигнал может быть ложным"],
                "check_in_days": 14,
                "check_in_signals": ["2 созвона", "1 оплата"],
                "counter_argument": "если сигнал не повторится",
            }
        )

        self.assertIn("<b>Вердикт:</b> Делай X", rendered)
        self.assertIn("<b>Почему:</b>", rendered)
        self.assertIn("<b>Не делай:</b>", rendered)
        self.assertIn("<b>Риски:</b>", rendered)
        self.assertIn("<b>Что проверить через 14 дней:</b>", rendered)
        self.assertIn("<b>Что может это опровергнуть:</b>", rendered)

    def test_decide_persists_run_record_and_review(self) -> None:
        fake_processor = FakeProcessor(
            {
                "title": "Focus on onboarding",
                "decision_type": "prioritize",
                "summary": "Freeze experiments and focus on onboarding.",
                "verdict": "Сфокусируйся на onboarding",
                "why": ["есть signal", "меньше распыления"],
                "do_not_do": ["не трогай новый feature"],
                "risks": ["сигнал может быть шумом"],
                "check_in_days": 14,
                "check_in_signals": ["рост активаций", "меньше drop-off"],
                "counter_argument": "если активации не вырастут",
                "confidence": 0.82,
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decision-store.sqlite3"
            with DecisionStore(db_path) as store:
                service = DecisionService(
                    vault_path=tmpdir,
                    processor=fake_processor,
                    store=store,
                )

                result = service.decide(
                    "У меня три направления, не понимаю, что оставить",
                    user_id=42,
                )

                self.assertNotIn("error", result)
                self.assertIn("Сфокусируйся на onboarding", result["report"])
                self.assertEqual(fake_processor.calls[0][2], 14)
                self.assertIn("<b>Что у тебя повторяется:</b>", result["report"])

                runs = store.list_recent(42)
                self.assertEqual(len(runs), 1)
                self.assertEqual(runs[0].final_verdict, "Сфокусируйся на onboarding")

                records = store.list_records("42")
                self.assertEqual(len(records), 1)
                self.assertEqual(records[0].chosen_option, "Сфокусируйся на onboarding")
                self.assertEqual(records[0].expected_signals, ["рост активаций", "меньше drop-off"])

                reviews = store.list_reviews("42")
                self.assertEqual(len(reviews), 1)
                self.assertEqual(reviews[0].expected_outcome, "рост активаций; меньше drop-off")

                patterns = store.list_patterns("42")
                self.assertGreaterEqual(len(patterns), 1)
                self.assertEqual(patterns[0].status.value, "active")


if __name__ == "__main__":
    unittest.main()
