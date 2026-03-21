from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import d_brain.services.decision_service as decision_service_module
from d_brain.services.decision_service import DecisionService, format_decision_html
from d_brain.services.decision_store import DecisionStore


class FakeProcessor:
    def __init__(self, decision: dict[str, object]) -> None:
        self.decision = decision
        self.calls: list[tuple[str, int, int]] = []

    def execute_decision(self, prompt: str, user_id: int, horizon_days: int) -> dict[str, object]:
        self.calls.append((prompt, user_id, horizon_days))
        return {"decision": self.decision, "processed_entries": 1}


class FailingDecisionStore(DecisionStore):
    def __init__(self, database_path: Path, *, fail_on: str) -> None:
        super().__init__(database_path)
        self.fail_on = fail_on

    def persist_decision(self, *args, **kwargs):
        if self.fail_on == "persist_decision":
            raise RuntimeError("persist_decision failed")
        return super().persist_decision(*args, **kwargs)

    def create_review(self, *args, **kwargs):
        if self.fail_on == "create_review":
            raise RuntimeError("create_review failed")
        return super().create_review(*args, **kwargs)

    def persist_pattern(self, *args, **kwargs):
        if self.fail_on == "persist_pattern":
            raise RuntimeError("persist_pattern failed")
        return super().persist_pattern(*args, **kwargs)


class DecisionServiceTests(unittest.TestCase):
    def _decision_payload(self) -> dict[str, object]:
        return {
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
        fake_processor = FakeProcessor(self._decision_payload())

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
                self.assertEqual(
                    records[0].linked_pattern_names,
                    ["focus_fragmentation", "analysis_paralysis"],
                )

                reviews = store.list_reviews("42")
                self.assertEqual(len(reviews), 1)
                self.assertEqual(reviews[0].expected_outcome, "рост активаций; меньше drop-off")

                patterns = store.list_patterns("42")
                self.assertGreaterEqual(len(patterns), 1)
                self.assertEqual(patterns[0].status.value, "active")

    def test_decide_rolls_back_run_if_decision_persistence_fails(self) -> None:
        fake_processor = FakeProcessor(self._decision_payload())

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decision-store.sqlite3"
            with FailingDecisionStore(db_path, fail_on="persist_decision") as store:
                service = DecisionService(
                    vault_path=tmpdir,
                    processor=fake_processor,
                    store=store,
                )

                result = service.decide(
                    "У меня три направления, не понимаю, что оставить",
                    user_id=42,
                )

                self.assertIn("error", result)
                self.assertIn("persist_decision failed", result["error"])
                self.assertEqual(store.list_recent(42), [])
                self.assertEqual(store.list_records("42"), [])
                self.assertEqual(store.list_reviews("42"), [])
                self.assertEqual(store.list_patterns("42"), [])

    def test_decide_rolls_back_run_and_record_if_review_persistence_fails(self) -> None:
        fake_processor = FakeProcessor(self._decision_payload())

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decision-store.sqlite3"
            with FailingDecisionStore(db_path, fail_on="create_review") as store:
                service = DecisionService(
                    vault_path=tmpdir,
                    processor=fake_processor,
                    store=store,
                )

                result = service.decide(
                    "У меня три направления, не понимаю, что оставить",
                    user_id=42,
                )

                self.assertIn("error", result)
                self.assertIn("create_review failed", result["error"])
                self.assertEqual(store.list_recent(42), [])
                self.assertEqual(store.list_records("42"), [])
                self.assertEqual(store.list_reviews("42"), [])

    def test_decide_keeps_core_commit_when_pattern_persistence_fails(self) -> None:
        fake_processor = FakeProcessor(self._decision_payload())

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decision-store.sqlite3"
            with FailingDecisionStore(db_path, fail_on="persist_pattern") as store:
                service = DecisionService(
                    vault_path=tmpdir,
                    processor=fake_processor,
                    store=store,
                )

                with patch.object(decision_service_module.logger, "warning"):
                    result = service.decide(
                        "У меня три направления, не понимаю, что оставить",
                        user_id=42,
                    )

                self.assertNotIn("error", result)
                self.assertEqual(len(store.list_recent(42)), 1)
                self.assertEqual(len(store.list_records("42")), 1)
                self.assertEqual(len(store.list_reviews("42")), 1)
                self.assertEqual(store.list_patterns("42"), [])

    def test_decide_reuses_existing_pattern_records_instead_of_creating_duplicates(self) -> None:
        fake_processor = FakeProcessor(self._decision_payload())

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decision-store.sqlite3"
            with DecisionStore(db_path) as store:
                service = DecisionService(
                    vault_path=tmpdir,
                    processor=fake_processor,
                    store=store,
                )

                first = service.decide(
                    "У меня много направлений и вариантов, не понимаю что выбрать из этого хаоса",
                    user_id=42,
                )
                self.assertNotIn("error", first)
                first_patterns = store.list_patterns("42")
                first_ids = {pattern.name: pattern.id for pattern in first_patterns}

                second = service.decide(
                    "У меня много направлений и вариантов, не понимаю что выбрать из этого хаоса",
                    user_id=42,
                )
                self.assertNotIn("error", second)

                second_patterns = store.list_patterns("42")
                self.assertEqual(len(second_patterns), len(first_patterns))
                self.assertEqual(
                    {pattern.name: pattern.id for pattern in second_patterns},
                    first_ids,
                )


if __name__ == "__main__":
    unittest.main()
