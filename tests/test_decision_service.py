from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import d_brain.services.decision_service as decision_service_module
from d_brain.services.decision_models import DecisionOutcomeStatus
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
                "trace_run_id": 7,
            }
        )

        self.assertIn("<b>Вердикт:</b> Делай X", rendered)
        self.assertIn("<b>Почему:</b>", rendered)
        self.assertIn("<b>Не делай:</b>", rendered)
        self.assertIn("<b>Риски:</b>", rendered)
        self.assertIn("<b>Что проверить через 14 дней:</b>", rendered)
        self.assertIn("<b>Что может это опровергнуть:</b>", rendered)
        self.assertIn("/decide_trace 7", rendered)

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
                self.assertIn("/decide_trace 1", result["report"])
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

    def test_render_decision_trace_shows_run_record_patterns_and_review(self) -> None:
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

                run_id = result["decision"]["trace_run_id"]
                rendered = service.render_decision_trace(42, run_id)

                self.assertIn("Decision Trace", rendered)
                self.assertIn(f"<code>{run_id}</code>", rendered)
                self.assertIn("У меня три направления", rendered)
                self.assertIn("Сфокусируйся на onboarding", rendered)
                self.assertIn("focus_fragmentation", rendered)
                self.assertIn("/review_trace", rendered)
                self.assertIn("/review_done 1", rendered)

    def test_render_decision_trace_rejects_foreign_run(self) -> None:
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

                with self.assertRaises(RuntimeError):
                    service.render_decision_trace(7, result["decision"]["trace_run_id"])

    def test_render_recent_decisions_shows_latest_records_with_trace_and_review(self) -> None:
        fake_processor = FakeProcessor(self._decision_payload())

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decision-store.sqlite3"
            with DecisionStore(db_path) as store:
                service = DecisionService(
                    vault_path=tmpdir,
                    processor=fake_processor,
                    store=store,
                )

                first = service.decide("Первый запрос про фокус", user_id=42)
                second = service.decide("Второй запрос про приоритет", user_id=42)

                self.assertNotIn("error", first)
                self.assertNotIn("error", second)

                rendered = service.render_recent_decisions(42)

                self.assertIn("Последние решения", rendered)
                self.assertIn("Второй запрос про приоритет", rendered)
                self.assertIn("/decide_trace 2", rendered)
                self.assertIn("/review_trace 2", rendered)
                self.assertIn("Без итога", rendered)

    def test_render_recent_decisions_prioritizes_follow_up_outcomes(self) -> None:
        fake_processor = FakeProcessor(self._decision_payload())

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decision-store.sqlite3"
            with DecisionStore(db_path) as store:
                service = DecisionService(
                    vault_path=tmpdir,
                    processor=fake_processor,
                    store=store,
                )

                first = service.decide("Первый запрос про фокус", user_id=42)
                second = service.decide("Второй запрос про приоритет", user_id=42)

                self.assertNotIn("error", first)
                self.assertNotIn("error", second)

                store.update_record_outcome(
                    1,
                    outcome_status=DecisionOutcomeStatus.INVALIDATED,
                    outcome_summary="Решение не подтвердилось",
                    needs_follow_up=True,
                )

                rendered = service.render_recent_decisions(42)

                self.assertIn("Требует внимания", rendered)
                self.assertIn("Не подтвердилось", rendered)
                self.assertLess(
                    rendered.index("Первый запрос про фокус"),
                    rendered.index("Второй запрос про приоритет"),
                )

    def test_render_recent_decisions_shows_friendly_outcome_labels(self) -> None:
        fake_processor = FakeProcessor(self._decision_payload())

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decision-store.sqlite3"
            with DecisionStore(db_path) as store:
                service = DecisionService(
                    vault_path=tmpdir,
                    processor=fake_processor,
                    store=store,
                )

                result = service.decide("Запрос про фокус", user_id=42)
                self.assertNotIn("error", result)
                store.update_record_outcome(
                    1,
                    outcome_status=DecisionOutcomeStatus.CONFIRMED,
                    outcome_summary="Решение подтвердилось",
                    needs_follow_up=False,
                )

                rendered = service.render_recent_decisions(42)

                self.assertIn("Подтвердилось", rendered)
                self.assertNotIn("<b>Outcome:</b> confirmed", rendered)

    def test_render_recent_decisions_prioritizes_overdue_reviews(self) -> None:
        fake_processor = FakeProcessor(self._decision_payload())
        current_time = datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc)

        def clock() -> datetime:
            return current_time

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decision-store.sqlite3"
            with DecisionStore(db_path, clock=clock) as store:
                service = DecisionService(
                    vault_path=tmpdir,
                    processor=fake_processor,
                    store=store,
                    clock=clock,
                )

                first = service.decide("Старое решение", user_id=42)
                current_time = current_time + timedelta(days=10)
                second = service.decide("Новое решение", user_id=42)

                self.assertNotIn("error", first)
                self.assertNotIn("error", second)

                current_time = current_time + timedelta(days=5)
                rendered = service.render_recent_decisions(42)

                self.assertIn("Review просрочен", rendered)
                self.assertLess(rendered.index("Старое решение"), rendered.index("Новое решение"))

    def test_render_recent_decisions_marks_review_due_soon(self) -> None:
        fake_processor = FakeProcessor(self._decision_payload())
        current_time = datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc)

        def clock() -> datetime:
            return current_time

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decision-store.sqlite3"
            with DecisionStore(db_path, clock=clock) as store:
                service = DecisionService(
                    vault_path=tmpdir,
                    processor=fake_processor,
                    store=store,
                    clock=clock,
                )

                result = service.decide("Скоро review", user_id=42)
                self.assertNotIn("error", result)

                current_time = current_time + timedelta(days=13)
                rendered = service.render_recent_decisions(42)

                self.assertIn("Review скоро", rendered)
                self.assertIn("Следующее действие", rendered)
                self.assertIn("Подготовь review", rendered)
                self.assertIn("/review_trace 1", rendered)

    def test_render_recent_decisions_suggests_closing_overdue_review(self) -> None:
        fake_processor = FakeProcessor(self._decision_payload())
        current_time = datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc)

        def clock() -> datetime:
            return current_time

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decision-store.sqlite3"
            with DecisionStore(db_path, clock=clock) as store:
                service = DecisionService(
                    vault_path=tmpdir,
                    processor=fake_processor,
                    store=store,
                    clock=clock,
                )

                result = service.decide("Просроченный review", user_id=42)
                self.assertNotIn("error", result)

                current_time = current_time + timedelta(days=15)
                rendered = service.render_recent_decisions(42)

                self.assertIn("Следующее действие", rendered)
                self.assertIn("Закрой review", rendered)
                self.assertIn("/review_done 1", rendered)

    def test_render_recent_decisions_suggests_revisiting_follow_up_decision(self) -> None:
        fake_processor = FakeProcessor(self._decision_payload())

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decision-store.sqlite3"
            with DecisionStore(db_path) as store:
                service = DecisionService(
                    vault_path=tmpdir,
                    processor=fake_processor,
                    store=store,
                )

                result = service.decide("Нужен follow-up", user_id=42)
                self.assertNotIn("error", result)
                store.update_record_outcome(
                    1,
                    outcome_status=DecisionOutcomeStatus.INVALIDATED,
                    outcome_summary="Нужно пересмотреть",
                    needs_follow_up=True,
                )

                rendered = service.render_recent_decisions(42)

                self.assertIn("Следующее действие", rendered)
                self.assertIn("Пересмотри решение", rendered)
                self.assertIn("/decide_trace 1", rendered)

    def test_render_recent_decisions_suggests_observing_confirmed_decision(self) -> None:
        fake_processor = FakeProcessor(self._decision_payload())

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decision-store.sqlite3"
            with DecisionStore(db_path) as store:
                service = DecisionService(
                    vault_path=tmpdir,
                    processor=fake_processor,
                    store=store,
                )

                result = service.decide("Подтверждённое решение", user_id=42)
                self.assertNotIn("error", result)
                store.update_record_outcome(
                    1,
                    outcome_status=DecisionOutcomeStatus.CONFIRMED,
                    outcome_summary="Решение подтвердилось",
                    needs_follow_up=False,
                )

                rendered = service.render_recent_decisions(42)

                self.assertIn("Следующее действие", rendered)
                self.assertIn("Наблюдай", rendered)

    def test_render_recent_decisions_groups_attention_and_stable_sections(self) -> None:
        fake_processor = FakeProcessor(self._decision_payload())
        current_time = datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc)

        def clock() -> datetime:
            return current_time

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decision-store.sqlite3"
            with DecisionStore(db_path, clock=clock) as store:
                service = DecisionService(
                    vault_path=tmpdir,
                    processor=fake_processor,
                    store=store,
                    clock=clock,
                )

                first = service.decide("Просроченный attention", user_id=42)
                current_time = current_time + timedelta(days=10)
                second = service.decide("Стабильное решение", user_id=42)

                self.assertNotIn("error", first)
                self.assertNotIn("error", second)

                store.update_record_outcome(
                    2,
                    outcome_status=DecisionOutcomeStatus.CONFIRMED,
                    outcome_summary="Решение подтвердилось",
                    needs_follow_up=False,
                )

                current_time = current_time + timedelta(days=5)
                rendered = service.render_recent_decisions(42)

                self.assertIn("Needs Attention", rendered)
                self.assertIn("Stable", rendered)
                self.assertLess(rendered.index("Needs Attention"), rendered.index("Stable"))
                self.assertLess(rendered.index("Просроченный attention"), rendered.index("Стабильное решение"))

    def test_render_recent_decisions_groups_due_soon_section(self) -> None:
        fake_processor = FakeProcessor(self._decision_payload())
        current_time = datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc)

        def clock() -> datetime:
            return current_time

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decision-store.sqlite3"
            with DecisionStore(db_path, clock=clock) as store:
                service = DecisionService(
                    vault_path=tmpdir,
                    processor=fake_processor,
                    store=store,
                    clock=clock,
                )

                result = service.decide("Скоро review секция", user_id=42)
                self.assertNotIn("error", result)

                current_time = current_time + timedelta(days=13)
                rendered = service.render_recent_decisions(42)

                self.assertIn("Due Soon", rendered)
                self.assertIn("Скоро review секция", rendered)

    def test_render_recent_decisions_shows_section_counters_in_header(self) -> None:
        fake_processor = FakeProcessor(self._decision_payload())
        current_time = datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc)

        def clock() -> datetime:
            return current_time

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decision-store.sqlite3"
            with DecisionStore(db_path, clock=clock) as store:
                service = DecisionService(
                    vault_path=tmpdir,
                    processor=fake_processor,
                    store=store,
                    clock=clock,
                )

                attention = service.decide("Attention решение", user_id=42)
                current_time = current_time + timedelta(days=12)
                due_soon = service.decide("Due soon решение", user_id=42)
                current_time = current_time + timedelta(days=5)
                stable = service.decide("Stable решение", user_id=42)

                self.assertNotIn("error", attention)
                self.assertNotIn("error", due_soon)
                self.assertNotIn("error", stable)

                with store._conn:
                    store._conn.execute(
                        "UPDATE review_records SET due_at = ? WHERE id = ?",
                        (store._serialize_datetime(current_time - timedelta(days=1)), 1),
                    )
                    store._conn.execute(
                        "UPDATE review_records SET due_at = ? WHERE id = ?",
                        (store._serialize_datetime(current_time + timedelta(days=1)), 2),
                    )
                    store._conn.execute(
                        "UPDATE review_records SET due_at = ? WHERE id = ?",
                        (store._serialize_datetime(current_time + timedelta(days=10)), 3),
                    )

                store.update_record_outcome(
                    3,
                    outcome_status=DecisionOutcomeStatus.CONFIRMED,
                    outcome_summary="Решение подтвердилось",
                    needs_follow_up=False,
                )

                rendered = service.render_recent_decisions(42)

                self.assertIn("Needs Attention: 1", rendered)
                self.assertIn("Due Soon: 1", rendered)
                self.assertIn("Stable: 1", rendered)

    def test_render_recent_decisions_shows_hidden_count_footer_when_limited(self) -> None:
        fake_processor = FakeProcessor(self._decision_payload())

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decision-store.sqlite3"
            with DecisionStore(db_path) as store:
                service = DecisionService(
                    vault_path=tmpdir,
                    processor=fake_processor,
                    store=store,
                )

                first = service.decide("Решение 1", user_id=42)
                second = service.decide("Решение 2", user_id=42)
                third = service.decide("Решение 3", user_id=42)

                self.assertNotIn("error", first)
                self.assertNotIn("error", second)
                self.assertNotIn("error", third)

                rendered = service.render_recent_decisions(42, limit=2)

                self.assertIn("ещё 1 решение вне среза", rendered)
                self.assertIn("/decisions [limit]", rendered)
                self.assertNotIn("Решение 1", rendered)

    def test_render_recent_decisions_omits_hidden_count_footer_when_not_truncated(self) -> None:
        fake_processor = FakeProcessor(self._decision_payload())

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decision-store.sqlite3"
            with DecisionStore(db_path) as store:
                service = DecisionService(
                    vault_path=tmpdir,
                    processor=fake_processor,
                    store=store,
                )

                first = service.decide("Решение A", user_id=42)
                second = service.decide("Решение B", user_id=42)

                self.assertNotIn("error", first)
                self.assertNotIn("error", second)

                rendered = service.render_recent_decisions(42, limit=5)

                self.assertNotIn("вне среза", rendered)

    def test_render_recent_decisions_shows_empty_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decision-store.sqlite3"
            with DecisionStore(db_path) as store:
                service = DecisionService(
                    vault_path=tmpdir,
                    processor=FakeProcessor(self._decision_payload()),
                    store=store,
                )

                rendered = service.render_recent_decisions(42)

                self.assertIn("Пока нет сохранённых решений", rendered)

    def test_render_recent_decisions_rejects_foreign_workspace(self) -> None:
        fake_processor = FakeProcessor(self._decision_payload())

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "decision-store.sqlite3"
            with DecisionStore(db_path) as store:
                service = DecisionService(
                    vault_path=tmpdir,
                    processor=fake_processor,
                    store=store,
                )

                result = service.decide("Чужое решение", user_id=42)

                self.assertNotIn("error", result)
                rendered = service.render_recent_decisions(7)
                self.assertIn("Пока нет сохранённых решений", rendered)


if __name__ == "__main__":
    unittest.main()
