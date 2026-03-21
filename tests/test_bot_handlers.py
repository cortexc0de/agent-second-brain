from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

_MISSING = object()
_ORIGINAL_MODULES: dict[str, object] = {}

def _install_aiogram_stubs() -> None:
    for module_name in ("aiogram", "aiogram.filters", "aiogram.types"):
        _ORIGINAL_MODULES.setdefault(module_name, sys.modules.get(module_name, _MISSING))
    if "aiogram" in sys.modules:
        return

    aiogram_module = types.ModuleType("aiogram")
    filters_module = types.ModuleType("aiogram.filters")
    types_module = types.ModuleType("aiogram.types")

    class Router:
        def __init__(self, *args, **kwargs) -> None:
            self.args = args
            self.kwargs = kwargs

        def message(self, *args, **kwargs):
            def decorator(func):
                return func

            return decorator

    class Command:
        def __init__(self, *args, **kwargs) -> None:
            self.args = args
            self.kwargs = kwargs

    class CommandObject:
        def __init__(self, args: str | None = None) -> None:
            self.args = args

    class Message:
        pass

    aiogram_module.Router = Router
    filters_module.Command = Command
    filters_module.CommandObject = CommandObject
    types_module.Message = Message

    sys.modules["aiogram"] = aiogram_module
    sys.modules["aiogram.filters"] = filters_module
    sys.modules["aiogram.types"] = types_module


def _install_project_stubs() -> None:
    if "d_brain.config" not in sys.modules:
        _ORIGINAL_MODULES.setdefault("d_brain.config", _MISSING)
        config_module = types.ModuleType("d_brain.config")

        def get_settings():
            return SimpleNamespace(vault_path=Path("/tmp"), todoist_api_key="")

        config_module.get_settings = get_settings
        sys.modules["d_brain.config"] = config_module

    if "d_brain.bot.formatters" not in sys.modules:
        _ORIGINAL_MODULES.setdefault("d_brain.bot.formatters", _MISSING)
        formatters_module = types.ModuleType("d_brain.bot.formatters")
        formatters_module.format_process_report = lambda report: report.get("report", "")
        sys.modules["d_brain.bot.formatters"] = formatters_module

    if "d_brain.services.decision_service" not in sys.modules:
        _ORIGINAL_MODULES.setdefault("d_brain.services.decision_service", _MISSING)
        decision_service_module = types.ModuleType("d_brain.services.decision_service")

        class DecisionServiceError(RuntimeError):
            pass

        class DecisionService:
            def __init__(self, *args, **kwargs) -> None:
                pass

            def decide(self, prompt: str, user_id: int = 0) -> dict:
                return {"report": f"{prompt}:{user_id}"}

            def render_decision_trace(self, user_id: int, run_id: int) -> str:
                return f"decision-trace:{user_id}:{run_id}"

            def render_recent_decisions(self, user_id: int) -> str:
                return f"recent-decisions:{user_id}"

        decision_service_module.DecisionService = DecisionService
        decision_service_module.DecisionServiceError = DecisionServiceError
        sys.modules["d_brain.services.decision_service"] = decision_service_module

    if "d_brain.services.review_service" not in sys.modules:
        _ORIGINAL_MODULES.setdefault("d_brain.services.review_service", _MISSING)
        review_service_module = types.ModuleType("d_brain.services.review_service")

        class ReviewServiceError(RuntimeError):
            pass

        class ReviewService:
            def __init__(self, *args, **kwargs) -> None:
                pass

            def render_review_overview(self, user_id: int) -> str:
                return str(user_id)

            def render_review_trace(self, user_id: int, review_id: int) -> str:
                return f"trace:{user_id}:{review_id}"

            def complete_review(self, user_id: int, review_id: int, outcome: str) -> str:
                return f"{user_id}:{review_id}:{outcome}"

            def skip_review(self, user_id: int, review_id: int) -> str:
                return f"{user_id}:{review_id}"

        review_service_module.ReviewService = ReviewService
        review_service_module.ReviewServiceError = ReviewServiceError
        sys.modules["d_brain.services.review_service"] = review_service_module


def _restore_project_modules() -> None:
    for module_name, original in _ORIGINAL_MODULES.items():
        if original is _MISSING:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = original


def _load_handler_module(module_name: str, relative_path: str):
    _install_aiogram_stubs()
    _install_project_stubs()
    path = Path(__file__).resolve().parents[1] / "src" / relative_path
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


decide = _load_handler_module("test_decide_handler_module", "d_brain/bot/handlers/decide.py")
review = _load_handler_module("test_review_handler_module", "d_brain/bot/handlers/review.py")
ReviewServiceError = review.ReviewServiceError
_restore_project_modules()


class DecideHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_cmd_decide_rejects_missing_user(self) -> None:
        status_message = SimpleNamespace(edit_text=AsyncMock())
        message = SimpleNamespace(
            from_user=None,
            answer=AsyncMock(return_value=status_message),
        )
        command = SimpleNamespace(args="куда идти дальше")
        service = MagicMock()
        import asyncio
        done_task = asyncio.get_running_loop().create_future()
        done_task.set_result({"report": "should not be used"})

        with (
            patch.object(decide, "DecisionService", return_value=service),
            patch.object(decide.asyncio, "to_thread", new=lambda func, *args: done_task),
            patch.object(decide.asyncio, "create_task", return_value=done_task),
        ):
            await decide.cmd_decide(message, command)

        message.answer.assert_awaited_once()
        reply_text = message.answer.await_args.args[0]
        self.assertIn("Не удалось определить пользователя", reply_text)
        service.decide.assert_not_called()

    async def test_cmd_decide_rejects_whitespace_only_args(self) -> None:
        status_message = SimpleNamespace(edit_text=AsyncMock())
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            answer=AsyncMock(return_value=status_message),
        )
        command = SimpleNamespace(args="   ")
        service = MagicMock()
        import asyncio
        done_task = asyncio.get_running_loop().create_future()
        done_task.set_result({"report": "should not be used"})

        with (
            patch.object(decide, "DecisionService", return_value=service),
            patch.object(decide.asyncio, "to_thread", new=lambda func, *args: done_task),
            patch.object(decide.asyncio, "create_task", return_value=done_task),
        ):
            await decide.cmd_decide(message, command)

        message.answer.assert_awaited_once()
        reply_text = message.answer.await_args.args[0]
        self.assertIn("/decide твой вопрос", reply_text)
        service.decide.assert_not_called()

    async def test_cmd_decide_handles_service_exception(self) -> None:
        status_message = SimpleNamespace(edit_text=AsyncMock())
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            answer=AsyncMock(return_value=status_message),
        )
        command = SimpleNamespace(args="нормальный запрос")
        service = MagicMock()
        import asyncio

        failed_task = asyncio.get_running_loop().create_future()
        failed_task.set_exception(RuntimeError("boom"))

        with (
            patch.object(decide, "DecisionService", return_value=service),
            patch.object(decide.asyncio, "to_thread", new=lambda func, *args: failed_task),
            patch.object(decide.asyncio, "create_task", return_value=failed_task),
            patch.object(decide, "format_process_report", side_effect=lambda report: f"ERR:{report['error']}"),
            patch.object(decide.logger, "exception"),
        ):
            await decide.cmd_decide(message, command)

        status_message.edit_text.assert_awaited_once_with("ERR:boom")

    async def test_cmd_decide_trace_requires_run_id(self) -> None:
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            answer=AsyncMock(),
        )
        command = SimpleNamespace(args=None)

        await decide.cmd_decide_trace(message, command)

        message.answer.assert_awaited_once()
        reply_text = message.answer.await_args.args[0]
        self.assertIn("/decide_trace ID", reply_text)

    async def test_cmd_decide_trace_rejects_missing_user(self) -> None:
        message = SimpleNamespace(
            from_user=None,
            answer=AsyncMock(),
        )
        command = SimpleNamespace(args="7")

        await decide.cmd_decide_trace(message, command)

        message.answer.assert_awaited_once()
        reply_text = message.answer.await_args.args[0]
        self.assertIn("Не удалось определить пользователя", reply_text)

    async def test_cmd_decide_trace_renders_trace(self) -> None:
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            answer=AsyncMock(),
        )
        command = SimpleNamespace(args="7")
        service = MagicMock()
        service.render_decision_trace.return_value = "trace"

        with patch.object(decide, "_build_decision_service", return_value=service):
            await decide.cmd_decide_trace(message, command)

        service.render_decision_trace.assert_called_once_with(42, 7)
        message.answer.assert_awaited_once_with("trace")

    async def test_cmd_decide_trace_handles_service_error(self) -> None:
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            answer=AsyncMock(),
        )
        command = SimpleNamespace(args="7")
        service = MagicMock()
        service.render_decision_trace.side_effect = RuntimeError("boom")

        with patch.object(decide, "_build_decision_service", return_value=service):
            await decide.cmd_decide_trace(message, command)

        message.answer.assert_awaited_once()
        reply_text = message.answer.await_args.args[0]
        self.assertIn("Ошибка", reply_text)
        self.assertIn("boom", reply_text)

    async def test_cmd_decisions_rejects_missing_user(self) -> None:
        message = SimpleNamespace(
            from_user=None,
            answer=AsyncMock(),
        )

        await decide.cmd_decisions(message)

        message.answer.assert_awaited_once()
        reply_text = message.answer.await_args.args[0]
        self.assertIn("Не удалось определить пользователя", reply_text)

    async def test_cmd_decisions_renders_recent_decisions(self) -> None:
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            answer=AsyncMock(),
        )
        service = MagicMock()
        service.render_recent_decisions.return_value = "recent"

        with patch.object(decide, "_build_decision_service", return_value=service):
            await decide.cmd_decisions(message)

        service.render_recent_decisions.assert_called_once_with(42)
        message.answer.assert_awaited_once_with("recent")

    async def test_cmd_decisions_handles_service_error(self) -> None:
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            answer=AsyncMock(),
        )
        service = MagicMock()
        service.render_recent_decisions.side_effect = RuntimeError("boom")

        with patch.object(decide, "_build_decision_service", return_value=service):
            await decide.cmd_decisions(message)

        message.answer.assert_awaited_once()
        reply_text = message.answer.await_args.args[0]
        self.assertIn("Ошибка", reply_text)
        self.assertIn("boom", reply_text)


class ReviewHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_cmd_review_rejects_missing_user(self) -> None:
        message = SimpleNamespace(
            from_user=None,
            answer=AsyncMock(),
        )

        await review.cmd_review(message)

        message.answer.assert_awaited_once()
        reply_text = message.answer.await_args.args[0]
        self.assertIn("Не удалось определить пользователя", reply_text)

    async def test_cmd_review_renders_overview(self) -> None:
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            answer=AsyncMock(),
        )
        service = MagicMock()
        service.render_review_overview.return_value = "overview"

        with patch.object(review, "_build_review_service", return_value=service):
            await review.cmd_review(message)

        service.render_review_overview.assert_called_once_with(42)
        message.answer.assert_awaited_once_with("overview")

    async def test_cmd_review_handles_service_error(self) -> None:
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            answer=AsyncMock(),
        )
        service = MagicMock()
        service.render_review_overview.side_effect = ReviewServiceError("boom")

        with patch.object(review, "_build_review_service", return_value=service):
            await review.cmd_review(message)

        message.answer.assert_awaited_once()
        reply_text = message.answer.await_args.args[0]
        self.assertIn("Ошибка", reply_text)
        self.assertIn("boom", reply_text)

    async def test_cmd_review_trace_requires_review_id(self) -> None:
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            answer=AsyncMock(),
        )
        command = SimpleNamespace(args=None)

        await review.cmd_review_trace(message, command)

        message.answer.assert_awaited_once()
        reply_text = message.answer.await_args.args[0]
        self.assertIn("/review_trace ID", reply_text)

    async def test_cmd_review_trace_rejects_missing_user(self) -> None:
        message = SimpleNamespace(
            from_user=None,
            answer=AsyncMock(),
        )
        command = SimpleNamespace(args="7")

        await review.cmd_review_trace(message, command)

        message.answer.assert_awaited_once()
        reply_text = message.answer.await_args.args[0]
        self.assertIn("Не удалось определить пользователя", reply_text)

    async def test_cmd_review_trace_rejects_non_numeric_review_id(self) -> None:
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            answer=AsyncMock(),
        )
        command = SimpleNamespace(args="abc")

        await review.cmd_review_trace(message, command)

        message.answer.assert_awaited_once()
        reply_text = message.answer.await_args.args[0]
        self.assertIn("/review_trace ID", reply_text)

    async def test_cmd_review_trace_renders_trace(self) -> None:
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            answer=AsyncMock(),
        )
        command = SimpleNamespace(args="7")
        service = MagicMock()
        service.render_review_trace.return_value = "trace"

        with patch.object(review, "_build_review_service", return_value=service):
            await review.cmd_review_trace(message, command)

        service.render_review_trace.assert_called_once_with(42, 7)
        message.answer.assert_awaited_once_with("trace")

    async def test_cmd_review_trace_handles_service_error(self) -> None:
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            answer=AsyncMock(),
        )
        command = SimpleNamespace(args="7")
        service = MagicMock()
        service.render_review_trace.side_effect = ReviewServiceError("boom")

        with patch.object(review, "_build_review_service", return_value=service):
            await review.cmd_review_trace(message, command)

        message.answer.assert_awaited_once()
        reply_text = message.answer.await_args.args[0]
        self.assertIn("Ошибка", reply_text)
        self.assertIn("boom", reply_text)

    async def test_cmd_review_done_completes_review(self) -> None:
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            answer=AsyncMock(),
        )
        command = SimpleNamespace(args="7 outcome text")
        service = MagicMock()
        service.complete_review.return_value = "done"

        with patch.object(review, "_build_review_service", return_value=service):
            await review.cmd_review_done(message, command)

        service.complete_review.assert_called_once_with(42, 7, "outcome text")
        message.answer.assert_awaited_once_with("done")

    async def test_cmd_review_done_rejects_malformed_args(self) -> None:
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            answer=AsyncMock(),
        )
        command = SimpleNamespace(args="abc")

        await review.cmd_review_done(message, command)

        message.answer.assert_awaited_once()
        reply_text = message.answer.await_args.args[0]
        self.assertIn("Нужны ID и результат", reply_text)

    async def test_cmd_review_skip_completes_skip(self) -> None:
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            answer=AsyncMock(),
        )
        command = SimpleNamespace(args="7")
        service = MagicMock()
        service.skip_review.return_value = "skipped"

        with patch.object(review, "_build_review_service", return_value=service):
            await review.cmd_review_skip(message, command)

        service.skip_review.assert_called_once_with(42, 7)
        message.answer.assert_awaited_once_with("skipped")

    async def test_cmd_review_skip_handles_service_error(self) -> None:
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=42),
            answer=AsyncMock(),
        )
        command = SimpleNamespace(args="7")
        service = MagicMock()
        service.skip_review.side_effect = ReviewServiceError("forbidden")

        with patch.object(review, "_build_review_service", return_value=service):
            await review.cmd_review_skip(message, command)

        message.answer.assert_awaited_once()
        reply_text = message.answer.await_args.args[0]
        self.assertIn("Ошибка", reply_text)
        self.assertIn("forbidden", reply_text)


if __name__ == "__main__":
    unittest.main()
