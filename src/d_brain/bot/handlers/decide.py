"""Handler for /decide command - focused decision support."""

import asyncio
import logging

from aiogram import Router
from aiogram.filters import Command, CommandObject
from aiogram.types import Message

from d_brain.bot.formatters import format_process_report
from d_brain.config import get_settings
from d_brain.services.decision_service import DecisionService, DecisionServiceError

router = Router(name="decide")
logger = logging.getLogger(__name__)


def _build_decision_service() -> DecisionService:
    settings = get_settings()
    store_path = settings.vault_path / ".decision-store.sqlite3"
    return DecisionService(
        settings.vault_path,
        settings.todoist_api_key,
        store_path=store_path,
    )


async def _require_user_id(message: Message) -> int | None:
    if message.from_user is None:
        await message.answer("❌ <b>Ошибка:</b> Не удалось определить пользователя.")
        return None
    return message.from_user.id


@router.message(Command("decide"))
async def cmd_decide(message: Message, command: CommandObject) -> None:
    """Handle /decide command for structured decision support."""
    user_id = await _require_user_id(message)
    if user_id is None:
        return

    if not command.args or not command.args.strip():
        await message.answer(
            "🎯 <b>Формат:</b> <code>/decide твой вопрос</code>\n\n"
            "Пример:\n"
            "<code>/decide у меня 5 направлений, не понимаю, что оставить на ближайшие 2 недели</code>"
        )
        return

    status_msg = await message.answer("⏳ Думаю над решением...")
    service = _build_decision_service()

    async def run_with_progress() -> dict:
        task = asyncio.create_task(
            asyncio.to_thread(service.decide, command.args, user_id)
        )

        elapsed = 0
        while not task.done():
            await asyncio.sleep(15)
            elapsed += 15
            if not task.done():
                try:
                    await status_msg.edit_text(
                        f"⏳ Думаю над решением... ({elapsed // 60}m {elapsed % 60}s)"
                    )
                except Exception:
                    logger.debug("Failed to update /decide progress message", exc_info=True)

        return await task

    try:
        report = await run_with_progress()
    except Exception as exc:
        logger.exception("Decision flow failed")
        report = {"error": str(exc)}
    formatted = format_process_report(report)

    try:
        await status_msg.edit_text(formatted)
    except Exception:
        await status_msg.edit_text(formatted, parse_mode=None)


@router.message(Command("decide_trace"))
async def cmd_decide_trace(message: Message, command: CommandObject) -> None:
    """Show trace details for a persisted decision run."""
    user_id = await _require_user_id(message)
    if user_id is None:
        return

    if not command.args or not command.args.strip().isdigit():
        await message.answer(
            "🔎 <b>Формат:</b> <code>/decide_trace ID</code>\n\n"
            "Пример:\n"
            "<code>/decide_trace 7</code>"
        )
        return

    service = _build_decision_service()
    try:
        result = service.render_decision_trace(user_id, int(command.args.strip()))
    except (DecisionServiceError, RuntimeError) as exc:
        await message.answer(f"❌ <b>Ошибка:</b> {exc}")
        return

    await message.answer(result)
