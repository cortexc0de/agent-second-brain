"""Handler for /decide command - focused decision support."""

import asyncio
import logging

from aiogram import Router
from aiogram.filters import Command, CommandObject
from aiogram.types import Message

from d_brain.bot.formatters import format_process_report
from d_brain.config import get_settings
from d_brain.services.decision_service import DecisionService

router = Router(name="decide")
logger = logging.getLogger(__name__)


@router.message(Command("decide"))
async def cmd_decide(message: Message, command: CommandObject) -> None:
    """Handle /decide command for structured decision support."""
    user_id = message.from_user.id if message.from_user else 0

    if not command.args:
        await message.answer(
            "🎯 <b>Формат:</b> <code>/decide твой вопрос</code>\n\n"
            "Пример:\n"
            "<code>/decide у меня 5 направлений, не понимаю, что оставить на ближайшие 2 недели</code>"
        )
        return

    status_msg = await message.answer("⏳ Думаю над решением...")
    settings = get_settings()
    store_path = settings.vault_path / ".decision-store.sqlite3"
    service = DecisionService(
        settings.vault_path,
        settings.todoist_api_key,
        store_path=store_path,
    )

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

    report = await run_with_progress()
    formatted = format_process_report(report)

    try:
        await status_msg.edit_text(formatted)
    except Exception:
        await status_msg.edit_text(formatted, parse_mode=None)
