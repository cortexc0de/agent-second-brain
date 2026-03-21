"""Handlers for review-loop commands."""

from __future__ import annotations

from aiogram import Router
from aiogram.filters import Command, CommandObject
from aiogram.types import Message

from d_brain.config import get_settings
from d_brain.services.review_service import ReviewService, ReviewServiceError

router = Router(name="review")


def _build_review_service() -> ReviewService:
    settings = get_settings()
    store_path = settings.vault_path / ".decision-store.sqlite3"
    return ReviewService(store_path=store_path)


async def _require_user_id(message: Message) -> int | None:
    if message.from_user is None:
        await message.answer("❌ <b>Ошибка:</b> Не удалось определить пользователя.")
        return None
    return message.from_user.id


@router.message(Command("review"))
async def cmd_review(message: Message) -> None:
    """Show due reviews for the current user."""
    user_id = await _require_user_id(message)
    if user_id is None:
        return
    service = _build_review_service()
    try:
        result = service.render_review_overview(user_id)
    except ReviewServiceError as exc:
        await message.answer(f"❌ <b>Ошибка:</b> {exc}")
        return

    await message.answer(result)


@router.message(Command("review_trace"))
async def cmd_review_trace(message: Message, command: CommandObject) -> None:
    """Show proactive delivery trace for a review."""
    user_id = await _require_user_id(message)
    if user_id is None:
        return
    if not command.args or not command.args.strip().isdigit():
        await message.answer(
            "🔎 <b>Формат:</b> <code>/review_trace ID</code>\n\n"
            "Пример:\n"
            "<code>/review_trace 3</code>"
        )
        return

    service = _build_review_service()
    try:
        result = service.render_review_trace(user_id, int(command.args.strip()))
    except ReviewServiceError as exc:
        await message.answer(f"❌ <b>Ошибка:</b> {exc}")
        return

    await message.answer(result)


@router.message(Command("review_done"))
async def cmd_review_done(message: Message, command: CommandObject) -> None:
    """Complete a review with a short outcome note."""
    user_id = await _require_user_id(message)
    if user_id is None:
        return
    if not command.args:
        await message.answer(
            "🔁 <b>Формат:</b> <code>/review_done ID что получилось</code>\n\n"
            "Пример:\n"
            "<code>/review_done 3 активации выросли, фокус подтвердился</code>"
        )
        return

    parts = command.args.strip().split(maxsplit=1)
    if len(parts) != 2 or not parts[0].isdigit():
        await message.answer(
            "❌ <b>Нужны ID и результат.</b>\n"
            "Используй формат: <code>/review_done ID что получилось</code>"
        )
        return

    service = _build_review_service()
    try:
        result = service.complete_review(user_id, int(parts[0]), parts[1])
    except ReviewServiceError as exc:
        await message.answer(f"❌ <b>Ошибка:</b> {exc}")
        return

    await message.answer(result)


@router.message(Command("review_skip"))
async def cmd_review_skip(message: Message, command: CommandObject) -> None:
    """Skip a review checkpoint."""
    user_id = await _require_user_id(message)
    if user_id is None:
        return
    if not command.args or not command.args.strip().isdigit():
        await message.answer(
            "⏭️ <b>Формат:</b> <code>/review_skip ID</code>\n\n"
            "Пример:\n"
            "<code>/review_skip 3</code>"
        )
        return

    service = _build_review_service()
    try:
        result = service.skip_review(user_id, int(command.args.strip()))
    except ReviewServiceError as exc:
        await message.answer(f"❌ <b>Ошибка:</b> {exc}")
        return

    await message.answer(result)
