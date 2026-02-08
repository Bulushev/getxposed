import asyncio
import logging
import os
import re
from typing import Optional

from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.filters import Command, CommandStart
from aiogram.filters.command import CommandObject
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from dotenv import load_dotenv
from flask import Flask

import db

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN is not set. Put it in .env or environment.")
PORT = int(os.getenv("PORT", "8080"))

logging.basicConfig(level=logging.INFO)

router = Router()
health_app = Flask(__name__)

RATINGS = [
    "🔥 горячий",
    "⚡ магнит",
    "💔 краш",
    "👀 странный",
    "🗿 мутный",
    "🤯 непредсказуемый",
    "😈 опасный",
    "🚩 ред флаг",
]

USERNAME_RE = re.compile(r"^@([A-Za-z0-9_]{3,32})$")
WAITING_FOR_USERNAME: set[int] = set()
NOTIFY_TEXTS = {
    "🔥 горячий": "🔥 осторожно.\nкто-то явно на тебя залип.",
    "⚡ магнит": "⚡ сопротивляться тебе сложно.\nи кто-то это только что подтвердил.",
    "💔 краш": "💔 кто-то в тебя втрескался.\nи явно не собирается признаваться 🙂",
    "👀 странный": "👀 тебя только что назвали странным.\nв хорошем смысле…\nнаверное.",
    "🗿 мутный": "🗿 кто-то вообще не понимает, что у тебя в голове.",
    "🤯 непредсказуемый": "🤯 ты явно делаешь неожиданные вещи.\nи люди это запоминают.",
    "😈 опасный": "😈 с тобой явно не всё так просто.\nи кто-то это уже понял.",
    "🚩 ред флаг": "🚩 похоже, рядом с тобой у кого-то включается режим \"осторожно\".",
}


def normalize_username(raw: str) -> Optional[str]:
    raw = raw.strip()
    m = USERNAME_RE.match(raw)
    if not m:
        return None
    return f"@{m.group(1)}"


def build_rating_kb(target: str) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for idx, label in enumerate(RATINGS):
        kb.button(text=label, callback_data=f"rate|{idx}|{target}")
    kb.adjust(2, 2, 2)
    return kb.as_markup()


@health_app.get("/health")
def health() -> tuple[str, int]:
    return "ok", 200


def build_main_kb() -> types.ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.button(text="мой профиль")
    kb.button(text="дать коммент")
    kb.adjust(2)
    return kb.as_markup(resize_keyboard=True)


def register_user(message: types.Message) -> None:
    if message.from_user and message.from_user.id and message.from_user.username:
        db.upsert_user(message.from_user.id, f"@{message.from_user.username}")


@router.message(CommandStart())
async def cmd_start(message: types.Message, bot: Bot, command: CommandObject):
    register_user(message)
    payload = command.args
    if payload and payload.startswith("ref_"):
        raw = payload[4:]
        target = normalize_username(f"@{raw}") if not raw.startswith("@") else normalize_username(raw)
        if target:
            await message.answer(
                f"Оцени пользователя {target}:",
                reply_markup=build_rating_kb(target),
            )
            await message.answer("меню", reply_markup=build_main_kb())
            return
    total = 0
    if message.from_user and message.from_user.username:
        total = db.get_total(f"@{message.from_user.username}")
    await message.answer(
        f"тебя уже оценили {total} человека, открой свой профиль",
        reply_markup=build_main_kb(),
    )


@router.message(Command("ref"))
async def cmd_ref(message: types.Message, bot: Bot):
    register_user(message)
    text = message.text or ""
    parts = text.split(maxsplit=1)
    username = None
    if len(parts) == 2:
        username = normalize_username(parts[1])
    if not username and message.from_user and message.from_user.username:
        username = f"@{message.from_user.username}"

    if not username:
        await message.answer("Укажи @username, чтобы сделать реферальную ссылку.")
        return

    me = await bot.get_me()
    bot_username = me.username
    link = f"https://t.me/{bot_username}?start=ref_{username.lstrip('@')}"
    await message.answer(
        f"Ссылка для оценки {username}:\n{link}",
        disable_web_page_preview=True,
        reply_markup=build_main_kb(),
    )


@router.message(Command("stats"))
async def cmd_stats(message: types.Message):
    register_user(message)
    text = message.text or ""
    parts = text.split(maxsplit=1)
    if len(parts) != 2:
        await message.answer("Использование: /stats @username", reply_markup=build_main_kb())
        return
    target = normalize_username(parts[1])
    if not target:
        await message.answer("Нужен корректный @username.", reply_markup=build_main_kb())
        return

    rows = db.get_stats(target)
    total = db.get_total(target)
    if total == 0:
        await message.answer(f"Пока нет оценок для {target}.", reply_markup=build_main_kb())
        return

    lines = [f"Статистика для {target} (всего {total}):"]
    counts = {label: 0 for label in RATINGS}
    for label, cnt in rows:
        counts[label] = cnt
    for label in RATINGS:
        lines.append(f"{label}: {counts[label]}")
    await message.answer("\n".join(lines), reply_markup=build_main_kb())


@router.message(F.text)
async def on_text(message: types.Message):
    register_user(message)
    text = (message.text or "").strip()
    lowered = text.lower()
    if lowered == "мой профиль":
        if not message.from_user or not message.from_user.username:
            await message.answer("Нужен @username в профиле Telegram", reply_markup=build_main_kb())
            return
        target = f"@{message.from_user.username}"
        rows = db.get_stats(target)
        total = db.get_total(target)
        if total == 0:
            await message.answer(f"Пока нет оценок для {target}.", reply_markup=build_main_kb())
            return
        lines = [f"Статистика для {target} (всего {total}):"]
        counts = {label: 0 for label in RATINGS}
        for label, cnt in rows:
            counts[label] = cnt
        for label in RATINGS:
            lines.append(f"{label}: {counts[label]}")
        await message.answer("\n".join(lines), reply_markup=build_main_kb())
        return
    if lowered == "дать коммент":
        if message.from_user:
            WAITING_FOR_USERNAME.add(message.from_user.id)
        await message.answer("Укажи @username, кому хочешь дать оценку.", reply_markup=build_main_kb())
        return
    if message.from_user and message.from_user.id in WAITING_FOR_USERNAME:
        target = normalize_username(text)
        if not target:
            await message.answer("Нужен корректный @username.", reply_markup=build_main_kb())
            return
        WAITING_FOR_USERNAME.discard(message.from_user.id)
        await message.answer(
            f"Оцени пользователя {target}:",
            reply_markup=build_rating_kb(target),
        )
        await message.answer("меню", reply_markup=build_main_kb())
        return
    target = normalize_username(text)
    if not target:
        return
    await message.answer(
        f"Оцени пользователя {target}:",
        reply_markup=build_rating_kb(target),
    )
    await message.answer("меню", reply_markup=build_main_kb())


@router.callback_query(F.data.startswith("rate|"))
async def on_rate(callback: types.CallbackQuery):
    parts = (callback.data or "").split("|", 2)
    if len(parts) != 3:
        await callback.answer("Некорректные данные", show_alert=True)
        return
    _, idx_str, target = parts
    try:
        idx = int(idx_str)
        label = RATINGS[idx]
    except Exception:
        await callback.answer("Некорректная оценка", show_alert=True)
        return

    voter_id = callback.from_user.id if callback.from_user else None
    ok = db.add_vote(target, label, voter_id)
    if not ok:
        await callback.answer("Вы уже оценивали этого пользователя", show_alert=True)
        return
    await callback.answer("Готово")
    await callback.message.answer(
        f"Оценка сохранена для {target}.",
        reply_markup=build_main_kb(),
    )

    target_id = db.get_user_id_by_username(target)
    if target_id:
        try:
            extra = NOTIFY_TEXTS.get(label, "")
            text = f"Тебя оценили: {label}"
            if extra:
                text = f"{text}\n\n{extra}"
            await callback.bot.send_message(target_id, text)
        except Exception:
            # User might have blocked the bot or not started it.
            pass


async def main():
    # Run minimal HTTP server for platform health checks.
    loop = asyncio.get_running_loop()
    loop.run_in_executor(
        None,
        lambda: health_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False),
    )
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
