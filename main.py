import asyncio
import logging
import os
import re
from urllib.parse import quote_plus
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
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "bulushew").lstrip("@").lower()

logging.basicConfig(level=logging.WARNING)

router = Router()
health_app = Flask(__name__)

USERNAME_RE = re.compile(r"^@([A-Za-z0-9_]{3,32})$")
WAITING_FOR_USERNAME: set[int] = set()
NEW_ANSWER_HINTS = [
    "👀 Появился новый взгляд",
    "⚡ Картина стала чуть точнее",
    "🔍 Кто-то помог уточнить первый шаг",
]


def normalize_username(raw: str) -> Optional[str]:
    raw = raw.strip()
    m = USERNAME_RE.match(raw)
    if not m:
        return None
    return f"@{m.group(1)}"


def build_tone_kb(target: str) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="😄 Легко, с юмора", callback_data=f"tone|easy|{target}")
    kb.button(text="🧠 Спокойно, по делу", callback_data=f"tone|serious|{target}")
    kb.adjust(1, 1)
    return kb.as_markup()


def build_speed_kb(target: str, tone: str) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🔥 Можно сразу", callback_data=f"speed|fast|{target}|{tone}")
    kb.button(text="🐢 Лучше постепенно", callback_data=f"speed|slow|{target}|{tone}")
    kb.adjust(1, 1)
    return kb.as_markup()


def build_format_kb(target: str, tone: str, speed: str) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="💬 Переписка", callback_data=f"format|text|{target}|{tone}|{speed}")
    kb.button(text="🎤 Живое общение", callback_data=f"format|live|{target}|{tone}|{speed}")
    kb.adjust(1, 1)
    return kb.as_markup()


def build_caution_kb(target: str, tone: str, speed: str, contact_format: str) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🚩 Да", callback_data=f"caution|true|{target}|{tone}|{speed}|{contact_format}")
    kb.button(text="👍 Нет", callback_data=f"caution|false|{target}|{tone}|{speed}|{contact_format}")
    kb.adjust(1, 1)
    return kb.as_markup()


def pick_recommendation(dimensions: dict[str, dict[str, int]]) -> tuple[str, str, str]:
    tone_counts = dimensions.get("tone", {})
    speed_counts = dimensions.get("speed", {})
    format_counts = dimensions.get("contact_format", {})

    tone_pick = "easy" if tone_counts.get("easy", 0) >= tone_counts.get("serious", 0) else "serious"
    speed_pick = "slow" if speed_counts.get("slow", 0) >= speed_counts.get("fast", 0) else "fast"
    format_pick = "text" if format_counts.get("text", 0) >= format_counts.get("live", 0) else "live"
    return tone_pick, speed_pick, format_pick


@health_app.get("/health")
def health() -> tuple[str, int]:
    return "ok", 200


def build_main_kb() -> types.ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.button(text="👀 Посмотреть себя")
    kb.button(text="✍️ Ответить про человека")
    kb.adjust(2)
    return kb.as_markup(resize_keyboard=True)


def build_after_rate_kb() -> types.ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    kb.button(text="👀 Посмотреть про себя")
    kb.button(text="➕ Ответить ещё про кого-то")
    kb.adjust(2)
    return kb.as_markup(resize_keyboard=True)


def build_share_kb(link: str) -> types.InlineKeyboardMarkup:
    share_text = "нука интересно как меня оценишь"
    share_url = f"https://t.me/share/url?url={quote_plus(link)}&text={quote_plus(share_text)}"
    kb = InlineKeyboardBuilder()
    kb.button(text="🔗 Поделиться ссылкой", url=share_url)
    return kb.as_markup()

def register_user(message: types.Message) -> None:
    if message.from_user and message.from_user.id and message.from_user.username:
        db.upsert_user(message.from_user.id, f"@{message.from_user.username}")


@router.message(CommandStart())
async def cmd_start(message: types.Message, command: CommandObject):
    register_user(message)
    payload = command.args
    if payload and payload.startswith("ref_"):
        raw = payload[4:]
        target = normalize_username(f"@{raw}") if not raw.startswith("@") else normalize_username(raw)
        if target:
            if message.from_user and message.from_user.id:
                db.add_ref_visit(target, message.from_user.id)
            await message.answer(
                "Как бы ты начал разговор?",
                reply_markup=build_tone_kb(target),
            )
            return
    start_text = (
        "Иногда сложно понять,\n"
        "как лучше начать разговор.\n\n"
        "Этот бот — про это."
    )
    await message.answer(
        start_text,
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

    total = db.get_total(target)
    if total == 0:
        await message.answer(f"Пока нет оценок для {target}.", reply_markup=build_main_kb())
        return

    dims = db.get_contact_dimensions(target)
    lines = [
        f"Статистика для {target}:",
        f"Всего ответов: {total}",
        f"Tone easy/serious: {dims['tone']['easy']}/{dims['tone']['serious']}",
        f"Speed fast/slow: {dims['speed']['fast']}/{dims['speed']['slow']}",
        f"Format text/live: {dims['contact_format']['text']}/{dims['contact_format']['live']}",
        f"Caution true/false: {dims['caution']['true']}/{dims['caution']['false']}",
    ]
    await message.answer("\n".join(lines), reply_markup=build_main_kb())


@router.message(Command("admin_stats"))
async def cmd_admin_stats(message: types.Message):
    register_user(message)
    username = (message.from_user.username or "").lower() if message.from_user else ""
    if username != ADMIN_USERNAME:
        return

    users_total = db.count_users()
    votes_total = db.count_votes()
    top_voters = db.top_voters(10)
    top_targets = db.top_targets(10)

    lines = [
        "Админ статистика:",
        f"Пользователей (/start): {users_total}",
        f"Всего оценок: {votes_total}",
        "",
        "Топ 10 кто больше всех оставил оценок:",
    ]
    if top_voters:
        for i, (uname, cnt) in enumerate(top_voters, start=1):
            label = uname if uname else "(без username)"
            lines.append(f"{i}. {label}: {cnt}")
    else:
        lines.append("пока пусто")

    lines.append("")
    lines.append("Топ 10 о ком больше всего оставили оценок:")
    if top_targets:
        for i, (target, cnt) in enumerate(top_targets, start=1):
            lines.append(f"{i}. {target}: {cnt}")
    else:
        lines.append("пока пусто")

    await message.answer("\n".join(lines), reply_markup=build_main_kb())


@router.message(Command("users"))
async def cmd_users(message: types.Message):
    register_user(message)
    username = (message.from_user.username or "").lower() if message.from_user else ""
    if username != ADMIN_USERNAME:
        return

    users = db.list_users(100)
    if not users:
        await message.answer("Список пуст.", reply_markup=build_main_kb())
        return

    text = "Пользователи (последние 100):\n" + "\n".join(users)
    await message.answer(text, reply_markup=build_main_kb())


@router.message(F.text)
async def on_text(message: types.Message):
    register_user(message)
    text = (message.text or "").strip()
    if text.startswith("/"):
        if message.from_user:
            WAITING_FOR_USERNAME.discard(message.from_user.id)
        return
    lowered = text.lower()
    if lowered in ("мой профиль", "👀 посмотреть себя", "👀 посмотреть про себя"):
        if not message.from_user or not message.from_user.username:
            await message.answer("Нужен @username в профиле Telegram", reply_markup=build_main_kb())
            return
        target = f"@{message.from_user.username}"
        me = await message.bot.get_me()
        link = f"https://t.me/{me.username}?start=ref_{message.from_user.username}"
        total = db.get_total(target)
        ref_count = db.count_ref_visitors(target)
        combined = total + ref_count
        viewed = int(combined * 1.4)
        silent = max(0, viewed - total)

        dimensions = db.get_contact_dimensions(target)

        lines = [
            "твоя ссылка 👇",
            f"`{link}`",
            "",
            f"👀 посмотрели — {viewed}",
            "🔥 ответов — похоже, кто-то уже заходил"
            if total == 0
            else f"🔥 ответов — {total}",
            f"👁 молча заглянули — {silent}",
            "",
            "— — —",
            "",
        ]
        if total < 3:
            lines += [
                "Похоже, кто-то уже отвечал.",
                "",
                "Нужно ещё пару ответов,",
                "чтобы собрать понятную картину.",
            ]
        else:
            tone_counts = dimensions["tone"]
            speed_counts = dimensions["speed"]
            format_counts = dimensions["contact_format"]

            tone_pick = "easy" if tone_counts["easy"] >= tone_counts["serious"] else "serious"
            speed_pick = "slow" if speed_counts["slow"] >= speed_counts["fast"] else "fast"
            format_pick = "text" if format_counts["text"] >= format_counts["live"] else "live"

            tone_text = "👉 лёгкий заход, с юмора" if tone_pick == "easy" else "👉 спокойно и по делу"
            speed_text = "👉 лучше не торопиться" if speed_pick == "slow" else "👉 можно сразу"
            format_text = "👉 начать с переписки" if format_pick == "text" else "👉 лучше в живом общении"

            lines += [
                "Как с тобой чаще всего",
                "начинают контакт:",
                "",
                tone_text,
                speed_text,
                format_text,
                "",
                "— — —",
            ]

            caution_counts = dimensions["caution"]
            redflag_ratio = caution_counts["true"] / total if total > 0 else 0
            if redflag_ratio >= 0.3:
                lines += [
                    "",
                    "⚠️ Иногда люди чувствуют напряжение.",
                    "Лучше не давить и дать время.",
                    "",
                    "— — —",
                ]

            def is_uncertain(a: int, b: int) -> bool:
                s = a + b
                return s > 0 and max(a, b) / s < 0.6

            uncertain = (
                is_uncertain(tone_counts["easy"], tone_counts["serious"])
                or is_uncertain(speed_counts["fast"], speed_counts["slow"])
                or is_uncertain(format_counts["text"], format_counts["live"])
            )
            if uncertain:
                lines += [
                    "",
                    "По этому пункту",
                    "мнения разделились —",
                    "лучше ориентироваться по ситуации.",
                ]

        text = "\n".join(lines)
        reply_kb = build_after_rate_kb() if total < 3 else build_main_kb()
        await message.answer(
            text,
            reply_markup=reply_kb,
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
        await message.answer(" ", reply_markup=build_share_kb(link))
        if total < 3:
            await message.answer(
                "Лучше всего работает,\nесли скинуть в знакомый чат",
                reply_markup=reply_kb,
            )
        return
    if lowered in (
        "дать коммент",
        "✍️ ответить про человека",
        "➕ ответить ещё про кого-то",
    ):
        if message.from_user:
            WAITING_FOR_USERNAME.add(message.from_user.id)
        await message.answer(
            "Про кого отвечаем?\n"
            "поле ввода @username\n\n"
            "Это просто для ориентира,\n"
            "никто не узнает, что это был ты",
            reply_markup=build_main_kb(),
        )
        return
    if message.from_user and message.from_user.id in WAITING_FOR_USERNAME:
        target = normalize_username(text)
        if not target:
            await message.answer("Нужен корректный @username.", reply_markup=build_main_kb())
            return
        WAITING_FOR_USERNAME.discard(message.from_user.id)
        await message.answer(
            "Как бы ты начал разговор?",
            reply_markup=build_tone_kb(target),
        )
        return
    target = normalize_username(text)
    if not target:
        return
    await message.answer(
        "Как бы ты начал разговор?",
        reply_markup=build_tone_kb(target),
    )


@router.callback_query(F.data.startswith("tone|"))
async def on_tone(callback: types.CallbackQuery):
    parts = (callback.data or "").split("|", 2)
    if len(parts) != 3:
        await callback.answer("Некорректные данные", show_alert=True)
        return
    _, tone, target = parts
    if tone not in {"easy", "serious"}:
        await callback.answer("Некорректный формат", show_alert=True)
        return

    await callback.answer("Принято")
    await callback.message.answer(
        "Насколько можно быть прямым?",
        reply_markup=build_speed_kb(target, tone),
    )


@router.callback_query(F.data.startswith("speed|"))
async def on_speed(callback: types.CallbackQuery):
    parts = (callback.data or "").split("|", 3)
    if len(parts) != 4:
        await callback.answer("Некорректные данные", show_alert=True)
        return
    _, speed, target, tone = parts
    if speed not in {"fast", "slow"}:
        await callback.answer("Некорректный формат", show_alert=True)
        return
    if tone not in {"easy", "serious"}:
        tone = "serious"

    await callback.answer("Принято")
    await callback.message.answer(
        "Где контакт зайдёт лучше?",
        reply_markup=build_format_kb(target, tone, speed),
    )


@router.callback_query(F.data.startswith("format|"))
async def on_format(callback: types.CallbackQuery):
    parts = (callback.data or "").split("|", 4)
    if len(parts) != 5:
        await callback.answer("Некорректные данные", show_alert=True)
        return
    _, contact_format, target, tone, speed = parts
    if contact_format not in {"text", "live"}:
        await callback.answer("Некорректный формат", show_alert=True)
        return
    if tone not in {"easy", "serious"}:
        tone = "serious"
    if speed not in {"fast", "slow"}:
        speed = "slow"

    await callback.answer("Принято")
    await callback.message.answer(
        "Есть ли что-то,\nс чем стоит быть аккуратнее?",
        reply_markup=build_caution_kb(target, tone, speed, contact_format),
    )


@router.callback_query(F.data.startswith("caution|"))
async def on_caution(callback: types.CallbackQuery):
    parts = (callback.data or "").split("|", 5)
    if len(parts) != 6:
        await callback.answer("Некорректные данные", show_alert=True)
        return
    _, caution, target, tone, speed, contact_format = parts
    if caution not in {"true", "false"}:
        await callback.answer("Некорректный формат", show_alert=True)
        return
    if tone not in {"easy", "serious"}:
        tone = "serious"
    if speed not in {"fast", "slow"}:
        speed = "slow"
    if contact_format not in {"text", "live"}:
        contact_format = "text"

    voter_id = callback.from_user.id if callback.from_user else None
    before_total = db.get_total(target)
    before_dimensions = db.get_contact_dimensions(target)
    rec_before = pick_recommendation(before_dimensions)
    ok = db.add_vote(target, "feedback", voter_id, tone, speed, contact_format, caution)
    if ok is None:
        await callback.answer("База недоступна, попробуй позже", show_alert=True)
        return
    if not ok:
        target_id = db.get_user_id_by_username(target)
        if target_id and voter_id is not None and db.mark_seen_hint_sent(target, voter_id):
            async def _send_seen_hint() -> None:
                try:
                    await asyncio.wait_for(
                        callback.bot.send_message(target_id, "👁 тебя явно рассматривают"),
                        timeout=3.0,
                    )
                except Exception:
                    pass

            asyncio.create_task(_send_seen_hint())
        await callback.answer("Вы уже оценивали этого пользователя", show_alert=True)
        return
    await callback.answer("Принято")
    await callback.message.answer(
        "Готово 👍\n\nТы помог понять,\nкак к этому человеку проще подойти.",
        reply_markup=build_after_rate_kb(),
    )

    target_id = db.get_user_id_by_username(target)
    if not target_id:
        return

    async def _send_notify() -> None:
        try:
            await asyncio.wait_for(
                callback.bot.send_message(target_id, random.choice(NEW_ANSWER_HINTS)),
                timeout=3.0,
            )
        except Exception:
            pass

    asyncio.create_task(_send_notify())

    after_dimensions = db.get_contact_dimensions(target)
    rec_after = pick_recommendation(after_dimensions)
    total = db.get_total(target)
    if rec_before != rec_after:
        async def _send_recommendation_changed_hint() -> None:
            try:
                await asyncio.wait_for(
                    callback.bot.send_message(
                        target_id,
                        "⚠️ Картина изменилась.\nТеперь тебя считывают немного иначе.",
                    ),
                    timeout=3.0,
                )
            except Exception:
                pass

        asyncio.create_task(_send_recommendation_changed_hint())

    if before_total <= 5 < total:
        async def _send_hype_hint() -> None:
            try:
                await asyncio.wait_for(
                    callback.bot.send_message(target_id, "🔥 вокруг тебя начинается движ"),
                    timeout=3.0,
                )
            except Exception:
                pass

        asyncio.create_task(_send_hype_hint())


async def main():
    # Run minimal HTTP server for platform health checks.
    loop = asyncio.get_running_loop()
    loop.run_in_executor(
        None,
        lambda: health_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False),
    )
    db.init_db()
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
