import asyncio
import hashlib
import hmac
import json
import logging
import os
import random
import re
import time
from urllib.parse import parse_qsl
from typing import Optional

from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.filters import Command, CommandStart
from aiogram.filters.command import CommandObject
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

import db

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN is not set. Put it in .env or environment.")
PORT = int(os.getenv("PORT", "8080"))
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "bulushew").lstrip("@").lower()
MINI_APP_URL = os.getenv("MINI_APP_URL", "").strip()
BOT_PUBLIC_USERNAME = os.getenv("BOT_USERNAME", "getxposedbot").lstrip("@")

logging.basicConfig(level=logging.WARNING)

router = Router()
health_app = Flask(__name__)

USERNAME_RE = re.compile(r"^@([A-Za-z0-9_]{3,32})$")
BOT_USERNAME_CACHE: Optional[str] = None
APP_BOT: Optional[Bot] = None
APP_LOOP: Optional[asyncio.AbstractEventLoop] = None
NEW_ANSWER_HINTS = [
    "👀 Появился новый взгляд",
    "⚡ Картина стала чуть точнее",
    "🔍 Кто-то помог уточнить первый шаг",
]
INITDATA_MAX_AGE_SECONDS = 86400


def normalize_username(raw: str) -> Optional[str]:
    raw = raw.strip()
    m = USERNAME_RE.match(raw)
    if not m:
        return None
    return f"@{m.group(1).lower()}"


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


@health_app.get("/")
def root_status() -> tuple[str, int]:
    return "ok", 200


@health_app.get("/miniapp")
def miniapp_index():
    return render_template("miniapp.html")


@health_app.get("/api/miniapp/me")
def api_miniapp_me():
    user = get_webapp_user()
    if not user:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    username = str(user.get("username") or "").strip().lower()
    if not username:
        return jsonify({"ok": False, "error": "Укажи @username в Telegram профиле"}), 400

    user_id = int(user.get("id"))
    db.upsert_user(
        user_id,
        f"@{username}",
        str(user.get("first_name") or ""),
        str(user.get("last_name") or ""),
        str(user.get("photo_url") or ""),
    )
    target = f"@{username}"
    payload = build_profile_payload(target)
    bot_username = get_bot_public_username()
    payload["link"] = f"https://t.me/{bot_username}?start=ref_{username}"
    payload["user"] = db.get_user_public_by_username(target) or {
        "id": user_id,
        "username": username,
        "first_name": str(user.get("first_name") or ""),
        "last_name": str(user.get("last_name") or ""),
        "photo_url": str(user.get("photo_url") or ""),
    }
    return jsonify({"ok": True, "data": payload})


@health_app.get("/api/miniapp/preview")
def api_miniapp_preview():
    return jsonify(
        {
            "ok": True,
            "data": {
                "target": "@preview_user",
                "viewed": 18,
                "answers": 13,
                "silent": 5,
                "enough": True,
                "recommendation": {"tone": "easy", "speed": "slow", "format": "text"},
                "caution_block": True,
                "uncertain_block": True,
                "link": f"https://t.me/{get_bot_public_username()}?start=ref_preview_user",
                "user": {
                    "id": 1,
                    "username": "preview_user",
                    "first_name": "Preview",
                    "last_name": "User",
                    "photo_url": "",
                },
            },
        }
    )


@health_app.get("/api/miniapp/profile")
def api_miniapp_profile():
    user = get_webapp_user()
    if not user:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    raw_target = request.args.get("target", "")
    target = normalize_username(raw_target)
    if not target:
        return jsonify({"ok": False, "error": "Нужен корректный @username"}), 400

    payload = build_profile_payload(target)
    payload["link"] = f"https://t.me/{get_bot_public_username()}?start=ref_{target.lstrip('@')}"
    payload["user"] = db.get_user_public_by_username(target) or {
        "id": 0,
        "username": target.lstrip("@"),
        "first_name": "",
        "last_name": "",
        "photo_url": "",
    }
    return jsonify({"ok": True, "data": payload})


@health_app.get("/api/miniapp/insight")
def api_miniapp_insight():
    user = get_webapp_user()
    if not user:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    raw_target = request.args.get("target", "")
    target = normalize_username(raw_target)
    if not target:
        return jsonify({"ok": False, "error": "Нужен корректный @username"}), 400

    insight_text = build_contact_insight_text(target)
    if not insight_text:
        return jsonify({"ok": True, "enough": False})
    return jsonify({"ok": True, "enough": True, "text": insight_text})


@health_app.get("/api/miniapp/preview-insight")
def api_miniapp_preview_insight():
    text = (
        "Как с этим человеком чаще всего\n"
        "начинают общение:\n\n"
        "👉 с юмора\n"
        "👉 не торопясь\n"
        "👉 через переписку\n\n"
        "⚠️ Иногда лучше не давить\n"
        "и дать время."
    )
    return jsonify({"ok": True, "enough": True, "text": text})


@health_app.get("/api/miniapp/search-users")
def api_miniapp_search_users():
    user = get_webapp_user()
    if not user:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    q = str(request.args.get("q") or "")
    items = db.search_users(q, 20)
    return jsonify({"ok": True, "items": items})


@health_app.get("/api/miniapp/preview-users")
def api_miniapp_preview_users():
    return jsonify(
        {
            "ok": True,
            "items": [
                "@bulushew",
                "@blackgrizzly17",
                "@pursenka",
                "@artemeeey",
                "@taaarraaas",
            ],
        }
    )


@health_app.get("/api/miniapp/recent-targets")
def api_miniapp_recent_targets():
    user = get_webapp_user()
    if not user:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    voter_id = int(user.get("id"))
    items = db.list_recent_targets_for_voter(voter_id, 20)
    return jsonify({"ok": True, "items": items})


@health_app.get("/api/miniapp/preview-recent-targets")
def api_miniapp_preview_recent_targets():
    return jsonify(
        {
            "ok": True,
            "items": [
                "@bulushew",
                "@pursenka",
                "@blackgrizzly17",
                "@artemeeey",
                "@taaarraaas",
            ],
        }
    )


@health_app.post("/api/miniapp/feedback")
def api_miniapp_feedback():
    user = get_webapp_user()
    if not user:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    if APP_LOOP is None or APP_BOT is None:
        return jsonify({"ok": False, "error": "service_unavailable"}), 503

    data = request.get_json(silent=True) or {}
    target = normalize_username(str(data.get("target") or ""))
    if not target:
        return jsonify({"ok": False, "error": "Нужен корректный @username"}), 400

    tone = normalize_feedback_value(str(data.get("tone") or ""), {"easy", "serious"}, "serious")
    speed = normalize_feedback_value(str(data.get("speed") or ""), {"fast", "slow"}, "slow")
    contact_format = normalize_feedback_value(str(data.get("contact_format") or ""), {"text", "live"}, "text")
    caution = normalize_feedback_value(str(data.get("caution") or ""), {"true", "false"}, "false")
    voter_id = int(user.get("id"))
    username = str(user.get("username") or "").strip().lower()
    if username:
        db.upsert_user(
            voter_id,
            f"@{username}",
            str(user.get("first_name") or ""),
            str(user.get("last_name") or ""),
            str(user.get("photo_url") or ""),
        )

    future = asyncio.run_coroutine_threadsafe(
        process_feedback_submission(
            APP_BOT,
            target,
            voter_id,
            tone,
            speed,
            contact_format,
            caution,
        ),
        APP_LOOP,
    )
    try:
        result, message = future.result(timeout=8)
    except Exception:
        return jsonify({"ok": False, "error": "timeout"}), 504

    if result is None:
        return jsonify({"ok": False, "error": message}), 503
    if result == "duplicate_recent":
        return jsonify({"ok": False, "error": message, "code": "duplicate_recent"}), 429
    return jsonify({"ok": True, "result": result, "message": message})


@health_app.post("/api/miniapp/preview-feedback")
def api_miniapp_preview_feedback():
    return jsonify({"ok": True, "result": "inserted", "message": "Готово 👍 (preview)"})


def build_launch_kb() -> Optional[types.InlineKeyboardMarkup]:
    if not MINI_APP_URL:
        return None
    kb = InlineKeyboardBuilder()
    kb.button(text="Открыть приложение", web_app=types.WebAppInfo(url=MINI_APP_URL))
    return kb.as_markup()


def register_user(message: types.Message) -> None:
    if message.from_user and message.from_user.id and message.from_user.username:
        asyncio.create_task(
            asyncio.to_thread(
                db.upsert_user,
                message.from_user.id,
                f"@{message.from_user.username.lower()}",
            )
        )


async def db_call(func, *args):
    return await asyncio.to_thread(func, *args)


async def get_bot_username(bot: Bot) -> str:
    global BOT_USERNAME_CACHE
    if BOT_USERNAME_CACHE:
        return BOT_USERNAME_CACHE
    me = await bot.get_me()
    BOT_USERNAME_CACHE = me.username or ""
    return BOT_USERNAME_CACHE


def get_bot_public_username() -> str:
    return BOT_USERNAME_CACHE or BOT_PUBLIC_USERNAME


def verify_telegram_init_data(init_data: str) -> Optional[dict]:
    if not init_data:
        return None
    pairs = dict(parse_qsl(init_data, keep_blank_values=True))
    provided_hash = pairs.pop("hash", None)
    if not provided_hash:
        return None

    data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
    secret = hmac.new(b"WebAppData", BOT_TOKEN.encode("utf-8"), hashlib.sha256).digest()
    calculated_hash = hmac.new(secret, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calculated_hash, provided_hash):
        return None

    auth_date_raw = pairs.get("auth_date")
    try:
        auth_date = int(auth_date_raw) if auth_date_raw else 0
    except ValueError:
        return None
    if auth_date <= 0 or abs(int(time.time()) - auth_date) > INITDATA_MAX_AGE_SECONDS:
        return None

    user_raw = pairs.get("user")
    if not user_raw:
        return None
    try:
        user = json.loads(user_raw)
    except json.JSONDecodeError:
        return None
    return user if isinstance(user, dict) else None


def get_webapp_user() -> Optional[dict]:
    init_data = request.headers.get("X-Telegram-Init-Data", "")
    return verify_telegram_init_data(init_data)


def normalize_feedback_value(value: str, allowed: set[str], default: str) -> str:
    return value if value in allowed else default


def build_profile_payload(target: str) -> dict:
    total = db.get_total(target)
    ref_count = db.count_ref_visitors(target)
    dimensions = db.get_contact_dimensions(target)
    combined = total + ref_count
    viewed = int(combined * 1.4)
    silent = max(0, viewed - total)
    result = {
        "target": target,
        "viewed": viewed,
        "answers": total,
        "silent": silent,
        "enough": total >= 3,
        "recommendation": None,
        "caution_block": False,
        "uncertain_block": False,
    }
    if total < 3:
        return result

    tone_pick, speed_pick, format_pick = pick_recommendation(dimensions)
    result["recommendation"] = {
        "tone": tone_pick,
        "speed": speed_pick,
        "format": format_pick,
    }
    caution_counts = dimensions["caution"]
    result["caution_block"] = (caution_counts["true"] / total) >= 0.3 if total > 0 else False

    def is_uncertain(a: int, b: int) -> bool:
        s = a + b
        return s > 0 and max(a, b) / s < 0.6

    tone_counts = dimensions["tone"]
    speed_counts = dimensions["speed"]
    format_counts = dimensions["contact_format"]
    result["uncertain_block"] = (
        is_uncertain(tone_counts["easy"], tone_counts["serious"])
        or is_uncertain(speed_counts["fast"], speed_counts["slow"])
        or is_uncertain(format_counts["text"], format_counts["live"])
    )
    return result


def queue_coroutine(coro) -> None:
    if APP_LOOP is None:
        return
    try:
        asyncio.run_coroutine_threadsafe(coro, APP_LOOP)
    except Exception:
        pass


def build_contact_insight_text(target: str) -> Optional[str]:
    total = db.get_total(target)
    if total < 3:
        return None

    dimensions = db.get_contact_dimensions(target)
    tone_counts = dimensions["tone"]
    speed_counts = dimensions["speed"]
    format_counts = dimensions["contact_format"]
    caution_counts = dimensions["caution"]

    tone_pick = "easy" if tone_counts["easy"] >= tone_counts["serious"] else "serious"
    speed_pick = "slow" if speed_counts["slow"] >= speed_counts["fast"] else "fast"
    format_pick = "text" if format_counts["text"] >= format_counts["live"] else "live"

    tone_text = "с юмора" if tone_pick == "easy" else "спокойно, по делу"
    speed_text = "не торопясь" if speed_pick == "slow" else "сразу"
    format_text = "через переписку" if format_pick == "text" else "в живом общении"

    lines = [
        "Как с этим человеком чаще всего",
        "начинают общение:",
        "",
        f"👉 {tone_text}",
        f"👉 {speed_text}",
        f"👉 {format_text}",
    ]

    def no_clear_majority(a: int, b: int) -> bool:
        s = a + b
        return s > 0 and max(a, b) / s < 0.6

    uncertain = (
        no_clear_majority(tone_counts["easy"], tone_counts["serious"])
        or no_clear_majority(speed_counts["fast"], speed_counts["slow"])
        or no_clear_majority(format_counts["text"], format_counts["live"])
    )
    if uncertain:
        lines += [
            "",
            "По этому пункту мнения разделились —",
            "лучше ориентироваться по ситуации.",
        ]

    caution_ratio = caution_counts["true"] / total if total > 0 else 0
    if caution_ratio >= 0.3:
        lines += [
            "",
            "⚠️ Иногда лучше не давить",
            "и дать время.",
        ]

    return "\n".join(lines)


async def send_tracked_push(bot: Bot, target_id: int, text: str) -> bool:
    try:
        await asyncio.wait_for(bot.send_message(target_id, text), timeout=3.0)
        return True
    except Exception as exc:
        target_username = (await db_call(db.get_username_by_user_id, target_id)) or f"id={target_id}"
        await db_call(db.delete_user_by_user_id, target_id)

        admin_id = await db_call(db.get_user_id_by_username, f"@{ADMIN_USERNAME}")
        if admin_id:
            try:
                reason = f"{type(exc).__name__}: {exc}"
                await asyncio.wait_for(
                    bot.send_message(
                        admin_id,
                        "Не удалось отправить push пользователю.\n"
                        f"Пользователь: {target_username}\n"
                        f"Причина: {reason}\n"
                        "Пользователь удалён из /users.",
                    ),
                    timeout=3.0,
                )
            except Exception:
                pass
        return False


async def process_feedback_submission(
    bot: Bot,
    target: str,
    voter_id: Optional[int],
    tone: str,
    speed: str,
    contact_format: str,
    caution: str,
) -> tuple[Optional[str], str]:
    before_total = await db_call(db.get_total, target)
    before_dimensions = await db_call(db.get_contact_dimensions, target)
    rec_before = pick_recommendation(before_dimensions)
    result = await db_call(db.add_vote, target, "feedback", voter_id, tone, speed, contact_format, caution)
    if result is None:
        return None, "База недоступна, попробуй позже"
    if result == "duplicate_recent":
        target_id = await db_call(db.get_user_id_by_username, target)
        first_seen = False
        if target_id and voter_id is not None:
            first_seen = await db_call(db.mark_seen_hint_sent, target, voter_id)
        if target_id and first_seen:
            queue_coroutine(send_tracked_push(bot, target_id, "👁 тебя явно рассматривают"))
        return result, "Мнение можно менять не чаще 1 раза в сутки"

    target_id = await db_call(db.get_user_id_by_username, target)
    if target_id:
        if result == "updated":
            queue_coroutine(send_tracked_push(bot, target_id, "⚠️ Мнение одного человека о тебе изменилось."))
        else:
            queue_coroutine(send_tracked_push(bot, target_id, random.choice(NEW_ANSWER_HINTS)))

        after_dimensions = await db_call(db.get_contact_dimensions, target)
        rec_after = pick_recommendation(after_dimensions)
        total = await db_call(db.get_total, target)
        if rec_before != rec_after:
            queue_coroutine(
                send_tracked_push(
                    bot,
                    target_id,
                    "⚠️ Картина изменилась.\nТеперь тебя считывают немного иначе.",
                )
            )
        if before_total <= 5 < total:
            queue_coroutine(send_tracked_push(bot, target_id, "🔥 вокруг тебя начинается движ"))

    message = "Мнение обновлено." if result == "updated" else "Готово 👍\n\nТы помог понять,\nкак к этому человеку проще подойти."
    return result, message


@router.message(CommandStart())
async def cmd_start(message: types.Message, command: CommandObject):
    register_user(message)
    payload = command.args or ""
    if payload.startswith("ref_") and message.from_user and message.from_user.id:
        raw = payload[4:]
        target = normalize_username(f"@{raw}") if not raw.startswith("@") else normalize_username(raw)
        if target:
            await db_call(db.add_ref_visit, target, message.from_user.id)

    launch_kb = build_launch_kb()
    if launch_kb:
        await message.answer("Открой приложение и оставь анонимный ответ 👇", reply_markup=launch_kb)
    else:
        await message.answer("Mini App временно недоступен. Обратись к администратору.")


@router.message(Command("ref"))
async def cmd_ref(message: types.Message):
    launch_kb = build_launch_kb()
    if launch_kb:
        await message.answer("Создание ссылок теперь в Mini App 👇", reply_markup=launch_kb)
    else:
        await message.answer("Mini App временно недоступен.")


@router.message(Command("stats"))
async def cmd_stats(message: types.Message):
    launch_kb = build_launch_kb()
    if launch_kb:
        await message.answer("Статистика теперь в Mini App 👇", reply_markup=launch_kb)
    else:
        await message.answer("Mini App временно недоступен.")


@router.message(Command("admin_stats"))
async def cmd_admin_stats(message: types.Message):
    register_user(message)
    username = (message.from_user.username or "").lower() if message.from_user else ""
    if username != ADMIN_USERNAME:
        return

    users_total = await db_call(db.count_users)
    votes_total = await db_call(db.count_votes)
    top_voters = await db_call(db.top_voters, 10)
    top_targets = await db_call(db.top_targets, 10)

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

    await message.answer("\n".join(lines))


@router.message(Command("users"))
async def cmd_users(message: types.Message):
    register_user(message)
    username = (message.from_user.username or "").lower() if message.from_user else ""
    if username != ADMIN_USERNAME:
        return

    users = await db_call(db.list_users, 100)
    if not users:
        await message.answer("Список пуст.")
        return

    text = "Пользователи (последние 100):\n" + "\n".join(users)
    await message.answer(text)


@router.message(Command("normalize_case"))
async def cmd_normalize_case(message: types.Message):
    register_user(message)
    username = (message.from_user.username or "").lower() if message.from_user else ""
    if username != ADMIN_USERNAME:
        return
    merged, lowercased = await db_call(db.normalize_case_data)
    await message.answer(
        f"Нормализация выполнена.\nСхлопнуто дублей users: {merged}\nПриведено к lower-case: {lowercased}",
    )


@router.message(F.text)
async def on_text(message: types.Message):
    register_user(message)
    if (message.text or "").startswith("/"):
        return
    launch_kb = build_launch_kb()
    if launch_kb:
        await message.answer("Весь функционал перенесён в Mini App 👇", reply_markup=launch_kb)
    else:
        await message.answer("Mini App временно недоступен.")


async def main():
    global APP_BOT, APP_LOOP
    # Run minimal HTTP server for platform health checks.
    loop = asyncio.get_running_loop()
    APP_LOOP = loop
    loop.run_in_executor(
        None,
        lambda: health_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False),
    )
    db.init_db()
    db.normalize_case_data()
    bot = Bot(BOT_TOKEN)
    APP_BOT = bot
    await get_bot_username(bot)
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
