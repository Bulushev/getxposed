import asyncio
import logging
import os
from typing import Optional

from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.filters import Command, CommandStart
from aiogram.filters.command import CommandObject
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, render_template, request

import db
from app.profile import (
    build_contact_insight_text,
    build_profile_payload,
    normalize_feedback_value,
    normalize_username,
)
from app.push import PushManager
from app.telegram_profile import (
    fetch_avatar_from_telegram,
    fetch_public_user_from_telegram,
    fetch_user_bio_from_telegram,
)
from app.ui import build_launch_kb
from app.webapp_auth import build_avatar_proxy_url, get_webapp_user

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

BOT_USERNAME_CACHE: Optional[str] = None
APP_BOT: Optional[Bot] = None
APP_LOOP: Optional[asyncio.AbstractEventLoop] = None
INITDATA_MAX_AGE_SECONDS = 86400
PUSH_TIMEOUT_SECONDS = 15.0


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
    user = get_webapp_user(request, BOT_TOKEN, INITDATA_MAX_AGE_SECONDS)
    if not user:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    username = str(user.get("username") or "").strip().lower()
    if not username:
        return jsonify({"ok": False, "error": "Укажи @username в Telegram профиле"}), 400

    user_id = int(user.get("id"))
    first_name = str(user.get("first_name") or "")
    last_name = str(user.get("last_name") or "")
    init_photo_url = str(user.get("photo_url") or "")
    is_new = db.upsert_user_with_flag(
        user_id,
        f"@{username}",
        first_name,
        last_name,
        init_photo_url,
    )
    if is_new and APP_BOT:
        queue_coroutine(notify_admin_new_user(APP_BOT, user_id, f"@{username}", "miniapp"))
    target = f"@{username}"
    payload = build_profile_payload(target)
    bot_username = get_bot_public_username()
    payload["link"] = f"https://t.me/{bot_username}?start=ref_{username}"
    payload["invite_link"] = f"https://t.me/{bot_username}"
    payload["is_app_user"] = True
    stored_user = db.get_user_public_by_username(target) or {}
    payload["user"] = {
        "id": int(stored_user.get("id") or user_id),
        "username": str(stored_user.get("username") or username),
        "first_name": str(stored_user.get("first_name") or first_name),
        "last_name": str(stored_user.get("last_name") or last_name),
        "photo_url": str(stored_user.get("photo_url") or init_photo_url),
    }
    if not payload["user"]["username"]:
        payload["user"] = {
        "id": user_id,
        "username": username,
        "first_name": first_name,
        "last_name": last_name,
        "photo_url": init_photo_url,
        }
    payload["user"]["avatar_url"] = build_avatar_proxy_url(payload["user"]["username"])
    note = db.get_profile_note(user_id)
    if not note and APP_LOOP and APP_BOT:
        try:
            note = asyncio.run_coroutine_threadsafe(
                fetch_user_bio_from_telegram(APP_BOT, user_id),
                APP_LOOP,
            ).result(timeout=4)
        except Exception:
            note = ""
    payload["profile_note"] = note
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
                "visitors": 18,
                "silent": 5,
                "enough": True,
                "recommendation": {"tone": "easy", "speed": "slow", "format": "text"},
                "caution_block": True,
                "uncertain_block": True,
                "link": f"https://t.me/{get_bot_public_username()}?start=ref_preview_user",
                "invite_link": f"https://t.me/{get_bot_public_username()}",
                "is_app_user": True,
                "profile_note": "Люблю спокойное и уважительное общение.",
                "result_rows": [
                    {"title": "Темп", "value": "лучше не спеша и без частых сообщений"},
                    {"title": "Инициатива", "value": "лучше аккуратно и без давления"},
                    {"title": "Контакт", "value": "легче начать с шутки и переписки"},
                ],
                "extra_hint": "Человеку может понадобиться время на ответ",
                "adaptive_questions": {
                    "ask_tone_question": False,
                    "ask_uncertainty_question": False,
                },
                "user": {
                    "id": 1,
                    "username": "preview_user",
                    "first_name": "Preview",
                    "last_name": "User",
                    "photo_url": "",
                    "avatar_url": "",
                },
            },
        }
    )


@health_app.get("/api/miniapp/profile")
def api_miniapp_profile():
    user = get_webapp_user(request, BOT_TOKEN, INITDATA_MAX_AGE_SECONDS)
    if not user:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    raw_target = request.args.get("target", "")
    target = normalize_username(raw_target)
    if not target:
        return jsonify({"ok": False, "error": "Нужен корректный @username"}), 400

    user_payload = db.get_user_public_by_username(target)
    target_is_app_user = bool(user_payload and user_payload.get("app_user"))
    # If profile data isn't in DB yet, try resolving basic public user info from Telegram.
    if (not user_payload or (not user_payload.get("first_name") and not user_payload.get("last_name"))) and APP_LOOP and APP_BOT:
        try:
            resolved = asyncio.run_coroutine_threadsafe(
                fetch_public_user_from_telegram(APP_BOT, target),
                APP_LOOP,
            ).result(timeout=5)
        except Exception:
            resolved = None
        if resolved:
            db.upsert_user(
                int(resolved["id"]),
                f"@{resolved['username']}",
                str(resolved.get("first_name") or ""),
                str(resolved.get("last_name") or ""),
                str(resolved.get("photo_url") or ""),
                False,
            )
            user_payload = db.get_user_public_by_username(target)

    payload = build_profile_payload(target)
    bot_username = get_bot_public_username()
    payload["link"] = f"https://t.me/{bot_username}?start=ref_{target.lstrip('@')}"
    payload["invite_link"] = f"https://t.me/{bot_username}"
    payload["user"] = user_payload or {
        "id": 0,
        "username": target.lstrip("@"),
        "first_name": "",
        "last_name": "",
        "photo_url": "",
        "app_user": False,
    }
    payload["user"]["avatar_url"] = build_avatar_proxy_url(payload["user"]["username"])
    payload["is_app_user"] = bool(payload["user"].get("app_user") or target_is_app_user)
    target_user_id = int(payload["user"].get("id") or 0)
    note = db.get_profile_note(target_user_id)
    if not note and target_user_id and APP_LOOP and APP_BOT:
        try:
            note = asyncio.run_coroutine_threadsafe(
                fetch_user_bio_from_telegram(APP_BOT, target_user_id),
                APP_LOOP,
            ).result(timeout=4)
        except Exception:
            note = ""
    payload["profile_note"] = note
    return jsonify({"ok": True, "data": payload})


@health_app.post("/api/miniapp/profile-note")
def api_miniapp_profile_note():
    user = get_webapp_user(request, BOT_TOKEN, INITDATA_MAX_AGE_SECONDS)
    if not user:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    body = request.get_json(silent=True) or {}
    note = str(body.get("note") or "").strip()
    if len(note) > 90:
        return jsonify({"ok": False, "error": "Максимум 90 символов"}), 400
    lowered = note.lower()
    if (
        "http://" in lowered
        or "https://" in lowered
        or "www." in lowered
        or "t.me/" in lowered
    ):
        return jsonify({"ok": False, "error": "Ссылки в описании запрещены"}), 400
    db.set_profile_note(int(user.get("id")), note)
    return jsonify({"ok": True, "note": note})


@health_app.get("/api/miniapp/avatar")
def api_miniapp_avatar():
    username = str(request.args.get("username") or "").strip().lstrip("@").lower()
    if not username:
        return Response(status=400)
    if APP_LOOP is None or APP_BOT is None:
        return Response(status=503)
    try:
        result = asyncio.run_coroutine_threadsafe(
            fetch_avatar_from_telegram(APP_BOT, username),
            APP_LOOP,
        ).result(timeout=8)
    except Exception:
        return Response(status=504)
    if not result:
        return Response(status=404)
    content, content_type = result
    resp = Response(content, status=200, mimetype=content_type)
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return resp


@health_app.get("/api/miniapp/insight")
def api_miniapp_insight():
    user = get_webapp_user(request, BOT_TOKEN, INITDATA_MAX_AGE_SECONDS)
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
    user = get_webapp_user(request, BOT_TOKEN, INITDATA_MAX_AGE_SECONDS)
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


@health_app.post("/api/miniapp/feedback")
def api_miniapp_feedback():
    user = get_webapp_user(request, BOT_TOKEN, INITDATA_MAX_AGE_SECONDS)
    if not user:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    if APP_LOOP is None or APP_BOT is None:
        return jsonify({"ok": False, "error": "service_unavailable"}), 503

    data = request.get_json(silent=True) or {}
    target = normalize_username(str(data.get("target") or ""))
    if not target:
        return jsonify({"ok": False, "error": "Нужен корректный @username"}), 400
    try:
        allowed, reason = asyncio.run_coroutine_threadsafe(
            get_push_manager().validate_feedback_target(APP_BOT, target),
            APP_LOOP,
        ).result(timeout=5)
    except Exception:
        return jsonify({"ok": False, "error": "Не удалось проверить username, попробуй позже."}), 503
    if not allowed:
        return jsonify({"ok": False, "error": reason}), 400

    tone = normalize_feedback_value(str(data.get("tone") or ""), {"easy", "serious"}, "serious")
    speed = normalize_feedback_value(str(data.get("speed") or ""), {"fast", "slow"}, "slow")
    contact_format = normalize_feedback_value(str(data.get("contact_format") or ""), {"text", "live"}, "text")
    initiative = normalize_feedback_value(str(data.get("initiative") or ""), {"self", "wait"}, "wait")
    start_context = normalize_feedback_value(str(data.get("start_context") or ""), {"topic", "direct"}, "topic")
    attention_reaction = normalize_feedback_value(str(data.get("attention_reaction") or ""), {"likes", "careful"}, "careful")
    caution = normalize_feedback_value(str(data.get("caution") or ""), {"true", "false"}, "false")
    frequency = normalize_feedback_value(str(data.get("frequency") or ""), {"often", "rare"}, "rare")
    comm_format = normalize_feedback_value(str(data.get("comm_format") or ""), {"informal", "reserved"}, "reserved")
    emotion_tone = normalize_feedback_value(str(data.get("emotion_tone") or ""), {"warm", "neutral"}, "neutral")
    feedback_style = normalize_feedback_value(str(data.get("feedback_style") or ""), {"direct", "soft"}, "soft")
    uncertainty = normalize_feedback_value(str(data.get("uncertainty") or ""), {"low", "high"}, "high")
    voter_id = int(user.get("id"))
    username = str(user.get("username") or "").strip().lower()
    if username:
        is_new = db.upsert_user_with_flag(
            voter_id,
            f"@{username}",
            str(user.get("first_name") or ""),
            str(user.get("last_name") or ""),
            str(user.get("photo_url") or ""),
        )
        if is_new and APP_BOT:
            queue_coroutine(notify_admin_new_user(APP_BOT, voter_id, f"@{username}", "miniapp"))

    future = asyncio.run_coroutine_threadsafe(
        get_push_manager().process_feedback_submission(
            APP_BOT,
            target,
            voter_id,
            tone,
            speed,
            contact_format,
            initiative,
            start_context,
            attention_reaction,
            caution,
            frequency,
            comm_format,
            emotion_tone,
            feedback_style,
            uncertainty,
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


async def notify_admin_new_user(bot: Bot, user_id: int, username: str, source: str) -> None:
    admin_id = await db_call(db.get_user_id_by_username, f"@{ADMIN_USERNAME}")
    if not admin_id or admin_id == user_id:
        return
    text = (
        "Новый пользователь в приложении.\n"
        f"Username: {username}\n"
        f"ID: {user_id}\n"
        f"Источник: {source}"
    )
    try:
        await asyncio.wait_for(bot.send_message(admin_id, text), timeout=3.0)
    except Exception:
        pass


async def upsert_user_and_maybe_notify(
    bot: Bot,
    user_id: int,
    username: str,
    first_name: str = "",
    last_name: str = "",
    photo_url: str = "",
    source: str = "bot",
) -> None:
    is_new = await db_call(
        db.upsert_user_with_flag,
        user_id,
        username,
        first_name,
        last_name,
        photo_url,
    )
    if is_new:
        await notify_admin_new_user(bot, user_id, username, source)


def register_user(message: types.Message) -> None:
    if message.from_user and message.from_user.id and message.from_user.username and message.bot:
        asyncio.create_task(
            upsert_user_and_maybe_notify(
                message.bot,
                message.from_user.id,
                f"@{message.from_user.username.lower()}",
                str(message.from_user.first_name or ""),
                str(message.from_user.last_name or ""),
                "",
                "bot",
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


def queue_coroutine(coro) -> None:
    if APP_LOOP is None:
        return
    try:
        asyncio.run_coroutine_threadsafe(coro, APP_LOOP)
    except Exception:
        pass


PUSH_MANAGER: Optional[PushManager] = None


def get_push_manager() -> PushManager:
    global PUSH_MANAGER
    if PUSH_MANAGER is None:
        PUSH_MANAGER = PushManager(
            db_call=db_call,
            queue_coroutine=queue_coroutine,
            build_profile_payload=build_profile_payload,
            admin_username=ADMIN_USERNAME,
            push_timeout_seconds=PUSH_TIMEOUT_SECONDS,
        )
    return PUSH_MANAGER

@router.message(CommandStart())
async def cmd_start(message: types.Message, command: CommandObject):
    register_user(message)
    payload = command.args or ""
    ref_target: Optional[str] = None
    if payload.startswith("ref_") and message.from_user and message.from_user.id:
        raw = payload[4:]
        target = normalize_username(f"@{raw}") if not raw.startswith("@") else normalize_username(raw)
        if target:
            ref_target = target
            target_user_id = await db_call(db.get_user_id_by_username, target)
            await db_call(db.add_ref_visit, target, message.from_user.id, target_user_id)

    launch_kb = build_launch_kb(MINI_APP_URL, ref_target)
    if launch_kb:
        await message.answer("Открой приложение и оставь анонимный ответ 👇", reply_markup=launch_kb)
    else:
        await message.answer("Mini App временно недоступен. Обратись к администратору.")


@router.message(Command("ref"))
async def cmd_ref(message: types.Message):
    launch_kb = build_launch_kb(MINI_APP_URL)
    if launch_kb:
        await message.answer("Создание ссылок теперь в Mini App 👇", reply_markup=launch_kb)
    else:
        await message.answer("Mini App временно недоступен.")


@router.message(Command("stats"))
async def cmd_stats(message: types.Message):
    launch_kb = build_launch_kb(MINI_APP_URL)
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
    launch_kb = build_launch_kb(MINI_APP_URL)
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
