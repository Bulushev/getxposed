import asyncio
import hashlib
import hmac
import io
import json
import logging
import os
import random
import re
import time
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from typing import Optional

from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.filters import Command, CommandStart
from aiogram.filters.command import CommandObject
from aiogram.utils.keyboard import InlineKeyboardBuilder
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, render_template, request

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
PUSH_TIMEOUT_SECONDS = 15.0


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


def pick_majority(dimensions: dict[str, dict[str, int]], field: str, left: str, right: str) -> str:
    counts = dimensions.get(field, {})
    return left if counts.get(left, 0) >= counts.get(right, 0) else right


def build_answer_cards(dimensions: dict[str, dict[str, int]]) -> list[dict]:
    spec = [
        ("style", "Как лучше начать", "tone", "easy", "serious", "🙂 с шутки", "🧠 по делу"),
        ("tempo", "Темп", "speed", "fast", "slow", "🔥 сразу", "🐢 не спеша"),
        ("channel", "Канал", "contact_format", "text", "live", "💬 в переписке", "🎤 вживую"),
        ("initiative", "Первый шаг", "initiative", "self", "wait", "👉 ему/ей ок, если напишут", "👀 лучше, если сначала присмотрятся"),
        ("start_context", "Контекст старта", "start_context", "topic", "direct", "🌱 с лёгкого", "🎯 сразу по сути"),
        ("first_reaction", "Реакция в начале", "attention_reaction", "likes", "careful", "😊 быстро включается", "😶 сначала смотрит"),
        ("pressure", "Давление", "caution", "false", "true", "🫶 можно активнее", "⚠️ лучше аккуратно"),
        ("frequency", "Частота", "frequency", "often", "rare", "📬 можно часто", "🕰 лучше редко"),
        ("tone", "Тон общения", "comm_format", "informal", "reserved", "😄 свободно", "🤝 сдержанно"),
        ("vibe", "Вайб", "emotion_tone", "warm", "neutral", "☀️ легко", "🌙 спокойно"),
        ("dialog", "Диалог", "feedback_style", "direct", "soft", "💬 любит обсуждать", "👂 больше слушает"),
        ("certainty", "Неопределённость", "uncertainty", "low", "high", "🧭 нормально", "🚧 лучше конкретно"),
    ]
    cards: list[dict] = []
    for key, title, field, left_key, right_key, left_text, right_text in spec:
        pick = pick_majority(dimensions, field, left_key, right_key)
        cards.append(
            {
                "id": key,
                "title": title,
                "value": left_text if pick == left_key else right_text,
            }
        )
    return cards


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
    payload["profile_note"] = db.get_profile_note(user_id)
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
                "invite_link": f"https://t.me/{get_bot_public_username()}",
                "is_app_user": True,
                "profile_note": "Люблю спокойное и уважительное общение.",
                "answer_cards": [
                    {"id": "style", "title": "Стиль входа", "value": "🙂 с шутки"},
                    {"id": "tempo", "title": "Темп", "value": "🐢 не спеша"},
                    {"id": "channel", "title": "Канал", "value": "💬 в переписке"},
                    {"id": "initiative", "title": "Первый шаг", "value": "👀 лучше, если сначала присмотрятся"},
                ],
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
    user = get_webapp_user()
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
    payload["profile_note"] = db.get_profile_note(int(payload["user"].get("id") or 0))
    return jsonify({"ok": True, "data": payload})


@health_app.post("/api/miniapp/profile-note")
def api_miniapp_profile_note():
    user = get_webapp_user()
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
    try:
        allowed, reason = asyncio.run_coroutine_threadsafe(
            validate_feedback_target(APP_BOT, target),
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
        process_feedback_submission(
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


def with_rate_param(url: str, target: Optional[str]) -> str:
    if not target:
        return url
    uname = target.lstrip("@").lower()
    if not uname:
        return url
    parts = urlsplit(url)
    pairs = list(parse_qsl(parts.query, keep_blank_values=True))
    pairs = [(k, v) for (k, v) in pairs if k != "rate"]
    pairs.append(("rate", uname))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(pairs), parts.fragment))


def build_launch_kb(prefill_target: Optional[str] = None) -> Optional[types.InlineKeyboardMarkup]:
    if not MINI_APP_URL:
        return None
    app_url = with_rate_param(MINI_APP_URL, prefill_target)
    kb = InlineKeyboardBuilder()
    kb.button(text="Открыть приложение", web_app=types.WebAppInfo(url=app_url))
    return kb.as_markup()


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


def build_avatar_proxy_url(username: str) -> str:
    uname = username.lstrip("@").lower()
    return f"/api/miniapp/avatar?username={uname}"


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
        "answer_cards": [],
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
    result["answer_cards"] = build_answer_cards(dimensions)
    return result


def queue_coroutine(coro) -> None:
    if APP_LOOP is None:
        return
    try:
        asyncio.run_coroutine_threadsafe(coro, APP_LOOP)
    except Exception:
        pass


async def fetch_public_user_from_telegram(bot: Bot, target: str) -> Optional[dict]:
    try:
        chat = await asyncio.wait_for(bot.get_chat(target), timeout=3.0)
    except Exception:
        return None
    if chat.type != "private":
        return None
    username = (chat.username or target.lstrip("@")).lower()
    return {
        "id": int(chat.id),
        "username": username,
        "first_name": str(chat.first_name or ""),
        "last_name": str(chat.last_name or ""),
        "photo_url": "",
    }


async def fetch_avatar_from_telegram(bot: Bot, username: str) -> Optional[tuple[bytes, str]]:
    target = f"@{username.lstrip('@').lower()}"
    try:
        chat = await asyncio.wait_for(bot.get_chat(target), timeout=3.0)
    except Exception:
        return None
    if chat.type != "private" or not chat.photo or not chat.photo.big_file_id:
        return None
    try:
        file = await asyncio.wait_for(bot.get_file(chat.photo.big_file_id), timeout=3.0)
        buf = io.BytesIO()
        await asyncio.wait_for(bot.download(file, destination=buf), timeout=5.0)
        content = buf.getvalue()
        if not content:
            return None
        path = (file.file_path or "").lower()
        if path.endswith(".png"):
            ctype = "image/png"
        elif path.endswith(".webp"):
            ctype = "image/webp"
        else:
            ctype = "image/jpeg"
        return content, ctype
    except Exception:
        return None


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
        await asyncio.wait_for(bot.send_message(target_id, text), timeout=PUSH_TIMEOUT_SECONDS)
        return True
    except Exception as exc:
        target_username = (await db_call(db.get_username_by_user_id, target_id)) or f"id={target_id}"
        reason = f"{type(exc).__name__}: {exc}"
        reason_l = reason.lower()
        should_delete = (
            "bot was blocked by the user" in reason_l
            or "chat not found" in reason_l
            or "user is deactivated" in reason_l
            or "forbidden" in reason_l
        )
        if should_delete:
            await db_call(db.delete_user_by_user_id, target_id)

        admin_id = await db_call(db.get_user_id_by_username, f"@{ADMIN_USERNAME}")
        if admin_id:
            try:
                await asyncio.wait_for(
                    bot.send_message(
                        admin_id,
                        "Не удалось отправить push пользователю.\n"
                        f"Пользователь: {target_username}\n"
                        f"Причина: {reason}\n"
                        + ("Пользователь удалён из /users." if should_delete else "Пользователь НЕ удалён (временная ошибка)."),
                    ),
                    timeout=PUSH_TIMEOUT_SECONDS,
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
    initiative: str,
    start_context: str,
    attention_reaction: str,
    caution: str,
    frequency: str,
    comm_format: str,
    emotion_tone: str,
    feedback_style: str,
    uncertainty: str,
) -> tuple[Optional[str], str]:
    before_total = await db_call(db.get_total, target)
    before_dimensions = await db_call(db.get_contact_dimensions, target)
    rec_before = pick_recommendation(before_dimensions)
    result = await db_call(
        db.add_vote,
        target,
        "feedback",
        voter_id,
        tone,
        speed,
        contact_format,
        caution,
        initiative,
        start_context,
        attention_reaction,
        frequency,
        comm_format,
        emotion_tone,
        feedback_style,
        uncertainty,
    )
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


async def validate_feedback_target(bot: Bot, target: str) -> tuple[bool, Optional[str]]:
    username = target.lstrip("@").lower()
    if username.endswith("bot"):
        return False, "Нельзя оставлять отзывы о ботах."
    try:
        chat = await asyncio.wait_for(bot.get_chat(target), timeout=3.0)
    except Exception:
        return True, None
    if chat.type in {"group", "supergroup"}:
        return False, "Нельзя оставлять отзывы о чатах."
    if chat.type == "channel":
        return False, "Нельзя оставлять отзывы о каналах."
    return True, None


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
            inserted = await db_call(db.add_ref_visit, target, message.from_user.id)
            if inserted and APP_BOT:
                owner = await db_call(db.get_user_public_by_username, target)
                target_user_id = int(owner.get("id") or 0) if owner else 0
                if target_user_id and target_user_id != message.from_user.id:
                    queue_coroutine(
                        send_tracked_push(
                            APP_BOT,
                            target_user_id,
                            "🔥 похоже, ты запустил небольшую цепную реакцию.\n\nпо твоей ссылке пришёл новый человек 👀",
                        )
                    )

    launch_kb = build_launch_kb(ref_target)
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
