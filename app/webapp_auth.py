import hashlib
import hmac
import json
import time
from typing import Optional
from urllib.parse import parse_qsl

from flask import Request


def verify_telegram_init_data(
    init_data: str,
    bot_token: str,
    max_age_seconds: int,
) -> Optional[dict]:
    if not init_data:
        return None
    pairs = dict(parse_qsl(init_data, keep_blank_values=True))
    provided_hash = pairs.pop("hash", None)
    if not provided_hash:
        return None

    data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
    secret = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
    calculated_hash = hmac.new(secret, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calculated_hash, provided_hash):
        return None

    auth_date_raw = pairs.get("auth_date")
    try:
        auth_date = int(auth_date_raw) if auth_date_raw else 0
    except ValueError:
        return None
    if auth_date <= 0 or abs(int(time.time()) - auth_date) > max_age_seconds:
        return None

    user_raw = pairs.get("user")
    if not user_raw:
        return None
    try:
        user = json.loads(user_raw)
    except json.JSONDecodeError:
        return None
    return user if isinstance(user, dict) else None


def get_webapp_user(request: Request, bot_token: str, max_age_seconds: int) -> Optional[dict]:
    init_data = request.headers.get("X-Telegram-Init-Data", "")
    return verify_telegram_init_data(init_data, bot_token, max_age_seconds)


def build_avatar_proxy_url(username: str) -> str:
    uname = username.lstrip("@").lower()
    return f"/api/miniapp/avatar?username={uname}"

