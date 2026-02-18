from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from aiogram import types
from aiogram.utils.keyboard import InlineKeyboardBuilder


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


def build_launch_kb(mini_app_url: str, prefill_target: Optional[str] = None) -> Optional[types.InlineKeyboardMarkup]:
    if not mini_app_url:
        return None
    app_url = with_rate_param(mini_app_url, prefill_target)
    kb = InlineKeyboardBuilder()
    kb.button(text="Открыть приложение", web_app=types.WebAppInfo(url=app_url))
    return kb.as_markup()

