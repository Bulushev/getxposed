import asyncio
import io
from typing import Optional

from aiogram import Bot


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


async def fetch_user_bio_from_telegram(bot: Bot, user_id: int) -> str:
    try:
        chat = await asyncio.wait_for(bot.get_chat(user_id), timeout=3.0)
    except Exception:
        return ""
    bio = str(getattr(chat, "bio", "") or "").strip()
    if not bio:
        return ""
    return bio[:90]


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

