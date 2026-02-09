import asyncio
import os
import sqlite3
from pathlib import Path

from aiogram import Bot

DB_PATH = Path("data.sqlite3")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    raise SystemExit("BOT_TOKEN is not set")

LABEL = "💔 краш"
MESSAGE = "Тебя оценили: 💔 краш\n\n💔 кто-то в тебя втрескался.\nи явно не собирается признаваться 🙂"

RATE_DELAY = 0.08  # ~12-13 msgs/sec


def iter_users_sqlite():
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.execute("SELECT user_id, username FROM users")
        for row in cur.fetchall():
            yield int(row[0]), row[1]
    finally:
        conn.close()


def iter_users_postgres():
    import psycopg

    conn = psycopg.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, username FROM users")
            for row in cur.fetchall():
                yield int(row[0]), row[1]
    finally:
        conn.close()


def add_vote_sqlite(target: str):
    conn = sqlite3.connect(DB_PATH)
    try:
        with conn:
            conn.execute(
                "INSERT OR IGNORE INTO votes (target, label, voter_id) VALUES (?, ?, ?)",
                (target, LABEL, 0),
            )
    finally:
        conn.close()


def add_vote_postgres(target: str):
    import psycopg

    conn = psycopg.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO votes (target, label, voter_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                (target, LABEL, 0),
            )
            conn.commit()
    finally:
        conn.close()


async def main():
    bot = Bot(BOT_TOKEN)

    if DATABASE_URL.lower().startswith("postgres"):
        users = list(iter_users_postgres())
        add_vote = add_vote_postgres
    else:
        users = list(iter_users_sqlite())
        add_vote = add_vote_sqlite

    sent = 0
    for user_id, username in users:
        target = username if username.startswith("@") else f"@{username}"
        add_vote(target)
        try:
            await bot.send_message(user_id, MESSAGE)
            sent += 1
        except Exception:
            pass
        await asyncio.sleep(RATE_DELAY)

    print(f"done. users={len(users)} sent={sent}")


if __name__ == "__main__":
    asyncio.run(main())
