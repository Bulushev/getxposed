import sqlite3
from pathlib import Path
from typing import List, Optional, Tuple

DB_PATH = Path("data.sqlite3")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target TEXT NOT NULL,
            label TEXT NOT NULL,
            voter_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    # Enforce one vote per (voter_id, target). Anonymous voting still uses voter_id.
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_votes_unique
        ON votes (target, voter_id)
        WHERE voter_id IS NOT NULL
        """
    )
    return conn


def upsert_user(user_id: int, username: str) -> None:
    conn = get_conn()
    with conn:
        conn.execute(
            """
            INSERT INTO users (user_id, username, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_id, username),
        )
    conn.close()


def get_user_id_by_username(username: str) -> Optional[int]:
    conn = get_conn()
    cur = conn.execute(
        "SELECT user_id FROM users WHERE username = ?",
        (username,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return int(row[0])


def add_vote(target: str, label: str, voter_id: Optional[int]) -> bool:
    conn = get_conn()
    try:
        with conn:
            conn.execute(
                "INSERT INTO votes (target, label, voter_id) VALUES (?, ?, ?)",
                (target, label, voter_id),
            )
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def get_stats(target: str) -> List[Tuple[str, int]]:
    conn = get_conn()
    cur = conn.execute(
        "SELECT label, COUNT(*) FROM votes WHERE target = ? GROUP BY label",
        (target,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_total(target: str) -> int:
    conn = get_conn()
    cur = conn.execute(
        "SELECT COUNT(*) FROM votes WHERE target = ?",
        (target,),
    )
    total = cur.fetchone()[0]
    conn.close()
    return int(total)
