import os
import sqlite3
from pathlib import Path
from typing import List, Optional, Tuple

DB_PATH = Path("data.sqlite3")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
USE_POSTGRES = DATABASE_URL.lower().startswith("postgres")

if USE_POSTGRES:
    import psycopg
    from psycopg import errors as pg_errors


def _get_sqlite_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    return conn


def _get_pg_conn():
    return psycopg.connect(DATABASE_URL)


def init_db() -> None:
    if USE_POSTGRES:
        conn = _get_pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS votes (
                        id SERIAL PRIMARY KEY,
                        target TEXT NOT NULL,
                        label TEXT NOT NULL,
                        voter_id BIGINT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        user_id BIGINT PRIMARY KEY,
                        username TEXT NOT NULL UNIQUE,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_votes_unique
                    ON votes (target, voter_id)
                    WHERE voter_id IS NOT NULL
                    """
                )
                conn.commit()
        finally:
            conn.close()
    else:
        conn = _get_sqlite_conn()
        try:
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
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_votes_unique
                ON votes (target, voter_id)
                WHERE voter_id IS NOT NULL
                """
            )
            conn.commit()
        finally:
            conn.close()


def add_vote(target: str, label: str, voter_id: Optional[int]) -> bool:
    if USE_POSTGRES:
        conn = _get_pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO votes (target, label, voter_id) VALUES (%s, %s, %s)",
                    (target, label, voter_id),
                )
                conn.commit()
            return True
        except pg_errors.UniqueViolation:
            conn.rollback()
            return False
        finally:
            conn.close()
    else:
        conn = _get_sqlite_conn()
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


def upsert_user(user_id: int, username: str) -> None:
    if USE_POSTGRES:
        conn = _get_pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO users (user_id, username, updated_at)
                    VALUES (%s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT(user_id) DO UPDATE SET
                        username = EXCLUDED.username,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (user_id, username),
                )
                conn.commit()
        finally:
            conn.close()
    else:
        conn = _get_sqlite_conn()
        try:
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
        finally:
            conn.close()


def get_user_id_by_username(username: str) -> Optional[int]:
    if USE_POSTGRES:
        conn = _get_pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT user_id FROM users WHERE username = %s", (username,))
                row = cur.fetchone()
        finally:
            conn.close()
    else:
        conn = _get_sqlite_conn()
        try:
            cur = conn.execute("SELECT user_id FROM users WHERE username = ?", (username,))
            row = cur.fetchone()
        finally:
            conn.close()

    if not row:
        return None
    return int(row[0])


def get_stats(target: str) -> List[Tuple[str, int]]:
    if USE_POSTGRES:
        conn = _get_pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT label, COUNT(*) FROM votes WHERE target = %s GROUP BY label",
                    (target,),
                )
                rows = cur.fetchall()
        finally:
            conn.close()
    else:
        conn = _get_sqlite_conn()
        try:
            cur = conn.execute(
                "SELECT label, COUNT(*) FROM votes WHERE target = ? GROUP BY label",
                (target,),
            )
            rows = cur.fetchall()
        finally:
            conn.close()
    return [(row[0], int(row[1])) for row in rows]


def get_total(target: str) -> int:
    if USE_POSTGRES:
        conn = _get_pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM votes WHERE target = %s", (target,))
                total = cur.fetchone()[0]
        finally:
            conn.close()
    else:
        conn = _get_sqlite_conn()
        try:
            cur = conn.execute("SELECT COUNT(*) FROM votes WHERE target = ?", (target,))
            total = cur.fetchone()[0]
        finally:
            conn.close()
    return int(total)


def count_users() -> int:
    if USE_POSTGRES:
        conn = _get_pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM users")
                total = cur.fetchone()[0]
        finally:
            conn.close()
    else:
        conn = _get_sqlite_conn()
        try:
            cur = conn.execute("SELECT COUNT(*) FROM users")
            total = cur.fetchone()[0]
        finally:
            conn.close()
    return int(total)


def count_votes() -> int:
    if USE_POSTGRES:
        conn = _get_pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM votes")
                total = cur.fetchone()[0]
        finally:
            conn.close()
    else:
        conn = _get_sqlite_conn()
        try:
            cur = conn.execute("SELECT COUNT(*) FROM votes")
            total = cur.fetchone()[0]
        finally:
            conn.close()
    return int(total)


def top_voters(limit: int = 10) -> List[Tuple[Optional[str], int]]:
    if USE_POSTGRES:
        conn = _get_pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT u.username, COUNT(v.id) as cnt
                    FROM votes v
                    LEFT JOIN users u ON u.user_id = v.voter_id
                    WHERE v.voter_id IS NOT NULL
                    GROUP BY v.voter_id, u.username
                    ORDER BY cnt DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
        finally:
            conn.close()
    else:
        conn = _get_sqlite_conn()
        try:
            cur = conn.execute(
                """
                SELECT u.username, COUNT(v.id) as cnt
                FROM votes v
                LEFT JOIN users u ON u.user_id = v.voter_id
                WHERE v.voter_id IS NOT NULL
                GROUP BY v.voter_id
                ORDER BY cnt DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = cur.fetchall()
        finally:
            conn.close()
    return [(row[0], int(row[1])) for row in rows]


def top_targets(limit: int = 10) -> List[Tuple[str, int]]:
    if USE_POSTGRES:
        conn = _get_pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT target, COUNT(id) as cnt
                    FROM votes
                    GROUP BY target
                    ORDER BY cnt DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
        finally:
            conn.close()
    else:
        conn = _get_sqlite_conn()
        try:
            cur = conn.execute(
                """
                SELECT target, COUNT(id) as cnt
                FROM votes
                GROUP BY target
                ORDER BY cnt DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = cur.fetchall()
        finally:
            conn.close()
    return [(row[0], int(row[1])) for row in rows]
