import logging
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

DB_PATH = Path("data.sqlite3")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
USE_POSTGRES = DATABASE_URL.lower().startswith("postgres")

if USE_POSTGRES:
    import psycopg


def _get_sqlite_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    return conn


def _get_pg_conn():
    return psycopg.connect(DATABASE_URL, connect_timeout=5)


def init_db() -> bool:
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS votes (
                            id SERIAL PRIMARY KEY,
                            target TEXT NOT NULL,
                            label TEXT NOT NULL,
                            tone TEXT DEFAULT 'serious',
                            speed TEXT DEFAULT 'slow',
                            contact_format TEXT DEFAULT 'text',
                            caution TEXT DEFAULT 'false',
                            voter_id BIGINT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                        """
                    )
                    cur.execute("ALTER TABLE votes ADD COLUMN IF NOT EXISTS tone TEXT DEFAULT 'serious'")
                    cur.execute("ALTER TABLE votes ADD COLUMN IF NOT EXISTS speed TEXT DEFAULT 'slow'")
                    cur.execute("ALTER TABLE votes ADD COLUMN IF NOT EXISTS contact_format TEXT DEFAULT 'text'")
                    cur.execute("ALTER TABLE votes ADD COLUMN IF NOT EXISTS caution TEXT DEFAULT 'false'")
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
                        CREATE TABLE IF NOT EXISTS ref_visits (
                            id SERIAL PRIMARY KEY,
                            target TEXT NOT NULL,
                            visitor_id BIGINT NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                        """
                    )
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS seen_hints (
                            target TEXT NOT NULL,
                            watcher_id BIGINT NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            PRIMARY KEY (target, watcher_id)
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
                    cur.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_ref_unique
                        ON ref_visits (target, visitor_id)
                        """
                    )
                conn.commit()
            finally:
                conn.close()
            return True
        except Exception as exc:
            logging.warning("DB init failed: %s", exc)
            return False
    else:
        conn = _get_sqlite_conn()
        try:
            conn.execute(
                """
                    CREATE TABLE IF NOT EXISTS votes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        target TEXT NOT NULL,
                        label TEXT NOT NULL,
                        tone TEXT DEFAULT 'serious',
                        speed TEXT DEFAULT 'slow',
                        contact_format TEXT DEFAULT 'text',
                        caution TEXT DEFAULT 'false',
                        voter_id INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            try:
                conn.execute("ALTER TABLE votes ADD COLUMN tone TEXT DEFAULT 'serious'")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE votes ADD COLUMN speed TEXT DEFAULT 'slow'")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE votes ADD COLUMN contact_format TEXT DEFAULT 'text'")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE votes ADD COLUMN caution TEXT DEFAULT 'false'")
            except sqlite3.OperationalError:
                pass
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
                CREATE TABLE IF NOT EXISTS ref_visits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target TEXT NOT NULL,
                    visitor_id INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS seen_hints (
                    target TEXT NOT NULL,
                    watcher_id INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (target, watcher_id)
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
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_ref_unique
                ON ref_visits (target, visitor_id)
                """
            )
            conn.commit()
        finally:
            conn.close()
        return True


def add_vote(
    target: str,
    label: str,
    voter_id: Optional[int],
    tone: str = "serious",
    speed: str = "slow",
    contact_format: str = "text",
    caution: str = "false",
) -> Optional[str]:
    cooldown = timedelta(hours=24)

    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    if voter_id is None:
                        cur.execute(
                            "INSERT INTO votes (target, label, tone, speed, contact_format, caution, voter_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            (target, label, tone, speed, contact_format, caution, voter_id),
                        )
                        conn.commit()
                        return "inserted"

                    cur.execute(
                        """
                        SELECT id, created_at
                        FROM votes
                        WHERE target = %s AND voter_id = %s
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                        (target, voter_id),
                    )
                    row = cur.fetchone()
                    if not row:
                        cur.execute(
                            "INSERT INTO votes (target, label, tone, speed, contact_format, caution, voter_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            (target, label, tone, speed, contact_format, caution, voter_id),
                        )
                        conn.commit()
                        return "inserted"

                    vote_id = int(row[0])
                    last_ts = row[1]
                    if isinstance(last_ts, datetime) and datetime.utcnow() - last_ts.replace(tzinfo=None) >= cooldown:
                        cur.execute(
                            """
                            UPDATE votes
                            SET label = %s,
                                tone = %s,
                                speed = %s,
                                contact_format = %s,
                                caution = %s,
                                created_at = CURRENT_TIMESTAMP
                            WHERE id = %s
                            """,
                            (label, tone, speed, contact_format, caution, vote_id),
                        )
                        conn.commit()
                        return "updated"

                    return "duplicate_recent"
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB add_vote failed: %s", exc)
            return None
    else:
        conn = _get_sqlite_conn()
        try:
            with conn:
                if voter_id is None:
                    conn.execute(
                        "INSERT INTO votes (target, label, tone, speed, contact_format, caution, voter_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (target, label, tone, speed, contact_format, caution, voter_id),
                    )
                    return "inserted"

                cur = conn.execute(
                    """
                    SELECT id, created_at
                    FROM votes
                    WHERE target = ? AND voter_id = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (target, voter_id),
                )
                row = cur.fetchone()
                if not row:
                    conn.execute(
                        "INSERT INTO votes (target, label, tone, speed, contact_format, caution, voter_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (target, label, tone, speed, contact_format, caution, voter_id),
                    )
                    return "inserted"

                vote_id = int(row[0])
                ts_raw = row[1]
                try:
                    last_ts = datetime.fromisoformat(str(ts_raw))
                except ValueError:
                    last_ts = datetime.strptime(str(ts_raw), "%Y-%m-%d %H:%M:%S")

                if datetime.utcnow() - last_ts >= cooldown:
                    conn.execute(
                        """
                        UPDATE votes
                        SET label = ?,
                            tone = ?,
                            speed = ?,
                            contact_format = ?,
                            caution = ?,
                            created_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (label, tone, speed, contact_format, caution, vote_id),
                    )
                    return "updated"

                return "duplicate_recent"
        except sqlite3.IntegrityError:
            return "duplicate_recent"
        finally:
            conn.close()


def upsert_user(user_id: int, username: str) -> None:
    if USE_POSTGRES:
        try:
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
        except Exception as exc:
            logging.warning("DB upsert_user failed: %s", exc)
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


def add_ref_visit(target: str, visitor_id: int) -> None:
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO ref_visits (target, visitor_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (target, visitor_id),
                    )
                    conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB add_ref_visit failed: %s", exc)
    else:
        conn = _get_sqlite_conn()
        try:
            with conn:
                conn.execute(
                    "INSERT OR IGNORE INTO ref_visits (target, visitor_id) VALUES (?, ?)",
                    (target, visitor_id),
                )
        finally:
            conn.close()


def count_ref_visitors(target: str) -> int:
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM ref_visits WHERE target = %s",
                        (target,),
                    )
                    total = cur.fetchone()[0]
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB count_ref_visitors failed: %s", exc)
            return 0
    else:
        conn = _get_sqlite_conn()
        try:
            cur = conn.execute(
                "SELECT COUNT(*) FROM ref_visits WHERE target = ?",
                (target,),
            )
            total = cur.fetchone()[0]
        finally:
            conn.close()
    return int(total)


def mark_seen_hint_sent(target: str, watcher_id: int) -> bool:
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO seen_hints (target, watcher_id)
                        VALUES (%s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (target, watcher_id),
                    )
                    inserted = cur.rowcount > 0
                    conn.commit()
                return inserted
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB mark_seen_hint_sent failed: %s", exc)
            return False
    else:
        conn = _get_sqlite_conn()
        try:
            with conn:
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO seen_hints (target, watcher_id)
                    VALUES (?, ?)
                    """,
                    (target, watcher_id),
                )
                return cur.rowcount > 0
        finally:
            conn.close()


def get_user_id_by_username(username: str) -> Optional[int]:
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT user_id FROM users WHERE username = %s", (username,))
                    row = cur.fetchone()
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB get_user_id_by_username failed: %s", exc)
            return None
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


def get_total(target: str) -> int:
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM votes WHERE target = %s AND label = 'feedback'",
                        (target,),
                    )
                    total = cur.fetchone()[0]
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB get_total failed: %s", exc)
            return 0
    else:
        conn = _get_sqlite_conn()
        try:
            cur = conn.execute(
                "SELECT COUNT(*) FROM votes WHERE target = ? AND label = 'feedback'",
                (target,),
            )
            total = cur.fetchone()[0]
        finally:
            conn.close()
    return int(total)


def count_users() -> int:
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM users")
                    total = cur.fetchone()[0]
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB count_users failed: %s", exc)
            return 0
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
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM votes WHERE label = 'feedback'")
                    total = cur.fetchone()[0]
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB count_votes failed: %s", exc)
            return 0
    else:
        conn = _get_sqlite_conn()
        try:
            cur = conn.execute("SELECT COUNT(*) FROM votes WHERE label = 'feedback'")
            total = cur.fetchone()[0]
        finally:
            conn.close()
    return int(total)


def top_voters(limit: int = 10) -> List[Tuple[Optional[str], int]]:
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT u.username, COUNT(v.id) as cnt
                        FROM votes v
                        LEFT JOIN users u ON u.user_id = v.voter_id
                        WHERE v.voter_id IS NOT NULL AND v.label = 'feedback'
                        GROUP BY v.voter_id, u.username
                        ORDER BY cnt DESC
                        LIMIT %s
                        """,
                        (limit,),
                    )
                    rows = cur.fetchall()
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB top_voters failed: %s", exc)
            return []
    else:
        conn = _get_sqlite_conn()
        try:
            cur = conn.execute(
                """
                SELECT u.username, COUNT(v.id) as cnt
                FROM votes v
                LEFT JOIN users u ON u.user_id = v.voter_id
                WHERE v.voter_id IS NOT NULL AND v.label = 'feedback'
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
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                    SELECT target, COUNT(id) as cnt
                    FROM votes
                    WHERE label = 'feedback'
                    GROUP BY target
                        ORDER BY cnt DESC
                        LIMIT %s
                        """,
                        (limit,),
                    )
                    rows = cur.fetchall()
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB top_targets failed: %s", exc)
            return []
    else:
        conn = _get_sqlite_conn()
        try:
            cur = conn.execute(
                """
                SELECT target, COUNT(id) as cnt
                FROM votes
                WHERE label = 'feedback'
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


def list_users(limit: int = 100) -> List[str]:
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT username FROM users ORDER BY updated_at DESC LIMIT %s",
                        (limit,),
                    )
                    rows = cur.fetchall()
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB list_users failed: %s", exc)
            return []
    else:
        conn = _get_sqlite_conn()
        try:
            cur = conn.execute(
                "SELECT username FROM users ORDER BY updated_at DESC LIMIT ?",
                (limit,),
            )
            rows = cur.fetchall()
        finally:
            conn.close()
    return [row[0] for row in rows]


def get_contact_dimensions(target: str) -> dict[str, dict[str, int]]:
    fields = {
        "tone": ("easy", "serious"),
        "speed": ("fast", "slow"),
        "contact_format": ("text", "live"),
        "caution": ("true", "false"),
    }
    result: dict[str, dict[str, int]] = {
        key: {option: 0 for option in options} for key, options in fields.items()
    }

    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    for field, options in fields.items():
                        cur.execute(
                            f"SELECT {field}, COUNT(*) FROM votes WHERE target = %s AND label = 'feedback' GROUP BY {field}",
                            (target,),
                        )
                        for value, cnt in cur.fetchall():
                            if value in options:
                                result[field][str(value)] = int(cnt)
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB get_contact_dimensions failed: %s", exc)
            return result
    else:
        conn = _get_sqlite_conn()
        try:
            for field, options in fields.items():
                cur = conn.execute(
                    f"SELECT {field}, COUNT(*) FROM votes WHERE target = ? AND label = 'feedback' GROUP BY {field}",
                    (target,),
                )
                for value, cnt in cur.fetchall():
                    if value in options:
                        result[field][str(value)] = int(cnt)
        finally:
            conn.close()

    return result
