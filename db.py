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
    return psycopg.connect(DATABASE_URL, connect_timeout=2)


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
                            first_name TEXT DEFAULT '',
                            last_name TEXT DEFAULT '',
                            photo_url TEXT DEFAULT '',
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                        """
                    )
                    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT DEFAULT ''")
                    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_name TEXT DEFAULT ''")
                    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS photo_url TEXT DEFAULT ''")
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
                    first_name TEXT DEFAULT '',
                    last_name TEXT DEFAULT '',
                    photo_url TEXT DEFAULT '',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            try:
                conn.execute("ALTER TABLE users ADD COLUMN first_name TEXT DEFAULT ''")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE users ADD COLUMN last_name TEXT DEFAULT ''")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE users ADD COLUMN photo_url TEXT DEFAULT ''")
            except sqlite3.OperationalError:
                pass
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
                        SELECT id, created_at, label
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
                    old_label = str(row[2]) if row[2] is not None else ""
                    if old_label != "feedback":
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
                            ("feedback", tone, speed, contact_format, caution, vote_id),
                        )
                        conn.commit()
                        return "inserted"

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
                    SELECT id, created_at, label
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
                old_label = str(row[2]) if row[2] is not None else ""
                if old_label != "feedback":
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
                        ("feedback", tone, speed, contact_format, caution, vote_id),
                    )
                    return "inserted"

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


def upsert_user_with_flag(
    user_id: int,
    username: str,
    first_name: str = "",
    last_name: str = "",
    photo_url: str = "",
) -> bool:
    username = username.lower()
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM users WHERE user_id = %s LIMIT 1", (user_id,))
                    existed = cur.fetchone() is not None
                    cur.execute(
                        "DELETE FROM users WHERE LOWER(username) = LOWER(%s) AND user_id <> %s",
                        (username, user_id),
                    )
                    cur.execute(
                        """
                        INSERT INTO users (user_id, username, first_name, last_name, photo_url, updated_at)
                        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT(user_id) DO UPDATE SET
                            username = EXCLUDED.username,
                            first_name = EXCLUDED.first_name,
                            last_name = EXCLUDED.last_name,
                            photo_url = EXCLUDED.photo_url,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        (user_id, username, first_name, last_name, photo_url),
                    )
                    conn.commit()
                    return not existed
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB upsert_user failed: %s", exc)
            return False
    else:
        conn = _get_sqlite_conn()
        try:
            with conn:
                cur = conn.execute("SELECT 1 FROM users WHERE user_id = ? LIMIT 1", (user_id,))
                existed = cur.fetchone() is not None
                conn.execute(
                    "DELETE FROM users WHERE LOWER(username) = LOWER(?) AND user_id <> ?",
                    (username, user_id),
                )
                conn.execute(
                    """
                    INSERT INTO users (user_id, username, first_name, last_name, photo_url, updated_at)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(user_id) DO UPDATE SET
                        username = excluded.username,
                        first_name = excluded.first_name,
                        last_name = excluded.last_name,
                        photo_url = excluded.photo_url,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (user_id, username, first_name, last_name, photo_url),
                )
                return not existed
        finally:
            conn.close()


def upsert_user(
    user_id: int,
    username: str,
    first_name: str = "",
    last_name: str = "",
    photo_url: str = "",
) -> None:
    upsert_user_with_flag(user_id, username, first_name, last_name, photo_url)


def get_user_public_by_username(username: str) -> Optional[dict]:
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT user_id, username, first_name, last_name, photo_url
                        FROM users
                        WHERE LOWER(username) = LOWER(%s)
                        LIMIT 1
                        """,
                        (username,),
                    )
                    row = cur.fetchone()
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB get_user_public_by_username failed: %s", exc)
            return None
    else:
        conn = _get_sqlite_conn()
        try:
            cur = conn.execute(
                """
                SELECT user_id, username, first_name, last_name, photo_url
                FROM users
                WHERE LOWER(username) = LOWER(?)
                LIMIT 1
                """,
                (username,),
            )
            row = cur.fetchone()
        finally:
            conn.close()
    if not row:
        return None
    return {
        "id": int(row[0]),
        "username": str(row[1]).lstrip("@"),
        "first_name": str(row[2] or ""),
        "last_name": str(row[3] or ""),
        "photo_url": str(row[4] or ""),
    }


def normalize_case_data() -> tuple[int, int]:
    """
    Lowercase usernames/targets and collapse users duplicates by case.
    Returns: (users_merged, rows_lowercased)
    """
    users_merged = 0
    rows_lowercased = 0

    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        WITH ranked AS (
                            SELECT user_id,
                                   ROW_NUMBER() OVER (
                                       PARTITION BY LOWER(username)
                                       ORDER BY updated_at DESC, user_id DESC
                                   ) AS rn
                            FROM users
                        )
                        DELETE FROM users u
                        USING ranked r
                        WHERE u.user_id = r.user_id AND r.rn > 1
                        """
                    )
                    users_merged += max(cur.rowcount, 0)

                    cur.execute("UPDATE users SET username = LOWER(username) WHERE username <> LOWER(username)")
                    rows_lowercased += max(cur.rowcount, 0)
                    cur.execute("UPDATE votes SET target = LOWER(target) WHERE target <> LOWER(target)")
                    rows_lowercased += max(cur.rowcount, 0)
                    cur.execute("UPDATE ref_visits SET target = LOWER(target) WHERE target <> LOWER(target)")
                    rows_lowercased += max(cur.rowcount, 0)
                    cur.execute("UPDATE seen_hints SET target = LOWER(target) WHERE target <> LOWER(target)")
                    rows_lowercased += max(cur.rowcount, 0)
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB normalize_case_data failed: %s", exc)
    else:
        conn = _get_sqlite_conn()
        try:
            with conn:
                try:
                    cur = conn.execute(
                        "SELECT user_id, username FROM users ORDER BY updated_at DESC, user_id DESC"
                    )
                    keep_by_lower: dict[str, int] = {}
                    drop_ids: list[int] = []
                    for row in cur.fetchall():
                        uid = int(row[0])
                        uname = str(row[1])
                        key = uname.lower()
                        if key in keep_by_lower:
                            drop_ids.append(uid)
                        else:
                            keep_by_lower[key] = uid
                    for uid in drop_ids:
                        conn.execute("DELETE FROM users WHERE user_id = ?", (uid,))
                    users_merged += len(drop_ids)
                except sqlite3.OperationalError:
                    pass

                try:
                    cur = conn.execute("UPDATE users SET username = LOWER(username) WHERE username <> LOWER(username)")
                    rows_lowercased += max(cur.rowcount, 0)
                except sqlite3.OperationalError:
                    pass
                try:
                    cur = conn.execute("UPDATE votes SET target = LOWER(target) WHERE target <> LOWER(target)")
                    rows_lowercased += max(cur.rowcount, 0)
                except sqlite3.OperationalError:
                    pass
                try:
                    cur = conn.execute("UPDATE ref_visits SET target = LOWER(target) WHERE target <> LOWER(target)")
                    rows_lowercased += max(cur.rowcount, 0)
                except sqlite3.OperationalError:
                    pass
                try:
                    cur = conn.execute("UPDATE seen_hints SET target = LOWER(target) WHERE target <> LOWER(target)")
                    rows_lowercased += max(cur.rowcount, 0)
                except sqlite3.OperationalError:
                    pass
        finally:
            conn.close()

    return users_merged, rows_lowercased


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
                    cur.execute("SELECT user_id FROM users WHERE LOWER(username) = LOWER(%s)", (username,))
                    row = cur.fetchone()
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB get_user_id_by_username failed: %s", exc)
            return None
    else:
        conn = _get_sqlite_conn()
        try:
            cur = conn.execute("SELECT user_id FROM users WHERE LOWER(username) = LOWER(?)", (username,))
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


def search_users(query: str, limit: int = 20) -> List[str]:
    q = query.strip().lower().lstrip("@")
    if not q:
        return []
    pattern = f"@{q}%"
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT username
                        FROM users
                        WHERE LOWER(username) LIKE %s
                        ORDER BY updated_at DESC
                        LIMIT %s
                        """,
                        (pattern, limit),
                    )
                    rows = cur.fetchall()
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB search_users failed: %s", exc)
            return []
    else:
        conn = _get_sqlite_conn()
        try:
            cur = conn.execute(
                """
                SELECT username
                FROM users
                WHERE LOWER(username) LIKE ?
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (pattern, limit),
            )
            rows = cur.fetchall()
        finally:
            conn.close()
    return [str(row[0]) for row in rows]


def list_recent_targets_for_voter(voter_id: int, limit: int = 20) -> List[str]:
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT target
                        FROM votes
                        WHERE voter_id = %s AND label = 'feedback'
                        GROUP BY target
                        ORDER BY MAX(created_at) DESC
                        LIMIT %s
                        """,
                        (voter_id, limit),
                    )
                    rows = cur.fetchall()
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB list_recent_targets_for_voter failed: %s", exc)
            return []
    else:
        conn = _get_sqlite_conn()
        try:
            cur = conn.execute(
                """
                SELECT target
                FROM votes
                WHERE voter_id = ? AND label = 'feedback'
                GROUP BY target
                ORDER BY MAX(created_at) DESC
                LIMIT ?
                """,
                (voter_id, limit),
            )
            rows = cur.fetchall()
        finally:
            conn.close()
    return [str(row[0]) for row in rows]


def get_username_by_user_id(user_id: int) -> Optional[str]:
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT username FROM users WHERE user_id = %s", (user_id,))
                    row = cur.fetchone()
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB get_username_by_user_id failed: %s", exc)
            return None
    else:
        conn = _get_sqlite_conn()
        try:
            cur = conn.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
            row = cur.fetchone()
        finally:
            conn.close()
    if not row:
        return None
    return str(row[0])


def delete_user_by_user_id(user_id: int) -> None:
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM users WHERE user_id = %s", (user_id,))
                    conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB delete_user_by_user_id failed: %s", exc)
    else:
        conn = _get_sqlite_conn()
        try:
            with conn:
                conn.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
        finally:
            conn.close()


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
