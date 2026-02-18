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
                            initiative TEXT DEFAULT 'wait',
                            start_context TEXT DEFAULT 'topic',
                            attention_reaction TEXT DEFAULT 'careful',
                            frequency TEXT DEFAULT 'rare',
                            comm_format TEXT DEFAULT 'reserved',
                            emotion_tone TEXT DEFAULT 'neutral',
                            feedback_style TEXT DEFAULT 'soft',
                            uncertainty TEXT DEFAULT 'high',
                            voter_id BIGINT,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                        """
                    )
                    cur.execute("ALTER TABLE votes ADD COLUMN IF NOT EXISTS tone TEXT DEFAULT 'serious'")
                    cur.execute("ALTER TABLE votes ADD COLUMN IF NOT EXISTS speed TEXT DEFAULT 'slow'")
                    cur.execute("ALTER TABLE votes ADD COLUMN IF NOT EXISTS contact_format TEXT DEFAULT 'text'")
                    cur.execute("ALTER TABLE votes ADD COLUMN IF NOT EXISTS caution TEXT DEFAULT 'false'")
                    cur.execute("ALTER TABLE votes ADD COLUMN IF NOT EXISTS initiative TEXT DEFAULT 'wait'")
                    cur.execute("ALTER TABLE votes ADD COLUMN IF NOT EXISTS start_context TEXT DEFAULT 'topic'")
                    cur.execute("ALTER TABLE votes ADD COLUMN IF NOT EXISTS attention_reaction TEXT DEFAULT 'careful'")
                    cur.execute("ALTER TABLE votes ADD COLUMN IF NOT EXISTS frequency TEXT DEFAULT 'rare'")
                    cur.execute("ALTER TABLE votes ADD COLUMN IF NOT EXISTS comm_format TEXT DEFAULT 'reserved'")
                    cur.execute("ALTER TABLE votes ADD COLUMN IF NOT EXISTS emotion_tone TEXT DEFAULT 'neutral'")
                    cur.execute("ALTER TABLE votes ADD COLUMN IF NOT EXISTS feedback_style TEXT DEFAULT 'soft'")
                    cur.execute("ALTER TABLE votes ADD COLUMN IF NOT EXISTS uncertainty TEXT DEFAULT 'high'")
                    cur.execute("ALTER TABLE votes ADD COLUMN IF NOT EXISTS target_user_id BIGINT")
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS users (
                            user_id BIGINT PRIMARY KEY,
                            username TEXT NOT NULL UNIQUE,
                            first_name TEXT DEFAULT '',
                            last_name TEXT DEFAULT '',
                            photo_url TEXT DEFAULT '',
                            app_user BOOLEAN DEFAULT TRUE,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                        """
                    )
                    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT DEFAULT ''")
                    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_name TEXT DEFAULT ''")
                    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS photo_url TEXT DEFAULT ''")
                    cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS app_user BOOLEAN DEFAULT TRUE")
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
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_votes_unique_user
                        ON votes (target_user_id, voter_id)
                        WHERE target_user_id IS NOT NULL AND voter_id IS NOT NULL
                        """
                    )
                    cur.execute("ALTER TABLE ref_visits ADD COLUMN IF NOT EXISTS target_user_id BIGINT")
                    cur.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_ref_unique
                        ON ref_visits (target, visitor_id)
                        """
                    )
                    cur.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_ref_unique_user
                        ON ref_visits (target_user_id, visitor_id)
                        WHERE target_user_id IS NOT NULL
                        """
                    )
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS profile_prefs (
                            user_id BIGINT PRIMARY KEY,
                            note TEXT DEFAULT '',
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                        """
                    )
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS push_events (
                            id SERIAL PRIMARY KEY,
                            user_id BIGINT NOT NULL,
                            event_type TEXT NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                        """
                    )
                    cur.execute(
                        """
                        UPDATE votes v
                        SET target_user_id = u.user_id
                        FROM users u
                        WHERE v.target_user_id IS NULL
                          AND LOWER(v.target) = LOWER(u.username)
                        """
                    )
                    cur.execute(
                        """
                        UPDATE ref_visits r
                        SET target_user_id = u.user_id
                        FROM users u
                        WHERE r.target_user_id IS NULL
                          AND LOWER(r.target) = LOWER(u.username)
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
                        initiative TEXT DEFAULT 'wait',
                        start_context TEXT DEFAULT 'topic',
                        attention_reaction TEXT DEFAULT 'careful',
                        frequency TEXT DEFAULT 'rare',
                        comm_format TEXT DEFAULT 'reserved',
                        emotion_tone TEXT DEFAULT 'neutral',
                        feedback_style TEXT DEFAULT 'soft',
                        uncertainty TEXT DEFAULT 'high',
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
            try:
                conn.execute("ALTER TABLE votes ADD COLUMN initiative TEXT DEFAULT 'wait'")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE votes ADD COLUMN start_context TEXT DEFAULT 'topic'")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE votes ADD COLUMN attention_reaction TEXT DEFAULT 'careful'")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE votes ADD COLUMN frequency TEXT DEFAULT 'rare'")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE votes ADD COLUMN comm_format TEXT DEFAULT 'reserved'")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE votes ADD COLUMN emotion_tone TEXT DEFAULT 'neutral'")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE votes ADD COLUMN feedback_style TEXT DEFAULT 'soft'")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE votes ADD COLUMN uncertainty TEXT DEFAULT 'high'")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE votes ADD COLUMN target_user_id INTEGER")
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
                    app_user INTEGER DEFAULT 1,
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
            try:
                conn.execute("ALTER TABLE users ADD COLUMN app_user INTEGER DEFAULT 1")
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
                CREATE UNIQUE INDEX IF NOT EXISTS idx_votes_unique_user
                ON votes (target_user_id, voter_id)
                WHERE target_user_id IS NOT NULL AND voter_id IS NOT NULL
                """
            )
            try:
                conn.execute("ALTER TABLE ref_visits ADD COLUMN target_user_id INTEGER")
            except sqlite3.OperationalError:
                pass
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_ref_unique
                ON ref_visits (target, visitor_id)
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_ref_unique_user
                ON ref_visits (target_user_id, visitor_id)
                WHERE target_user_id IS NOT NULL
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS profile_prefs (
                    user_id INTEGER PRIMARY KEY,
                    note TEXT DEFAULT '',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS push_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                UPDATE votes
                SET target_user_id = (
                    SELECT u.user_id FROM users u
                    WHERE LOWER(u.username) = LOWER(votes.target)
                    LIMIT 1
                )
                WHERE target_user_id IS NULL
                """
            )
            conn.execute(
                """
                UPDATE ref_visits
                SET target_user_id = (
                    SELECT u.user_id FROM users u
                    WHERE LOWER(u.username) = LOWER(ref_visits.target)
                    LIMIT 1
                )
                WHERE target_user_id IS NULL
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
    target_user_id: Optional[int] = None,
    tone: str = "serious",
    speed: str = "slow",
    contact_format: str = "text",
    caution: str = "false",
    initiative: str = "wait",
    start_context: str = "topic",
    attention_reaction: str = "careful",
    frequency: str = "rare",
    comm_format: str = "reserved",
    emotion_tone: str = "neutral",
    feedback_style: str = "soft",
    uncertainty: str = "high",
) -> Optional[str]:
    cooldown = timedelta(hours=24)

    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    if voter_id is None:
                        cur.execute(
                            "INSERT INTO votes (target, target_user_id, label, tone, speed, contact_format, caution, initiative, start_context, attention_reaction, frequency, comm_format, emotion_tone, feedback_style, uncertainty, voter_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            (target, target_user_id, label, tone, speed, contact_format, caution, initiative, start_context, attention_reaction, frequency, comm_format, emotion_tone, feedback_style, uncertainty, voter_id),
                        )
                        conn.commit()
                        return "inserted"

                    if target_user_id is not None:
                        cur.execute(
                            """
                            SELECT id, created_at, label
                            FROM votes
                            WHERE target_user_id = %s AND voter_id = %s
                            ORDER BY id DESC
                            LIMIT 1
                            """,
                            (target_user_id, voter_id),
                        )
                    else:
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
                            "INSERT INTO votes (target, target_user_id, label, tone, speed, contact_format, caution, initiative, start_context, attention_reaction, frequency, comm_format, emotion_tone, feedback_style, uncertainty, voter_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            (target, target_user_id, label, tone, speed, contact_format, caution, initiative, start_context, attention_reaction, frequency, comm_format, emotion_tone, feedback_style, uncertainty, voter_id),
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
                                target = %s,
                                target_user_id = %s,
                                tone = %s,
                                speed = %s,
                                contact_format = %s,
                                caution = %s,
                                initiative = %s,
                                start_context = %s,
                                attention_reaction = %s,
                                frequency = %s,
                                comm_format = %s,
                                emotion_tone = %s,
                                feedback_style = %s,
                                uncertainty = %s,
                                created_at = CURRENT_TIMESTAMP
                            WHERE id = %s
                            """,
                            ("feedback", target, target_user_id, tone, speed, contact_format, caution, initiative, start_context, attention_reaction, frequency, comm_format, emotion_tone, feedback_style, uncertainty, vote_id),
                        )
                        conn.commit()
                        return "inserted"

                    if isinstance(last_ts, datetime) and datetime.utcnow() - last_ts.replace(tzinfo=None) >= cooldown:
                        cur.execute(
                            """
                            UPDATE votes
                            SET label = %s,
                                target = %s,
                                target_user_id = %s,
                                tone = %s,
                                speed = %s,
                                contact_format = %s,
                                caution = %s,
                                initiative = %s,
                                start_context = %s,
                                attention_reaction = %s,
                                frequency = %s,
                                comm_format = %s,
                                emotion_tone = %s,
                                feedback_style = %s,
                                uncertainty = %s,
                                created_at = CURRENT_TIMESTAMP
                            WHERE id = %s
                            """,
                            (label, target, target_user_id, tone, speed, contact_format, caution, initiative, start_context, attention_reaction, frequency, comm_format, emotion_tone, feedback_style, uncertainty, vote_id),
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
                        "INSERT INTO votes (target, target_user_id, label, tone, speed, contact_format, caution, initiative, start_context, attention_reaction, frequency, comm_format, emotion_tone, feedback_style, uncertainty, voter_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (target, target_user_id, label, tone, speed, contact_format, caution, initiative, start_context, attention_reaction, frequency, comm_format, emotion_tone, feedback_style, uncertainty, voter_id),
                    )
                    return "inserted"

                if target_user_id is not None:
                    cur = conn.execute(
                        """
                        SELECT id, created_at, label
                        FROM votes
                        WHERE target_user_id = ? AND voter_id = ?
                        ORDER BY id DESC
                        LIMIT 1
                        """,
                        (target_user_id, voter_id),
                    )
                else:
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
                        "INSERT INTO votes (target, target_user_id, label, tone, speed, contact_format, caution, initiative, start_context, attention_reaction, frequency, comm_format, emotion_tone, feedback_style, uncertainty, voter_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (target, target_user_id, label, tone, speed, contact_format, caution, initiative, start_context, attention_reaction, frequency, comm_format, emotion_tone, feedback_style, uncertainty, voter_id),
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
                            target = ?,
                            target_user_id = ?,
                            tone = ?,
                            speed = ?,
                            contact_format = ?,
                            caution = ?,
                            initiative = ?,
                            start_context = ?,
                            attention_reaction = ?,
                            frequency = ?,
                            comm_format = ?,
                            emotion_tone = ?,
                            feedback_style = ?,
                            uncertainty = ?,
                            created_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        ("feedback", target, target_user_id, tone, speed, contact_format, caution, initiative, start_context, attention_reaction, frequency, comm_format, emotion_tone, feedback_style, uncertainty, vote_id),
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
                            target = ?,
                            target_user_id = ?,
                            tone = ?,
                            speed = ?,
                            contact_format = ?,
                            caution = ?,
                            initiative = ?,
                            start_context = ?,
                            attention_reaction = ?,
                            frequency = ?,
                            comm_format = ?,
                            emotion_tone = ?,
                            feedback_style = ?,
                            uncertainty = ?,
                            created_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (label, target, target_user_id, tone, speed, contact_format, caution, initiative, start_context, attention_reaction, frequency, comm_format, emotion_tone, feedback_style, uncertainty, vote_id),
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
    app_user: bool = True,
) -> bool:
    username = username.lower()
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT username FROM users WHERE user_id = %s LIMIT 1", (user_id,))
                    prev_row = cur.fetchone()
                    existed = prev_row is not None
                    prev_username = str(prev_row[0]).lower() if prev_row and prev_row[0] else ""
                    cur.execute(
                        "DELETE FROM users WHERE LOWER(username) = LOWER(%s) AND user_id <> %s",
                        (username, user_id),
                    )
                    cur.execute(
                        """
                        INSERT INTO users (user_id, username, first_name, last_name, photo_url, app_user, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT(user_id) DO UPDATE SET
                            username = EXCLUDED.username,
                            first_name = EXCLUDED.first_name,
                            last_name = EXCLUDED.last_name,
                            photo_url = EXCLUDED.photo_url,
                            app_user = users.app_user OR EXCLUDED.app_user,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        (user_id, username, first_name, last_name, photo_url, app_user),
                    )
                    aliases = [username]
                    if prev_username and prev_username not in aliases:
                        aliases.append(prev_username)
                    cur.execute(
                        """
                        UPDATE votes
                        SET target_user_id = %s
                        WHERE LOWER(target) = ANY(%s)
                          AND (target_user_id IS NULL OR target_user_id = %s)
                        """,
                        (user_id, aliases, user_id),
                    )
                    cur.execute(
                        """
                        UPDATE ref_visits
                        SET target_user_id = %s
                        WHERE LOWER(target) = ANY(%s)
                          AND (target_user_id IS NULL OR target_user_id = %s)
                        """,
                        (user_id, aliases, user_id),
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
                cur = conn.execute("SELECT username FROM users WHERE user_id = ? LIMIT 1", (user_id,))
                prev_row = cur.fetchone()
                existed = prev_row is not None
                prev_username = str(prev_row[0]).lower() if prev_row and prev_row[0] else ""
                conn.execute(
                    "DELETE FROM users WHERE LOWER(username) = LOWER(?) AND user_id <> ?",
                    (username, user_id),
                )
                conn.execute(
                    """
                    INSERT INTO users (user_id, username, first_name, last_name, photo_url, app_user, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(user_id) DO UPDATE SET
                        username = excluded.username,
                        first_name = excluded.first_name,
                        last_name = excluded.last_name,
                        photo_url = excluded.photo_url,
                        app_user = CASE
                            WHEN users.app_user = 1 OR excluded.app_user = 1 THEN 1
                            ELSE 0
                        END,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (user_id, username, first_name, last_name, photo_url, 1 if app_user else 0),
                )
                aliases = [username]
                if prev_username and prev_username not in aliases:
                    aliases.append(prev_username)
                alias_marks = ",".join("?" for _ in aliases)
                conn.execute(
                    f"""
                    UPDATE votes
                    SET target_user_id = ?
                    WHERE LOWER(target) IN ({alias_marks})
                      AND (target_user_id IS NULL OR target_user_id = ?)
                    """,
                    (user_id, *aliases, user_id),
                )
                conn.execute(
                    f"""
                    UPDATE ref_visits
                    SET target_user_id = ?
                    WHERE LOWER(target) IN ({alias_marks})
                      AND (target_user_id IS NULL OR target_user_id = ?)
                    """,
                    (user_id, *aliases, user_id),
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
    app_user: bool = True,
) -> None:
    upsert_user_with_flag(user_id, username, first_name, last_name, photo_url, app_user)


def get_user_public_by_username(username: str) -> Optional[dict]:
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT user_id, username, first_name, last_name, photo_url, app_user
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
                SELECT user_id, username, first_name, last_name, photo_url, app_user
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
        "app_user": bool(row[5]),
    }


def get_profile_note(user_id: int) -> str:
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT note FROM profile_prefs WHERE user_id = %s LIMIT 1", (user_id,))
                    row = cur.fetchone()
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB get_profile_note failed: %s", exc)
            return ""
    else:
        conn = _get_sqlite_conn()
        try:
            cur = conn.execute("SELECT note FROM profile_prefs WHERE user_id = ? LIMIT 1", (user_id,))
            row = cur.fetchone()
        finally:
            conn.close()
    return str(row[0] or "") if row else ""


def set_profile_note(user_id: int, note: str) -> None:
    note = str(note or "")
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO profile_prefs (user_id, note, updated_at)
                        VALUES (%s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT(user_id) DO UPDATE SET
                            note = EXCLUDED.note,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        (user_id, note),
                    )
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB set_profile_note failed: %s", exc)
    else:
        conn = _get_sqlite_conn()
        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO profile_prefs (user_id, note, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(user_id) DO UPDATE SET
                        note = excluded.note,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (user_id, note),
                )
        finally:
            conn.close()


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


def add_ref_visit(target: str, visitor_id: int, target_user_id: Optional[int] = None) -> bool:
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO ref_visits (target, target_user_id, visitor_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                        (target, target_user_id, visitor_id),
                    )
                    inserted = cur.rowcount > 0
                    conn.commit()
                    return inserted
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB add_ref_visit failed: %s", exc)
            return False
    else:
        conn = _get_sqlite_conn()
        try:
            with conn:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO ref_visits (target, target_user_id, visitor_id) VALUES (?, ?, ?)",
                    (target, target_user_id, visitor_id),
                )
                return (cur.rowcount or 0) > 0
        finally:
            conn.close()


def count_ref_visitors(target: str, target_user_id: Optional[int] = None) -> int:
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    if target_user_id is not None:
                        cur.execute("SELECT COUNT(*) FROM ref_visits WHERE target_user_id = %s", (target_user_id,))
                    else:
                        cur.execute("SELECT COUNT(*) FROM ref_visits WHERE target = %s", (target,))
                    total = cur.fetchone()[0]
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB count_ref_visitors failed: %s", exc)
            return 0
    else:
        conn = _get_sqlite_conn()
        try:
            if target_user_id is not None:
                cur = conn.execute("SELECT COUNT(*) FROM ref_visits WHERE target_user_id = ?", (target_user_id,))
            else:
                cur = conn.execute("SELECT COUNT(*) FROM ref_visits WHERE target = ?", (target,))
            total = cur.fetchone()[0]
        finally:
            conn.close()
    return int(total)


def count_ref_answerers(target: str, target_user_id: Optional[int] = None) -> int:
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    if target_user_id is not None:
                        cur.execute(
                            """
                            SELECT COUNT(DISTINCT v.voter_id)
                            FROM votes v
                            JOIN ref_visits r
                              ON r.target_user_id = v.target_user_id
                             AND r.visitor_id = v.voter_id
                            WHERE v.target_user_id = %s
                              AND v.label = 'feedback'
                              AND v.voter_id IS NOT NULL
                            """,
                            (target_user_id,),
                        )
                    else:
                        cur.execute(
                            """
                            SELECT COUNT(DISTINCT v.voter_id)
                            FROM votes v
                            JOIN ref_visits r
                              ON r.target = v.target
                             AND r.visitor_id = v.voter_id
                            WHERE v.target = %s
                              AND v.label = 'feedback'
                              AND v.voter_id IS NOT NULL
                            """,
                            (target,),
                        )
                    total = cur.fetchone()[0]
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB count_ref_answerers failed: %s", exc)
            return 0
    else:
        conn = _get_sqlite_conn()
        try:
            if target_user_id is not None:
                cur = conn.execute(
                    """
                    SELECT COUNT(DISTINCT v.voter_id)
                    FROM votes v
                    JOIN ref_visits r
                      ON r.target_user_id = v.target_user_id
                     AND r.visitor_id = v.voter_id
                    WHERE v.target_user_id = ?
                      AND v.label = 'feedback'
                      AND v.voter_id IS NOT NULL
                    """,
                    (target_user_id,),
                )
            else:
                cur = conn.execute(
                    """
                    SELECT COUNT(DISTINCT v.voter_id)
                    FROM votes v
                    JOIN ref_visits r
                      ON r.target = v.target
                     AND r.visitor_id = v.voter_id
                    WHERE v.target = ?
                      AND v.label = 'feedback'
                      AND v.voter_id IS NOT NULL
                    """,
                    (target,),
                )
            total = cur.fetchone()[0]
        finally:
            conn.close()
    return int(total or 0)


def count_pushes_today(user_id: int) -> int:
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM push_events
                        WHERE user_id = %s
                          AND created_at::date = CURRENT_DATE
                        """,
                        (user_id,),
                    )
                    total = cur.fetchone()[0]
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB count_pushes_today failed: %s", exc)
            return 0
    else:
        conn = _get_sqlite_conn()
        try:
            cur = conn.execute(
                """
                SELECT COUNT(*)
                FROM push_events
                WHERE user_id = ?
                  AND date(created_at) = date('now', 'localtime')
                """,
                (user_id,),
            )
            total = cur.fetchone()[0]
        finally:
            conn.close()
    return int(total or 0)


def add_push_event(user_id: int, event_type: str) -> None:
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO push_events (user_id, event_type) VALUES (%s, %s)",
                        (user_id, event_type),
                    )
                    conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB add_push_event failed: %s", exc)
    else:
        conn = _get_sqlite_conn()
        try:
            with conn:
                conn.execute(
                    "INSERT INTO push_events (user_id, event_type) VALUES (?, ?)",
                    (user_id, event_type),
                )
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


def get_total(target: str, target_user_id: Optional[int] = None) -> int:
    if USE_POSTGRES:
        try:
            conn = _get_pg_conn()
            try:
                with conn.cursor() as cur:
                    if target_user_id is not None:
                        cur.execute("SELECT COUNT(*) FROM votes WHERE target_user_id = %s AND label = 'feedback'", (target_user_id,))
                    else:
                        cur.execute("SELECT COUNT(*) FROM votes WHERE target = %s AND label = 'feedback'", (target,))
                    total = cur.fetchone()[0]
            finally:
                conn.close()
        except Exception as exc:
            logging.warning("DB get_total failed: %s", exc)
            return 0
    else:
        conn = _get_sqlite_conn()
        try:
            if target_user_id is not None:
                cur = conn.execute("SELECT COUNT(*) FROM votes WHERE target_user_id = ? AND label = 'feedback'", (target_user_id,))
            else:
                cur = conn.execute("SELECT COUNT(*) FROM votes WHERE target = ? AND label = 'feedback'", (target,))
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


def get_contact_dimensions(target: str, target_user_id: Optional[int] = None) -> dict[str, dict[str, int]]:
    fields = {
        "tone": ("easy", "serious"),
        "speed": ("fast", "slow"),
        "contact_format": ("text", "live"),
        "initiative": ("self", "wait"),
        "start_context": ("topic", "direct"),
        "attention_reaction": ("likes", "careful"),
        "caution": ("true", "false"),
        "frequency": ("often", "rare"),
        "comm_format": ("informal", "reserved"),
        "emotion_tone": ("warm", "neutral"),
        "feedback_style": ("direct", "soft"),
        "uncertainty": ("low", "high"),
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
                        if target_user_id is not None:
                            cur.execute(
                                f"SELECT {field}, COUNT(*) FROM votes WHERE target_user_id = %s AND label = 'feedback' GROUP BY {field}",
                                (target_user_id,),
                            )
                        else:
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
                if target_user_id is not None:
                    cur = conn.execute(
                        f"SELECT {field}, COUNT(*) FROM votes WHERE target_user_id = ? AND label = 'feedback' GROUP BY {field}",
                        (target_user_id,),
                    )
                else:
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
