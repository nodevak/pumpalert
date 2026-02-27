"""
Database layer using NeonDB (PostgreSQL).
Replaces seen_tokens.json and known_tokens.json with persistent DB tables.

Tables:
  seen_tokens  (bot_id TEXT, address TEXT)        — already alerted
  known_tokens (bot_id TEXT, address TEXT, created_at_ms BIGINT)  — all discovered
"""

import os
import logging
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")  # from NeonDB


def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    """Create tables if they don't exist. Call once on startup."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS seen_tokens (
                    bot_id  TEXT NOT NULL,
                    address TEXT NOT NULL,
                    PRIMARY KEY (bot_id, address)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS known_tokens (
                    bot_id        TEXT   NOT NULL,
                    address       TEXT   NOT NULL,
                    created_at_ms BIGINT NOT NULL DEFAULT 0,
                    PRIMARY KEY (bot_id, address)
                )
            """)
        conn.commit()
    log.info("[DB] Tables ready.")


# ── Seen tokens ───────────────────────────────────────────────────────────────
def load_seen_tokens(bot_id: str) -> set:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT address FROM seen_tokens WHERE bot_id = %s", (bot_id,))
            return {row[0] for row in cur.fetchall()}


def save_seen_tokens(bot_id: str, addresses: set):
    if not addresses:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO seen_tokens (bot_id, address) VALUES %s ON CONFLICT DO NOTHING",
                [(bot_id, addr) for addr in addresses],
            )
        conn.commit()


# ── Known tokens ──────────────────────────────────────────────────────────────
def load_known_tokens(bot_id: str) -> dict:
    """Returns {address: created_at_ms}"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT address, created_at_ms FROM known_tokens WHERE bot_id = %s",
                (bot_id,)
            )
            return {row[0]: row[1] for row in cur.fetchall()}


def save_known_tokens(bot_id: str, known: dict):
    """Upsert all known tokens."""
    if not known:
        return
    with get_conn() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO known_tokens (bot_id, address, created_at_ms)
                VALUES %s
                ON CONFLICT (bot_id, address)
                DO UPDATE SET created_at_ms = EXCLUDED.created_at_ms
                WHERE known_tokens.created_at_ms = 0
                """,
                [(bot_id, addr, ts) for addr, ts in known.items()],
            )
        conn.commit()


def delete_expired_known_tokens(bot_id: str, cutoff_ms: int):
    """Delete tokens older than cutoff_ms (but keep unknowns with created_at_ms=0)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM known_tokens
                WHERE bot_id = %s
                  AND created_at_ms > 0
                  AND created_at_ms < %s
                """,
                (bot_id, cutoff_ms),
            )
            removed = cur.rowcount
        conn.commit()
    if removed:
        log.info(f"[DB] Removed {removed} expired known tokens for {bot_id}")
    return removed
