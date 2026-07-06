"""
╔══════════════════════════════════════════════════════════════════════╗
║          TG BULK TRANSFER BOT  —  main.py                           ║
║  Phases : Scrape → Store → Sort → Copy (no "Forwarded from" tag)    ║
║  DB     : Neon DB / PostgreSQL  via asyncpg                         ║
║  Server : aiohttp health-check server on dynamic PORT               ║
╚══════════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import sys
from datetime import datetime
from typing import Optional

import asyncpg
from aiohttp import web
from dotenv import load_dotenv
from pyrogram import Client, errors
from pyrogram.types import Message

# ──────────────────────────────────────────────────────────────────────────────
# 0.  Bootstrap
# ──────────────────────────────────────────────────────────────────────────────

load_dotenv()  # no-op in production; loads .env for local dev

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("bulk_transfer")


# ──────────────────────────────────────────────────────────────────────────────
# 1.  Configuration — all values come from environment variables
# ──────────────────────────────────────────────────────────────────────────────

class Config:
    """Central config object.  Raises ValueError early if a required var is absent."""

    def __init__(self) -> None:
        # --- Web server ---
        self.PORT: int = int(os.environ.get("PORT", 8080))

        # --- Telegram API ---
        self.API_ID: int = self._require_int("API_ID")
        self.API_HASH: str = self._require("API_HASH")

        # Session string takes priority; fall back to bot token
        self.SESSION_STRING: Optional[str] = os.environ.get("SESSION_STRING")
        self.BOT_TOKEN: Optional[str] = os.environ.get("BOT_TOKEN")
        if not self.SESSION_STRING and not self.BOT_TOKEN:
            raise ValueError("Set either SESSION_STRING or BOT_TOKEN in env vars.")

        # --- Channels ---
        self.SOURCE_CHAT_ID: int = self._require_int("SOURCE_CHAT_ID")
        self.DEST_CHAT_ID: int = self._require_int("DEST_CHAT_ID")

        # --- Database ---
        self.DATABASE_URL: str = self._require("DATABASE_URL")

        # --- Tuning ---
        self.COPY_DELAY: float = float(os.environ.get("COPY_DELAY", 2))
        self.SCRAPE_BATCH_SIZE: int = int(os.environ.get("SCRAPE_BATCH_SIZE", 200))

    @staticmethod
    def _require(key: str) -> str:
        val = os.environ.get(key)
        if not val:
            raise ValueError(f"Required environment variable '{key}' is not set.")
        return val

    @staticmethod
    def _require_int(key: str) -> int:
        return int(Config._require(key))


config = Config()


# ──────────────────────────────────────────────────────────────────────────────
# 2.  Database Layer  (asyncpg + Neon DB / PostgreSQL)
# ──────────────────────────────────────────────────────────────────────────────

RESOLUTION_ORDER = {"480p": 1, "576p": 2, "720p": 3, "1080p": 4, "2160p": 5, "4k": 5}


class Database:
    """Async wrapper around asyncpg connection pool."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        """Open the connection pool. Works with Neon DB serverless driver."""
        self.pool = await asyncpg.create_pool(
            dsn=self._dsn,
            min_size=1,
            max_size=5,          # keep low for free-tier Neon connections
            command_timeout=60,
            server_settings={"application_name": "tg_bulk_transfer"},
        )
        await self._ensure_schema()
        log.info("Database pool ready.")

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()

    async def _ensure_schema(self) -> None:
        """Idempotent table creation."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS tg_files (
                    id              SERIAL PRIMARY KEY,
                    message_id      BIGINT      NOT NULL UNIQUE,
                    file_name       TEXT        NOT NULL,
                    file_unique_id  TEXT,
                    year            SMALLINT,
                    resolution      TEXT,
                    resolution_rank SMALLINT    DEFAULT 99,
                    status          TEXT        NOT NULL DEFAULT 'pending',
                    scraped_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    sent_at         TIMESTAMPTZ
                );
                CREATE INDEX IF NOT EXISTS idx_status
                    ON tg_files (status);
                CREATE INDEX IF NOT EXISTS idx_sort
                    ON tg_files (file_name, year, resolution_rank);
            """)
            log.info("Schema verified / created.")

    # ── Write helpers ────────────────────────────────────────────────────────

    async def upsert_file(
        self,
        message_id: int,
        file_name: str,
        file_unique_id: Optional[str],
        year: Optional[int],
        resolution: Optional[str],
    ) -> None:
        rank = RESOLUTION_ORDER.get((resolution or "").lower(), 99)
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO tg_files
                    (message_id, file_name, file_unique_id, year, resolution, resolution_rank)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (message_id) DO UPDATE
                    SET file_name       = EXCLUDED.file_name,
                        file_unique_id  = EXCLUDED.file_unique_id,
                        year            = EXCLUDED.year,
                        resolution      = EXCLUDED.resolution,
                        resolution_rank = EXCLUDED.resolution_rank;
            """, message_id, file_name, file_unique_id, year, rank)

    async def mark_sent(self, message_id: int) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute("""
                UPDATE tg_files
                   SET status  = 'sent',
                       sent_at = NOW()
                 WHERE message_id = $1;
            """, message_id)

    # ── Read helpers ─────────────────────────────────────────────────────────

    async def get_pending(self) -> list[asyncpg.Record]:
        """
        Return all pending rows sorted by:
          1. file_name  (group movies together)
          2. year       (oldest first)
          3. resolution_rank (lowest quality first → 480p, 720p, 1080p …)
        """
        async with self.pool.acquire() as conn:
            return await conn.fetch("""
                SELECT message_id, file_name, year, resolution, resolution_rank
                  FROM tg_files
                 WHERE status = 'pending'
              ORDER BY file_name      ASC NULLS LAST,
                       year           ASC NULLS LAST,
                       resolution_rank ASC NULLS LAST,
                       message_id     ASC;
            """)

    async def stats(self) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT
                    COUNT(*)                                    AS total,
                    COUNT(*) FILTER (WHERE status = 'pending') AS pending,
                    COUNT(*) FILTER (WHERE status = 'sent')    AS sent
                FROM tg_files;
            """)
            return dict(row)


# ──────────────────────────────────────────────────────────────────────────────
# 3.  Regex helpers — extract file_name / year / resolution from Telegram text
# ──────────────────────────────────────────────────────────────────────────────

# Year: 4-digit number in range 1900-2099
_RE_YEAR = re.compile(r"\b(19\d{2}|20\d{2})\b")

# Resolution: common variants
_RE_RESOLUTION = re.compile(
    r"\b(480p|576p|720p|1080p|1440p|2160p|4k|uhd)\b",
    re.IGNORECASE,
)

# Noise tokens to strip when deriving a clean movie title
_RE_NOISE = re.compile(
    r"""
    \b(
        19\d{2} | 20\d{2}          |   # years
        480p | 576p | 720p | 1080p |   # resolutions
        1440p | 2160p | 4k | uhd   |
        bluray | blu-ray | bdrip    |
        webrip | web-dl | webdl     |
        hdrip | dvdrip | dvdscr     |
        hdcam | cam | ts            |
        x264 | x265 | hevc | avc    |
        aac | dd5\.1 | dts | ac3    |
        10bit | hdr | sdr | atmos   |
        multi | dual | hindi | eng  |
        esub | sub | dubbed
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)
_RE_MULTI_SPACE = re.compile(r"\s{2,}")
_RE_BRACKETS = re.compile(r"[\[\](){}]+")
_RE_EXTENSION = re.compile(r"\.[a-zA-Z0-9]{2,4}$")


def parse_metadata(raw: str) -> tuple[str, Optional[int], Optional[str]]:
    """
    Given a raw file name or caption string, return:
        (clean_title, year_or_None, resolution_or_None)
    """
    # Strip file extension
    name = _RE_EXTENSION.sub("", raw)
    # Normalise separators
    name = name.replace(".", " ").replace("_", " ").replace("-", " ")

    year_match = _RE_YEAR.search(name)
    year: Optional[int] = int(year_match.group()) if year_match else None

    res_match = _RE_RESOLUTION.search(name)
    resolution: Optional[str] = res_match.group().lower() if res_match else None

    # Derive a clean movie title (everything before the first noise token)
    clean = _RE_NOISE.split(name)[0]
    clean = _RE_BRACKETS.sub(" ", clean)
    clean = _RE_MULTI_SPACE.sub(" ", clean).strip().title()

    return clean or raw, year, resolution


# ──────────────────────────────────────────────────────────────────────────────
# 4.  Pyrogram client factory
# ──────────────────────────────────────────────────────────────────────────────

def build_client() -> Client:
    if config.SESSION_STRING:
        log.info("Authenticating via SESSION_STRING (user account).")
        return Client(
            name="bulk_transfer_session",
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            session_string=config.SESSION_STRING,
            in_memory=True,
        )
    else:
        log.info("Authenticating via BOT_TOKEN.")
        return Client(
            name="bulk_transfer_bot",
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            bot_token=config.BOT_TOKEN,
            in_memory=True,
        )


# ──────────────────────────────────────────────────────────────────────────────
# 5.  Phase 1 — Scrape & store metadata
# ──────────────────────────────────────────────────────────────────────────────

async def phase_scrape(client: Client, db: Database) -> None:
    """
    Iterate every message in SOURCE_CHAT_ID.
    For each document / video / audio, extract metadata and upsert to DB.
    """
    log.info("═" * 60)
    log.info("PHASE 1 — Scraping source channel …")
    log.info("═" * 60)

    total = 0
    skipped = 0

    async for msg in client.get_chat_history(config.SOURCE_CHAT_ID):
        media = _extract_media(msg)
        if not media:
            skipped += 1
            continue

        raw_name = media.get("file_name") or media.get("file_unique_id", f"file_{msg.id}")
        title, year, resolution = parse_metadata(raw_name)

        await db.upsert_file(
            message_id=msg.id,
            file_name=title,
            file_unique_id=media.get("file_unique_id"),
            year=year,
            resolution=resolution,
        )
        total += 1
        if total % 500 == 0:
            log.info(f"  Scraped {total:,} files so far …")

    log.info(f"PHASE 1 COMPLETE — {total:,} media files saved, {skipped:,} non-media skipped.")


def _extract_media(msg: Message) -> Optional[dict]:
    """Return a dict with file_name & file_unique_id for any media type, or None."""
    for attr in ("document", "video", "audio", "voice", "video_note", "animation"):
        media = getattr(msg, attr, None)
        if media:
            return {
                "file_name": getattr(media, "file_name", None),
                "file_unique_id": getattr(media, "file_unique_id", None),
            }
    return None


# ──────────────────────────────────────────────────────────────────────────────
# 6.  Phase 2 — Sorted send
# ──────────────────────────────────────────────────────────────────────────────

async def phase_send(client: Client, db: Database) -> None:
    """
    Fetch pending rows (sorted by title → year → resolution),
    copy each message to DEST_CHAT_ID, then mark as 'sent'.
    Handles FloodWait and network errors gracefully.
    """
    log.info("═" * 60)
    log.info("PHASE 2 — Sending files (sorted) …")
    log.info("═" * 60)

    rows = await db.get_pending()
    total = len(rows)
    log.info(f"  {total:,} files queued for transfer.")

    sent_count = 0
    fail_count = 0

    for idx, row in enumerate(rows, start=1):
        msg_id = row["message_id"]
        label = f"[{idx}/{total}] MsgID={msg_id} | {row['file_name']} | {row['year']} | {row['resolution']}"

        success = await _copy_with_retry(client, msg_id, label)

        if success:
            await db.mark_sent(msg_id)
            sent_count += 1
        else:
            fail_count += 1

        # Polite delay between each send to stay within Telegram rate limits
        await asyncio.sleep(config.COPY_DELAY)

    log.info(f"PHASE 2 COMPLETE — Sent: {sent_count:,} | Failed: {fail_count:,}")


async def _copy_with_retry(client: Client, message_id: int, label: str) -> bool:
    """
    Attempt to copy a message. Retries on FloodWait up to 3 times.
    Returns True on success, False on permanent failure.
    """
    max_retries = 3

    for attempt in range(1, max_retries + 1):
        try:
            await client.copy_message(
                chat_id=config.DEST_CHAT_ID,
                from_chat_id=config.SOURCE_CHAT_ID,
                message_id=message_id,
            )
            log.info(f"  ✓ SENT   {label}")
            return True

        except errors.FloodWait as e:
            wait = e.value + 5  # add 5 s buffer
            log.warning(f"  ⏳ FloodWait {wait}s — sleeping … ({label})")
            await asyncio.sleep(wait)

        except errors.MessageIdInvalid:
            log.error(f"  ✗ MessageIdInvalid — skipping. {label}")
            return False

        except errors.ChatAdminRequired:
            log.critical("  ✗ Bot/account lacks admin rights in destination channel. Aborting.")
            raise SystemExit(1)

        except errors.RPCError as e:
            log.warning(f"  ✗ RPC error (attempt {attempt}/{max_retries}): {e} | {label}")
            await asyncio.sleep(5 * attempt)

        except Exception as e:
            log.error(f"  ✗ Unexpected error (attempt {attempt}/{max_retries}): {e} | {label}")
            await asyncio.sleep(5 * attempt)

    log.error(f"  ✗ FAILED after {max_retries} attempts — {label}")
    return False


# ──────────────────────────────────────────────────────────────────────────────
# 7.  aiohttp Health-check web server
# ──────────────────────────────────────────────────────────────────────────────

async def handle_health(request: web.Request) -> web.Response:
    """GET / → 200 OK with JSON status (keeps Render/Koyeb/HF alive)."""
    db: Database = request.app["db"]
    try:
        stats = await db.stats()
    except Exception:
        stats = {}
    payload = {
        "status": "running",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "db_stats": stats,
    }
    return web.json_response(payload)


async def handle_trigger(request: web.Request) -> web.Response:
    """
    POST /run  →  manually re-trigger Phase 2 (e.g. after a restart).
    Returns immediately; transfer runs in background.
    """
    app = request.app
    client: Client = app["client"]
    db: Database = app["db"]

    if app.get("transfer_running"):
        return web.json_response({"message": "Transfer already in progress."}, status=409)

    async def _run():
        app["transfer_running"] = True
        try:
            await phase_send(client, db)
        finally:
            app["transfer_running"] = False

    asyncio.create_task(_run())
    return web.json_response({"message": "Phase 2 started in background."})


def build_web_app(client: Client, db: Database) -> web.Application:
    app = web.Application()
    app["client"] = client
    app["db"] = db
    app["transfer_running"] = False
    app.router.add_get("/", handle_health)
    app.router.add_get("/health", handle_health)
    app.router.add_post("/run", handle_trigger)
    return app


# ──────────────────────────────────────────────────────────────────────────────
# 8.  Entry point — orchestrates everything
# ──────────────────────────────────────────────────────────────────────────────

async def main() -> None:
    log.info("┌─────────────────────────────────────────────────┐")
    log.info("│      TG BULK TRANSFER BOT  —  Starting up       │")
    log.info("└─────────────────────────────────────────────────┘")
    log.info(f"  Web server port : {config.PORT}")
    log.info(f"  Source chat     : {config.SOURCE_CHAT_ID}")
    log.info(f"  Destination     : {config.DEST_CHAT_ID}")
    log.info(f"  Copy delay      : {config.COPY_DELAY}s")

    # ── Connect DB ───────────────────────────────────────────────────────────
    db = Database(config.DATABASE_URL)
    await db.connect()

    # ── Connect Telegram ─────────────────────────────────────────────────────
    client = build_client()
    await client.start()
    me = await client.get_me()
    log.info(f"  Logged in as    : {me.first_name} (@{me.username})")

    # ── Build & start web server ──────────────────────────────────────────────
    web_app = build_web_app(client, db)
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=config.PORT)
    await site.start()
    log.info(f"  Health server   : http://0.0.0.0:{config.PORT}/health")

    # ── Run transfer pipeline ─────────────────────────────────────────────────
    try:
        # Phase 1: scrape & store metadata (skip if all already scraped)
        stats_before = await db.stats()
        if stats_before["total"] == 0:
            log.info("No records in DB — starting fresh scrape.")
            await phase_scrape(client, db)
        else:
            log.info(
                f"DB already has {stats_before['total']:,} records "
                f"({stats_before['pending']:,} pending). Skipping Phase 1."
            )
            log.info("  → To force a re-scrape, truncate the tg_files table manually.")

        # Phase 2: sort & send
        await phase_send(client, db)

    except (KeyboardInterrupt, SystemExit):
        log.info("Shutdown signal received.")
    except Exception as exc:
        log.exception(f"Fatal error in pipeline: {exc}")
    finally:
        # Keep web server alive even after transfer finishes
        # (cloud platforms need an always-running process)
        log.info("Pipeline finished. Web server stays up for health checks.")
        await asyncio.Event().wait()  # blocks forever until process is killed

    # ── Cleanup (reached only on forced exit) ────────────────────────────────
    await runner.cleanup()
    await client.stop()
    await db.close()
    log.info("Shutdown complete.")


if __name__ == "__main__":
    try:
        import uvloop  # noqa: F401 — faster event loop if available
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        log.info("Using uvloop event loop.")
    except ImportError:
        log.info("uvloop not available — using default asyncio event loop.")

    asyncio.run(main())
