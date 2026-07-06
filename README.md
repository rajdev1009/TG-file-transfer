# TG Bulk Transfer Bot

Transfers 50 000+ files from a source Telegram channel to a destination channel with strict sorting (by movie name → year → resolution). Uses `copy_message` so there is **no "Forwarded from" tag**. State is persisted in Neon DB / PostgreSQL, so the bot safely resumes after container restarts.

---

## Architecture Overview

```
Cloud Container (Render / Koyeb / HF Spaces)
│
├── aiohttp web server  →  GET /health  (keeps container alive)
│                       →  POST /run    (re-trigger Phase 2 manually)
│
├── Phase 1 — Scraper
│   get_chat_history() → Regex → asyncpg → Neon DB
│
└── Phase 2 — Sender (sorted)
    Neon DB (ORDER BY name, year, resolution_rank)
    → copy_message() → FloodWait handler → mark 'sent'
```

---

## Quick Start

### 1. Clone & install

```bash
git clone <your-repo>
cd tg_bulk_transfer
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Fill in .env with your real values
```

### 3. Generate a SESSION_STRING (user account — recommended)

```bash
python - <<'EOF'
from pyrogram import Client
import asyncio, os
from dotenv import load_dotenv
load_dotenv()

async def gen():
    async with Client(
        "gen_session",
        api_id=int(os.environ["API_ID"]),
        api_hash=os.environ["API_HASH"],
    ) as app:
        print(await app.export_session_string())

asyncio.run(gen())
EOF
```

Copy the printed string into `SESSION_STRING` in your `.env`.

### 4. Run locally

```bash
python main.py
```

---

## Deployment

### Render

1. New → **Web Service** → connect repo.
2. Build command: `pip install -r requirements.txt`
3. Start command: `python main.py`
4. Add all env vars from `.env.example` in the **Environment** tab.
5. Render injects `PORT` automatically.

### Koyeb

1. New App → **GitHub** source.
2. Set run command: `python main.py`
3. Add env vars in the Koyeb dashboard.
4. Koyeb injects `PORT` automatically.

### Hugging Face Spaces

1. Create a new **Docker** Space.
2. Add a `Dockerfile`:
   ```dockerfile
   FROM python:3.11-slim
   WORKDIR /app
   COPY . .
   RUN pip install --no-cache-dir -r requirements.txt
   EXPOSE 7860
   CMD ["python", "main.py"]
   ```
3. Set `PORT=7860` in Space Secrets (HF always uses 7860).
4. Add all other secrets in the Space settings.

---

## Database schema

```sql
CREATE TABLE tg_files (
    id              SERIAL PRIMARY KEY,
    message_id      BIGINT      NOT NULL UNIQUE,
    file_name       TEXT        NOT NULL,   -- cleaned movie title
    file_unique_id  TEXT,
    year            SMALLINT,               -- extracted by regex
    resolution      TEXT,                   -- e.g. "1080p"
    resolution_rank SMALLINT DEFAULT 99,    -- 480p=1, 720p=3, 1080p=4 …
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending | sent
    scraped_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at         TIMESTAMPTZ
);
```

---

## Sorting Logic

SQL `ORDER BY` used in Phase 2:

```sql
ORDER BY
    file_name       ASC NULLS LAST,   -- group all copies of a movie together
    year            ASC NULLS LAST,   -- oldest release first
    resolution_rank ASC NULLS LAST    -- 480p → 720p → 1080p → 4K
```

---

## Safe Resumption

Every file is marked `status = 'sent'` **immediately** after a successful `copy_message()`. If the container restarts, Phase 1 is skipped (records already exist) and Phase 2 picks up exactly where it left off — only `status = 'pending'` rows are fetched.

To force a complete re-scrape:
```sql
TRUNCATE TABLE tg_files;
```

To manually retrigger Phase 2 while the server is running:
```bash
curl -X POST https://your-app-url/run
```

---

## FloodWait Handling

```
errors.FloodWait  →  sleep(flood_wait_seconds + 5)  →  retry
```

Up to 3 retries per message. After 3 failures the message is skipped and the bot continues with the next file (it stays `pending` in the DB and can be retried later).
