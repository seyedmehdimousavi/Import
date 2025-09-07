#!/usr/bin/env python3
"""
Ingest Telegram channel posts into Supabase 'movies' table.

Requirements (install once):
  pip install telethon supabase==2.10.0 python-dotenv

Env (.env) – create alongside this script:
  TELEGRAM_API_ID=123456
  TELEGRAM_API_HASH=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
  TELEGRAM_SESSION=movie_ingest.session   # a local filename to store your session
  CHANNEL_USERNAME=@your_channel_handle   # e.g. @my_movies_channel
  SUPABASE_URL=https://YOUR-PROJECT.supabase.co
  SUPABASE_SERVICE_KEY=eyJhbGciOi...      # service_role key (server-side only)
  SUPABASE_STORAGE_BUCKET=covers          # optional; default: covers
  RLS_USER_UID=7314d471-8343-44b3-9fcc-a9ae01d99725  # maps to ADMIN_UID if you use RLS

Usage:
  1) Fill .env, run this once to log in: python ingest_telegram_to_supabase.py --login
  2) Run sync: python ingest_telegram_to_supabase.py --since 90d
  3) Cron example (hourly):
       0 * * * * /usr/bin/python /path/ingest_telegram_to_supabase.py --since 7d >> /var/log/movie_ingest.log 2>&1
"""
import os, re, io, sys, time, argparse, datetime as dt
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto
from supabase import create_client, Client

# ---- Setup
load_dotenv()
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH")
SESSION_NAME = os.getenv("TELEGRAM_SESSION", "movie_ingest.session")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
BUCKET = os.getenv("SUPABASE_STORAGE_BUCKET", "covers")
RLS_USER_UID = os.getenv("RLS_USER_UID")  # optional

assert API_ID and API_HASH and CHANNEL_USERNAME and SUPABASE_URL and SUPABASE_SERVICE_KEY, "Missing required env vars."

sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

@dataclass
class Movie:
    title: Optional[str] = None
    link: Optional[str] = None
    synopsis: Optional[str] = None
    director: Optional[str] = None
    product: Optional[str] = None
    stars: Optional[str] = None
    imdb: Optional[str] = None
    release_info: Optional[str] = None
    genre: Optional[str] = None
    cover_url: Optional[str] = None
    tg_message_id: Optional[int] = None
    tg_date: Optional[str] = None

# Regex patterns (both English & Persian labels)
FIELD_PATTERNS = {
    "title": [r"^Title[:：]\s*(.+)$", r"^عنوان[:：]\s*(.+)$"],
    "link": [r"^Link[:：]\s*(\S+)$", r"^لینک[:：]\s*(\S+)$"],
    "synopsis": [r"^Synopsis[:：]\s*(.+)$", r"^خلاصه[:：]\s*(.+)$"],
    "director": [r"^Director[:：]\s*(.+)$", r"^کارگردان[:：]\s*(.+)$"],
    "product": [r"^Product(?:ion)?[:：]\s*(.+)$", r"^محصول[:：]\s*(.+)$"],
    "stars": [r"^Stars?[:：]\s*(.+)$", r"^بازیگران[:：]\s*(.+)$"],
    "imdb": [r"^IMDB[:：]\s*(.+)$", r"^امتیاز\s*IMDB[:：]\s*(.+)$"],
    "release_info": [r"^Release(?: Info)?[:：]\s*(.+)$", r"^سال(?: انتشار)?[:：]\s*(.+)$"],
    "genre": [r"^Genre[:：]\s*(.+)$", r"^ژانر[:：]\s*(.+)$"],
}

def parse_movie(text: str) -> Dict[str, Any]:
    # Split lines and try to match label:value; also fallback: first line as title if none
    fields: Dict[str, Any] = {}
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for ln in lines:
        for key, pats in FIELD_PATTERNS.items():
            for pat in pats:
                m = re.match(pat, ln, flags=re.IGNORECASE)
                if m:
                    fields[key] = m.group(1).strip()
                    break
            if key in fields:
                break
    if "title" not in fields and lines:
        fields["title"] = lines[0][:200]
    # Compact whitespace
    for k, v in list(fields.items()):
        if isinstance(v, str):
            fields[k] = re.sub(r"\s+", " ", v).strip()
    return fields

def upload_cover_and_get_url(photo_bytes: bytes, filename: str) -> Optional[str]:
    # Upload to Supabase Storage and return public URL (bucket must be public or use getPublicUrl)
    path = f"{int(time.time())}_{filename}"
    res = sb.storage.from_(BUCKET).upload(path, photo_bytes, file_options={"content-type":"image/jpeg", "upsert": False})
    if res:
        # public URL (adjust if you restrict bucket)
        pub = sb.storage.from_(BUCKET).get_public_url(path)
        return pub
    return None

def upsert_movie(m: Movie) -> None:
    payload = {k: v for k, v in asdict(m).items() if v is not None}
    # Allow RLS policies by including an owner user id if needed
    if RLS_USER_UID:
        payload.setdefault("user_id", RLS_USER_UID)
    # upsert by tg_message_id if exists, else title+link combo
    # Note: Ensure a unique index exists in your DB on (tg_message_id) OR (title, link)
    resp = sb.table("movies").upsert(payload, on_conflict="tg_message_id").execute()
    if resp.error:
        print("Supabase upsert error:", resp.error)

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--login", action="store_true", help="Interactive login only (no sync).")
    parser.add_argument("--since", default="30d", help="How far back to fetch (e.g., 7d, 90d).")
    args = parser.parse_args()

    # Since parsing
    now = dt.datetime.utcnow()
    m = re.match(r"(\d+)([dhm])$", args.since.strip())
    delta = None
    if m:
        val, unit = int(m.group(1)), m.group(2)
        if unit == "d":
            delta = dt.timedelta(days=val)
        elif unit == "h":
            delta = dt.timedelta(hours=val)
        elif unit == "m":
            delta = dt.timedelta(minutes=val)
    if delta is None:
        delta = dt.timedelta(days=30)
    since_dt = now - delta

    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()  # will prompt on first run

    if args.login:
        print("Logged in successfully.")
        await client.disconnect()
        return

    async for msg in client.iter_messages(CHANNEL_USERNAME, offset_date=None, reverse=True):
        if msg.date.tzinfo:
            msg_utc = msg.date.astimezone(dt.timezone.utc).replace(tzinfo=None)
        else:
            msg_utc = msg.date
        if msg_utc < since_dt:
            continue

        if not (msg.message and msg.message.strip()):
            continue

        fields = parse_movie(msg.message)
        movie = Movie(**fields)
        movie.tg_message_id = msg.id
        movie.tg_date = msg_utc.isoformat()

        # Photo handling (first photo only)
        if isinstance(msg.media, MessageMediaPhoto):
            b = await client.download_media(msg, file=bytes)
            if b:
                movie.cover_url = upload_cover_and_get_url(b, f"tg_{msg.id}.jpg")

        upsert_movie(movie)
        print(f"Synced message {msg.id}: {movie.title!r}")

    await client.disconnect()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
