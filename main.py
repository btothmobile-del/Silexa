import os
import io
import json
import hashlib
import asyncio
import traceback
import feedparser
import httpx
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from openai import OpenAI
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session
from fastapi import Depends
from database import User, UserSettings, PushSubscription, FunnelEvent, PasswordResetToken, create_tables, get_db
from auth import hash_password, verify_password, create_token, get_current_user, SECRET_KEY, ALGORITHM
from jose import jwt as _jwt, JWTError as _JWTError

load_dotenv()

app = FastAPI()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

BRIEFINGS_DIR = Path(os.getenv("BRIEFINGS_DIR", "briefings"))
BRIEFINGS_DIR.mkdir(exist_ok=True)

# Cloudflare R2
import boto3
from botocore.exceptions import ClientError

R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.getenv("R2_BUCKET")
R2_ENABLED = all([R2_ACCOUNT_ID, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET])

if R2_ENABLED:
    r2 = boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto",
    )
    print(f"R2 storage aktív: {R2_BUCKET}")
else:
    r2 = None
    print("R2 nincs konfigurálva, lokális tárolás használatban.")


def r2_put(key: str, data: bytes, content_type: str = "audio/mpeg"):
    if r2:
        r2.put_object(Bucket=R2_BUCKET, Key=key, Body=data, ContentType=content_type)

def r2_get(key: str) -> bytes | None:
    if not r2:
        return None
    try:
        obj = r2.get_object(Bucket=R2_BUCKET, Key=key)
        return obj["Body"].read()
    except ClientError:
        return None

def r2_exists(key: str) -> bool:
    if not r2:
        return False
    try:
        r2.head_object(Bucket=R2_BUCKET, Key=key)
        return True
    except ClientError:
        return False

def r2_delete(key: str):
    if r2:
        try:
            r2.delete_object(Bucket=R2_BUCKET, Key=key)
        except ClientError:
            pass
# Web Push / VAPID
VAPID_PRIVATE_KEY = os.getenv("VAPID_PRIVATE_KEY", "")
VAPID_PUBLIC_KEY = os.getenv("VAPID_PUBLIC_KEY", "")
VAPID_EMAIL = os.getenv("VAPID_EMAIL", "mailto:admin@silexa.app")
PUSH_ENABLED = bool(VAPID_PRIVATE_KEY and VAPID_PUBLIC_KEY)

def send_push_to_all(title: str, body: str, db):
    if not PUSH_ENABLED:
        return
    try:
        from pywebpush import webpush, WebPushException
    except ImportError:
        return
    subs = db.query(PushSubscription).all()
    for sub in subs:
        try:
            webpush(
                subscription_info={"endpoint": sub.endpoint, "keys": {"p256dh": sub.p256dh, "auth": sub.auth}},
                data=json.dumps({"title": title, "body": body}),
                vapid_private_key=VAPID_PRIVATE_KEY,
                vapid_claims={"sub": VAPID_EMAIL},
            )
        except Exception:
            pass

SCHEDULE_CONFIG_FILE = BRIEFINGS_DIR / "schedule_config.json"

ALL_COUNTRIES = ["usa", "uk", "germany", "france", "brazil", "italy", "hungary"]

ALL_SAMPLE_INTERESTS = [
    "technológia", "üzlet", "befektetés", "tudomány", "világpolitika", "sport",
    "kultúra", "egészség", "közélet", "gazdaság", "környezet", "szórakozás",
    "utazás", "oktatás", "autó", "ingatlan",
]

DEFAULT_SCHEDULE_CONFIG = {
    "briefing_time": "06:00",
    "timezone": "Europe/Budapest",
    "is_premium": False,
    "interests": ["világ", "közélet"],
    "language": "magyar",
    "countries": ALL_COUNTRIES,
    "premium_feeds": {},
}

def load_schedule_config() -> dict:
    if SCHEDULE_CONFIG_FILE.exists():
        with open(SCHEDULE_CONFIG_FILE, encoding="utf-8") as f:
            return json.load(f)
    return dict(DEFAULT_SCHEDULE_CONFIG)

def save_schedule_config(config: dict):
    with open(SCHEDULE_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

scheduler = AsyncIOScheduler()

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

# --- Alap feedek országonként ---
BASIC_FEEDS = {
    "usa": [
        "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",
        "https://feeds.npr.org/1001/rss.xml",
        "https://feeds.abcnews.com/abcnews/topstories",
        "https://api.axios.com/feed/",
    ],
    "uk": [
        "http://feeds.bbci.co.uk/news/rss.xml",
        "https://www.theguardian.com/world/rss",
    ],
    "germany": [
        "https://www.spiegel.de/schlagzeilen/index.rss",
        "https://rss.dw.com/xml/rss-de-all",
    ],
    "france": [
        "https://www.france24.com/fr/rss",
        "https://www.lemonde.fr/rss/une.xml",
    ],
    "brazil": [
        "https://g1.globo.com/rss/g1/",
        "https://agenciabrasil.ebc.com.br/rss/ultimasnoticias/feed.xml",
    ],
    "italy": [
        "https://www.ansa.it/sito/notizie/mondo/mondo_rss.xml",
        "https://www.repubblica.it/rss/homepage/rss2.0.xml",
    ],
    "hungary": [
        "https://index.hu/24ora/rss/",
        "https://hvg.hu/rss",
        "https://telex.hu/rss",
        "https://444.hu/feed",
    ],
}


class BriefingRequest(BaseModel):
    interests: list[str]
    language: str = "magyar"
    premium_feeds: dict[str, list[str]] = {}
    is_premium: bool = False
    countries: list[str] = list(BASIC_FEEDS.keys())


def settings_signature(req: "BriefingRequest") -> str:
    """Userek azonos beállítások esetén ugyanazt a (cache-elt) briefinget kapják."""
    payload = {
        "language": req.language,
        "interests": sorted(i.lower().strip() for i in req.interests),
        "is_premium": req.is_premium,
        "countries": sorted(req.countries) if not req.is_premium else None,
        "premium_feeds": {k: sorted(v) for k, v in sorted(req.premium_feeds.items())} if req.is_premium else None,
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode()).hexdigest()[:10]


def req_from_settings(settings: UserSettings) -> BriefingRequest:
    return BriefingRequest(
        interests=json.loads(settings.interests),
        language=settings.language,
        premium_feeds=json.loads(settings.premium_feeds),
        is_premium=settings.is_premium,
        countries=json.loads(settings.countries),
    )


def get_user_from_token(token: str, db: Session) -> User:
    try:
        payload = _jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = int(payload.get("sub"))
    except (_JWTError, TypeError, ValueError):
        raise HTTPException(status_code=401, detail="Érvénytelen token.")
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=401, detail="Felhasználó nem található.")
    return user


class ReadRequest(BaseModel):
    text: str
    voice: str = "nova"


class PreviewRequest(BaseModel):
    interests: list[str]
    language: str = "magyar"
    duration_minutes: int = 5  # 3, 5, vagy 10


class ScheduleConfigRequest(BaseModel):
    briefing_time: str = "06:00"
    timezone: str = "Europe/Budapest"
    is_premium: bool = False
    interests: list[str] = []
    language: str = "magyar"
    countries: list[str] = ALL_COUNTRIES
    premium_feeds: dict[str, list[str]] = {}


class RegisterRequest(BaseModel):
    email: str
    password: str


class LoginRequest(BaseModel):
    email: str
    password: str


class UserSettingsRequest(BaseModel):
    language: str = "magyar"
    voice: str = "nova"
    interests: list[str] = ["világ", "közélet"]
    countries: list[str] = ALL_COUNTRIES
    is_premium: bool = False
    premium_feeds: dict[str, list[str]] = {}
    briefing_time: str = "06:00"
    timezone: str = "Europe/Budapest"


def clean_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text(separator=" ", strip=True)


SEEN_LINKS_FILE = BRIEFINGS_DIR / "seen_links.json"

def load_seen_links() -> set:
    if SEEN_LINKS_FILE.exists():
        with open(SEEN_LINKS_FILE, encoding="utf-8") as f:
            data = json.load(f)
        # Csak az elmúlt 48 óra linkjeit tartjuk (régebbiek kieshetnek)
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
        return {link for link, ts in data.items() if ts >= cutoff}
    return set()

def save_seen_links(seen: set, new_links: set):
    existing = {}
    if SEEN_LINKS_FILE.exists():
        with open(SEEN_LINKS_FILE, encoding="utf-8") as f:
            existing = json.load(f)
    now = datetime.now(timezone.utc).isoformat()
    # Régi linkek törlése (48 óránál régebbiek)
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
    existing = {k: v for k, v in existing.items() if v >= cutoff}
    for link in new_links:
        if link:
            existing[link] = now
    with open(SEEN_LINKS_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False)


def fetch_feed(url: str, seen_links: set = None) -> list[dict]:
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        try:
            resp = httpx.get(url, headers=HEADERS, timeout=15, follow_redirects=True)
            feed = feedparser.parse(resp.text)
        except Exception:
            feed = feedparser.parse(url, request_headers=HEADERS)
        if feed.bozo and not feed.entries:
            print(f"  Feed blokkolt ({url})")
            return []
        articles = []
        for entry in feed.entries[:30]:
            link = entry.get("link", "")
            # Már látott cikk kizárása
            if seen_links and link in seen_links:
                continue
            # Dátum ellenőrzés
            published = entry.get("published_parsed") or entry.get("updated_parsed")
            if published:
                pub_dt = datetime(*published[:6], tzinfo=timezone.utc)
                if pub_dt < cutoff:
                    continue
            summary = clean_html(entry.get("summary", "") or entry.get("description", ""))
            title = entry.get("title", "")
            if title and (summary or title):
                articles.append({
                    "title": title,
                    "summary": summary[:600],
                    "source": feed.feed.get("title", url),
                    "link": link,
                })
        return articles
    except Exception as e:
        print(f"Feed hiba ({url}): {e}")
        return []




async def scheduled_generate():
    """Ütemezett napi generálás: minden regisztrált user saját beállítása szerint,
    de azonos beállítású userek (signature) csak EGYSZER generálnak — költségmegosztás."""
    today = date.today().isoformat()
    db = next(get_db())
    try:
        all_settings = db.query(UserSettings).all()
        if not all_settings:
            return
        by_signature: dict[str, BriefingRequest] = {}
        for s in all_settings:
            req = req_from_settings(s)
            if not req.interests:
                continue
            sig = settings_signature(req)
            by_signature.setdefault(sig, req)
        print(f"Ütemezett generálás: {len(all_settings)} user, {len(by_signature)} egyedi beállítás-kombináció.")
        for sig, req in by_signature.items():
            briefing_key = f"{today}__{sig}"
            existing = _load_briefing_json(briefing_key)
            if existing and existing.get("categories"):
                continue
            try:
                await _generate_briefing_core(req, briefing_key)
            except Exception as e:
                print(f"Briefing hiba ({sig}): {e}")
    finally:
        db.close()


def split_for_tts(text: str, limit: int = 3800) -> list[str]:
    sentences = text.replace("\n\n", ". \n\n").split(". ")
    chunks, current = [], ""
    for s in sentences:
        piece = s if s.endswith(".") else s + "."
        if len(current) + len(piece) + 1 > limit and current:
            chunks.append(current.strip())
            current = piece
        else:
            current = f"{current} {piece}".strip()
    if current.strip():
        chunks.append(current.strip())
    return chunks


async def generate_samples():
    """Minden témakörből generál egy rövid sample hangfájlt (onboarding preview).
    Eredmény: {téma}.sample.mp3 + {téma}.sample.txt + samples_meta.json"""
    today = date.today().isoformat()
    print(f"Sample generálás indítása: {len(ALL_SAMPLE_INTERESTS)} témakör, {today}")

    # Cikkek gyűjtése (minden feed, deduplikáció nélkül)
    all_articles = []
    for country, urls in BASIC_FEEDS.items():
        for url in urls:
            arts = await asyncio.get_running_loop().run_in_executor(None, fetch_feed, url, None)
            for a in arts:
                a["country"] = country
            all_articles.extend(arts)

    if not all_articles:
        print("Sample generálás: nem sikerült cikkeket letölteni.")
        return

    # Rangsorolás GPT-vel (ugyanaz mint a rendes briefingnél)
    sample_articles = all_articles[:200]
    articles_text = "\n".join(
        f"{i}. [{a['country'].upper()}] {a['source']}: {a['title']}" for i, a in enumerate(sample_articles)
    )
    sample_example = "\n".join(
        f'    "{i}": [{{"indices": [0,1,2], "summary": "rövid összefoglaló"}}]'
        for i in ALL_SAMPLE_INTERESTS
    )
    try:
        ranking_resp = await asyncio.get_running_loop().run_in_executor(None, lambda: client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.3,
            response_format={"type": "json_object"},
            messages=[{"role": "user", "content": f"""Csoportosítsd és rangsorold az alábbi híreket témakörönként.
MINDEN témakört töltsd ki — ha nincs direkt hír, válaszd a legtematikusabbat.
Témakörök (MIND szerepeljen a válaszban): {', '.join(ALL_SAMPLE_INTERESTS)}

Válasz JSON (MINDEN kategória szerepeljen):
{{
  "categories": {{
{sample_example}
  }}
}}

Cikkek:
{articles_text}"""}],
        ))
        categories_data = json.loads(ranking_resp.choices[0].message.content).get("categories", {})
        categories_data = {k.lower().strip(): v for k, v in categories_data.items()}
    except Exception as e:
        print(f"Sample ranking hiba: {e}")
        return

    loop = asyncio.get_running_loop()

    async def generate_one_sample(interest: str):
        key = interest.lower().strip()
        stories = categories_data.get(key, [])
        if not stories:
            for cat_key in categories_data:
                if key in cat_key or cat_key in key:
                    stories = categories_data[cat_key]
                    break
        if not stories:
            print(f"  [{interest}] Nincs adat a rankingban, kihagyva.")
            return

        top_text = ""
        for story in stories[:3]:
            indices = story.get("indices", [])
            summaries = " ".join([sample_articles[i]["summary"] for i in indices[:2] if i < len(sample_articles)])
            top_text += f"\nTéma: {story['summary']}\nRészletek: {summaries}\n"

        try:
            script_resp = await loop.run_in_executor(None, lambda: client.chat.completions.create(
                model="gpt-4o-mini",
                temperature=0.7,
                response_format={"type": "json_object"},
                messages=[{"role": "user", "content": f"""Te egy profi hír-elemző és rádióbemondó vagy.
Témakör: {interest}
Mai dátum: {today}

Válaszolj PONTOSAN ebben a JSON formátumban:
{{
  "script": "200-300 szavas magyar rádióstílusú összefoglaló, természetes folyó szöveg, felsorolás nélkül",
  "insights": [
    {{"story": "Sztori neve (max 6 szó)", "why_it_matters": "1-2 mondat: miért fontos?"}}
  ],
  "perspectives": [
    {{"story": "Sztori neve", "sources": [
      {{"name": "Forrás neve", "tone": "positive/negative/neutral", "note": "1 mondat a nézőpontról"}}
    ]}}
  ]
}}

Hírek:
{top_text}"""}],
            ))
            parsed = json.loads(script_resp.choices[0].message.content)
            script = parsed.get("script", "").strip()
            insights = parsed.get("insights", [])
            perspectives = parsed.get("perspectives", [])
        except Exception as e:
            print(f"  [{interest}] Script generálás hiba: {e}")
            return

        # TTS
        try:
            chunks = split_for_tts(script)
            tts_results = await asyncio.gather(*[
                loop.run_in_executor(None, lambda c=chunk: client.audio.speech.create(
                    model="tts-1", voice="nova", input=c, response_format="mp3",
                )) for chunk in chunks
            ])
            audio_bytes = b"".join(r.content for r in tts_results)
        except Exception as e:
            print(f"  [{interest}] TTS hiba: {e}")
            return

        mp3_key = f"{interest}.sample.mp3"
        json_key = f"{interest}.sample.json"
        sample_data = {"script": script, "insights": insights, "perspectives": perspectives}
        if R2_ENABLED:
            r2_put(mp3_key, audio_bytes, "audio/mpeg")
            r2_put(json_key, json.dumps(sample_data, ensure_ascii=False).encode("utf-8"), "application/json")
        else:
            (BRIEFINGS_DIR / mp3_key).write_bytes(audio_bytes)
            (BRIEFINGS_DIR / json_key).write_text(json.dumps(sample_data, ensure_ascii=False), encoding="utf-8")
        print(f"  [{interest}] Sample kész ({len(audio_bytes)//1024}kb)")

    await asyncio.gather(*[generate_one_sample(i) for i in ALL_SAMPLE_INTERESTS])

    # Metadata mentése
    meta = {"generated_date": today, "interests": ALL_SAMPLE_INTERESTS}
    meta_bytes = json.dumps(meta, ensure_ascii=False).encode("utf-8")
    if R2_ENABLED:
        r2_put("samples_meta.json", meta_bytes, "application/json")
    else:
        (BRIEFINGS_DIR / "samples_meta.json").write_bytes(meta_bytes)
    print(f"Sample generálás kész: {today}")


def apply_schedule(config: dict):
    scheduler.remove_all_jobs()
    hour, minute = config.get("briefing_time", "06:00").split(":")
    tz = config.get("timezone", "Europe/Budapest")
    scheduler.add_job(
        scheduled_generate,
        CronTrigger(hour=int(hour), minute=int(minute), timezone=tz),
        id="daily_briefing",
        replace_existing=True,
    )
    scheduler.add_job(
        generate_samples,
        CronTrigger(day_of_week="mon", hour=6, minute=0, timezone="Europe/Budapest"),
        id="weekly_samples",
        replace_existing=True,
    )
    print(f"Ütemezés beállítva: {hour}:{minute} ({tz}), sample: hétfő 06:00")


@app.on_event("startup")
async def startup_event():
    create_tables()
    config = load_schedule_config()
    apply_schedule(config)
    scheduler.start()
    print("Scheduler elindult.")


@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown(wait=False)


@app.get("/api/schedule")
async def get_schedule():
    return load_schedule_config()


@app.post("/api/schedule")
async def update_schedule(req: ScheduleConfigRequest):
    config = req.model_dump()
    # Basic felhasználóknál az idő mindig 06:00 marad
    if not req.is_premium:
        config["briefing_time"] = "06:00"
    save_schedule_config(config)
    apply_schedule(config)
    return {"ok": True, "config": config}


@app.get("/api/preview/{interest}")
async def get_preview(interest: str):
    """Sample hangfájl metaadata az adott témakörre (onboarding preview)."""
    safe = interest.lower().strip()
    mp3_key = f"{safe}.sample.mp3"
    json_key = f"{safe}.sample.json"
    meta_key = "samples_meta.json"

    exists = r2_exists(mp3_key) if R2_ENABLED else (BRIEFINGS_DIR / mp3_key).exists()
    if not exists:
        raise HTTPException(status_code=404, detail="Még nincs sample ehhez a témához.")

    if R2_ENABLED:
        json_data = r2_get(json_key)
        sample = json.loads(json_data) if json_data else {}
        meta_data = r2_get(meta_key)
        generated_date = json.loads(meta_data).get("generated_date", "") if meta_data else ""
    else:
        json_path = BRIEFINGS_DIR / json_key
        sample = json.loads(json_path.read_text(encoding="utf-8")) if json_path.exists() else {}
        meta_path = BRIEFINGS_DIR / meta_key
        generated_date = json.loads(meta_path.read_text()).get("generated_date", "") if meta_path.exists() else ""

    return {
        "preview_id": safe + ".sample",
        "script": sample.get("script", "..."),
        "insights": sample.get("insights", []),
        "perspectives": sample.get("perspectives", []),
        "interest": interest,
        "generated_date": generated_date,
    }


@app.get("/api/preview/{preview_id}/audio")
async def get_preview_audio(preview_id: str):
    if not all(c.isalnum() or c in "-_." for c in preview_id):
        raise HTTPException(status_code=400, detail="Érvénytelen azonosító.")
    key = f"{preview_id}.mp3"
    if R2_ENABLED:
        data = r2_get(key)
        if not data:
            raise HTTPException(status_code=404, detail="Nincs hanganyag.")
        return StreamingResponse(io.BytesIO(data), media_type="audio/mpeg",
                                  headers={"Content-Length": str(len(data)), "Accept-Ranges": "bytes"})
    else:
        audio_path = BRIEFINGS_DIR / key
        if not audio_path.exists():
            raise HTTPException(status_code=404, detail="Nincs hanganyag.")
        return FileResponse(audio_path, media_type="audio/mpeg")


ADMIN_SECRET = os.getenv("ADMIN_SECRET", "silexa-admin")

@app.get("/api/admin-delete-user")
async def admin_delete_user(email: str, secret: str = "", db: Session = Depends(get_db)):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Tiltott.")
    user = db.query(User).filter(User.email == email).first()
    if not user:
        raise HTTPException(status_code=404, detail="Felhasználó nem található.")
    db.query(UserSettings).filter(UserSettings.user_id == user.id).delete()
    db.delete(user)
    db.commit()
    return {"ok": True, "deleted": email}


class FunnelEventRequest(BaseModel):
    event: str
    session_id: str = ""

FUNNEL_EVENTS = {"landing_view", "onboarding_start", "registered"}

@app.post("/api/funnel/event")
async def track_funnel(req: FunnelEventRequest, db: Session = Depends(get_db)):
    if req.event not in FUNNEL_EVENTS:
        raise HTTPException(status_code=400, detail="Ismeretlen esemény.")
    db.add(FunnelEvent(event=req.event, session_id=req.session_id or None))
    db.commit()
    return {"ok": True}


@app.get("/api/admin-funnel")
async def admin_funnel(secret: str = "", db: Session = Depends(get_db)):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Tiltott.")
    from sqlalchemy import func
    rows = db.query(FunnelEvent.event, func.count(FunnelEvent.id)).group_by(FunnelEvent.event).all()
    counts = {r[0]: r[1] for r in rows}
    landing = counts.get("landing_view", 0)
    started = counts.get("onboarding_start", 0)
    registered = counts.get("registered", 0)
    return {
        "landing_view": landing,
        "onboarding_start": started,
        "registered": registered,
        "start_rate": round(started / landing * 100, 1) if landing else 0,
        "reg_rate": round(registered / started * 100, 1) if started else 0,
        "overall_rate": round(registered / landing * 100, 1) if landing else 0,
    }


@app.get("/api/push/vapid-public-key")
async def get_vapid_public_key():
    return {"public_key": VAPID_PUBLIC_KEY}


class PushSubscribeRequest(BaseModel):
    endpoint: str
    p256dh: str
    auth: str


@app.post("/api/push/subscribe")
async def push_subscribe(req: PushSubscribeRequest, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    existing = db.query(PushSubscription).filter(PushSubscription.endpoint == req.endpoint).first()
    if existing:
        return {"ok": True}
    sub = PushSubscription(user_id=current_user.id, endpoint=req.endpoint, p256dh=req.p256dh, auth=req.auth)
    db.add(sub)
    db.commit()
    return {"ok": True}


@app.delete("/api/push/subscribe")
async def push_unsubscribe(req: PushSubscribeRequest, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    db.query(PushSubscription).filter(PushSubscription.endpoint == req.endpoint).delete()
    db.commit()
    return {"ok": True}


@app.get("/api/admin-r2-files")
async def admin_r2_files(secret: str = ""):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Tiltott.")
    if not R2_ENABLED:
        return {"files": [], "error": "R2 nincs konfigurálva."}
    try:
        paginator = r2.get_paginator("list_objects_v2")
        files = []
        json_meta = {}  # key -> {interests, language, story_count}
        for page in paginator.paginate(Bucket=R2_BUCKET):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                size_kb = round(obj["Size"] / 1024, 1)
                last_modified = obj["LastModified"].isoformat()
                date = key[:10] if len(key) >= 10 and key[4] == "-" else "unknown"
                files.append({"key": key, "size_kb": size_kb, "date": date, "last_modified": last_modified})
                # JSON fájlokból metaadatok kinyerése
                if key.endswith(".json") and not key.endswith("samples_meta.json"):
                    try:
                        data = r2_get(key)
                        if data:
                            briefing = json.loads(data)
                            briefing_key = key.replace(".json", "")
                            json_meta[briefing_key] = {
                                "interests": [c["category"] for c in briefing.get("categories", [])],
                                "language": briefing.get("language", ""),
                                "story_count": sum(c.get("story_count", 0) for c in briefing.get("categories", [])),
                                "duration_seconds": briefing.get("duration_seconds"),
                            }
                    except Exception:
                        pass
        files.sort(key=lambda f: f["date"], reverse=True)
        return {"files": files, "meta": json_meta}
    except Exception as e:
        return {"files": [], "error": str(e)}


@app.get("/api/admin-r2-delete")
async def admin_r2_delete(key: str, secret: str = ""):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Tiltott.")
    r2_delete(key)
    return {"ok": True, "deleted": key}


@app.get("/api/admin-r2-delete-all-audio")
async def admin_r2_delete_all_audio(secret: str = ""):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Tiltott.")
    if not R2_ENABLED:
        return {"ok": False, "error": "R2 nincs konfigurálva."}
    try:
        deleted = []
        paginator = r2.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=R2_BUCKET):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".mp3") or key.endswith(".txt"):
                    r2_delete(key)
                    deleted.append(key)
        return {"ok": True, "deleted_count": len(deleted)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/admin-users")
async def admin_users(secret: str = "", db: Session = Depends(get_db)):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Tiltott.")
    users = db.query(User).order_by(User.created_at.desc()).all()
    return {"users": [{"id": u.id, "email": u.email, "status": u.status, "created_at": u.created_at.isoformat() if u.created_at else None} for u in users]}


@app.get("/api/admin-set-status")
async def admin_set_status(email: str, status: str, secret: str = "", db: Session = Depends(get_db)):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Tiltott.")
    if status not in ("freemium", "basic", "premium", "admin"):
        raise HTTPException(status_code=400, detail="Érvénytelen státusz.")
    user = db.query(User).filter(User.email == email).first()
    if not user:
        raise HTTPException(status_code=404, detail="Felhasználó nem található.")
    user.status = status
    db.commit()
    return {"ok": True, "email": email, "status": status}


@app.get("/api/admin-reset")
async def admin_reset(secret: str = ""):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Tiltott.")
    today = date.today().isoformat()
    deleted = []
    # Töröljük a local fájlokat (mind a régi {today}.json, mind az új {today}__{sig}.json formátum)
    for f in list(BRIEFINGS_DIR.glob(f"{today}.json")) + list(BRIEFINGS_DIR.glob(f"{today}__*.json")) \
            + list(BRIEFINGS_DIR.glob(f"{today}*.mp3")) + list(BRIEFINGS_DIR.glob(f"{today}*.txt")):
        f.unlink()
        deleted.append(f.name)
    # Töröljük az R2 fájlokat
    if R2_ENABLED:
        try:
            resp = r2.list_objects_v2(Bucket=R2_BUCKET, Prefix=today)
            for obj in resp.get("Contents", []):
                r2_delete(obj["Key"])
                deleted.append(obj["Key"])
        except ClientError:
            pass
    return {"ok": True, "deleted": deleted}


@app.get("/api/admin-debug")
async def admin_debug(secret: str = ""):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Tiltott.")
    return {
        "R2_ACCOUNT_ID": bool(os.getenv("R2_ACCOUNT_ID")),
        "R2_ACCESS_KEY_ID": bool(os.getenv("R2_ACCESS_KEY_ID")),
        "R2_SECRET_ACCESS_KEY": bool(os.getenv("R2_SECRET_ACCESS_KEY")),
        "R2_BUCKET": os.getenv("R2_BUCKET"),
        "R2_ENABLED": R2_ENABLED,
    }


@app.get("/api/ping")
async def ping():
    return {"pong": True}

@app.get("/api/admin-generate")
async def admin_generate_now(secret: str = ""):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Tiltott.")
    try:
        await scheduled_generate()
        return {"ok": True, "message": "Mai briefingek legenerálva minden egyedi beállítás-kombinációra."}
    except HTTPException as e:
        return {"ok": False, "error": e.detail}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/admin-generate-samples")
async def admin_generate_samples(secret: str = ""):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Tiltott.")
    try:
        await generate_samples()
        return {"ok": True, "message": f"{len(ALL_SAMPLE_INTERESTS)} sample hangfájl legenerálva."}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/admin-sample-stats")
async def admin_sample_stats(secret: str = ""):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Tiltott.")

    all_articles = []
    for country, urls in BASIC_FEEDS.items():
        for url in urls:
            arts = await asyncio.get_running_loop().run_in_executor(None, fetch_feed, url, None)
            for a in arts:
                a["country"] = country
            all_articles.extend(arts)

    if not all_articles:
        return {"ok": False, "error": "Nem sikerült cikkeket letölteni."}

    sample_articles = all_articles[:350]
    articles_text = "\n".join(
        f"{i}. [{a['country'].upper()}] {a['source']}: {a['title']}" for i, a in enumerate(sample_articles)
    )
    try:
        ranking_resp = await asyncio.get_running_loop().run_in_executor(None, lambda: client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.3,
            response_format={"type": "json_object"},
            messages=[{"role": "user", "content": f"""Csoportosítsd az alábbi híreket témakörönként.
Rendeld hozzá a témakört, ha van legalább lazán kapcsolódó cikk is. Ha tényleg semmi sem kapcsolódik, hagyd ki.
Témakörök: {', '.join(ALL_SAMPLE_INTERESTS)}

Válasz JSON:
{{
  "categories": {{
    "témakör": [{{"indices": [0,1,2], "summary": "rövid összefoglaló"}}]
  }}
}}

Cikkek:
{articles_text}"""}],
        ))
        categories_data = json.loads(ranking_resp.choices[0].message.content).get("categories", {})
        categories_data = {k.lower().strip(): v for k, v in categories_data.items()}
    except Exception as e:
        return {"ok": False, "error": f"Ranking hiba: {e}"}

    stats = []
    for interest in ALL_SAMPLE_INTERESTS:
        key = interest.lower().strip()
        stories = categories_data.get(key, [])
        if not stories:
            for cat_key in categories_data:
                if key in cat_key or cat_key in key:
                    stories = categories_data[cat_key]
                    break
        stats.append({
            "topic": interest,
            "story_count": len(stories),
            "has_content": len(stories) > 0,
        })

    return {
        "ok": True,
        "total_articles_fetched": len(all_articles),
        "articles_ranked": len(sample_articles),
        "topics": stats,
    }


@app.post("/api/auth/register")
async def register(req: RegisterRequest, db: Session = Depends(get_db)):
    if db.query(User).filter(User.email == req.email).first():
        raise HTTPException(status_code=400, detail="Ez az email cím már foglalt.")
    if len(req.password) < 6:
        raise HTTPException(status_code=400, detail="A jelszónak legalább 6 karakter kell.")
    user = User(email=req.email, hashed_password=hash_password(req.password))
    db.add(user)
    db.commit()
    db.refresh(user)
    # Alapértelmezett beállítások létrehozása
    settings = UserSettings(user_id=user.id)
    db.add(settings)
    db.commit()
    token = create_token(user.id)
    return {"token": token, "email": user.email}


@app.post("/api/auth/login")
async def login(req: LoginRequest, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == req.email).first()
    if not user or not verify_password(req.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Hibás email vagy jelszó.")
    token = create_token(user.id)
    return {"token": token, "email": user.email}


@app.get("/api/auth/me")
async def me(current_user: User = Depends(get_current_user)):
    return {"id": current_user.id, "email": current_user.email, "status": current_user.status}


class ForgotPasswordRequest(BaseModel):
    email: str

class ResetPasswordRequest(BaseModel):
    token: str
    password: str

async def _send_reset_email(to_email: str, reset_url: str):
    api_key = os.getenv("RESEND_API_KEY", "")
    if not api_key:
        raise HTTPException(status_code=500, detail="Email küldés nincs konfigurálva.")
    async with httpx.AsyncClient() as client_http:
        res = await client_http.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "from": "Silexa <noreply@silexa.hu>",
                "to": [to_email],
                "subject": "Jelszó visszaállítás – Silexa",
                "html": f"""
                <div style="font-family:sans-serif;max-width:480px;margin:0 auto;padding:32px">
                  <h2 style="color:#6366f1">Silexa – Jelszó visszaállítás</h2>
                  <p>Kattints az alábbi gombra az új jelszó beállításához. A link 15 percig érvényes.</p>
                  <a href="{reset_url}" style="display:inline-block;margin:24px 0;padding:12px 28px;background:#6366f1;color:#fff;border-radius:8px;text-decoration:none;font-weight:600">Jelszó visszaállítása</a>
                  <p style="color:#64748b;font-size:0.85rem">Ha nem te kérted, hagyd figyelmen kívül ezt az emailt.</p>
                </div>
                """,
            },
            timeout=10,
        )
    if res.status_code >= 400:
        raise HTTPException(status_code=500, detail="Email küldési hiba.")

@app.post("/api/auth/forgot-password")
async def forgot_password(req: ForgotPasswordRequest, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == req.email).first()
    # mindig 200-at adunk vissza, hogy ne lehessen enumolni az emaileket
    if not user:
        return {"ok": True}
    token = os.urandom(32).hex()
    expires = datetime.now(timezone.utc) + timedelta(minutes=15)
    db.add(PasswordResetToken(user_id=user.id, token=token, expires_at=expires))
    db.commit()
    reset_url = f"https://www.silexa.hu/reset-password.html?token={token}"
    await _send_reset_email(user.email, reset_url)
    return {"ok": True}

@app.post("/api/auth/reset-password")
async def reset_password(req: ResetPasswordRequest, db: Session = Depends(get_db)):
    now = datetime.now(timezone.utc)
    record = db.query(PasswordResetToken).filter(
        PasswordResetToken.token == req.token,
        PasswordResetToken.used == False,
    ).first()
    if not record:
        raise HTTPException(status_code=400, detail="Érvénytelen vagy már felhasznált link.")
    expires = record.expires_at
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)
    if expires < now:
        raise HTTPException(status_code=400, detail="A link lejárt. Kérj új jelszó-visszaállítást.")
    user = db.query(User).filter(User.id == record.user_id).first()
    if not user:
        raise HTTPException(status_code=400, detail="Felhasználó nem található.")
    user.hashed_password = hash_password(req.password)
    record.used = True
    db.commit()
    token = create_token(user.id)
    return {"ok": True, "token": token, "email": user.email}


@app.get("/api/user/settings")
async def get_user_settings(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    settings = db.query(UserSettings).filter(UserSettings.user_id == current_user.id).first()
    if not settings:
        settings = UserSettings(user_id=current_user.id)
        db.add(settings)
        db.commit()
    return settings.to_dict()


@app.post("/api/user/settings")
async def save_user_settings(
    req: UserSettingsRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    settings = db.query(UserSettings).filter(UserSettings.user_id == current_user.id).first()
    if not settings:
        settings = UserSettings(user_id=current_user.id)
        db.add(settings)
    settings.language = req.language
    settings.voice = req.voice
    settings.interests = json.dumps(req.interests, ensure_ascii=False)
    settings.countries = json.dumps(req.countries, ensure_ascii=False)
    settings.is_premium = req.is_premium
    settings.premium_feeds = json.dumps(req.premium_feeds, ensure_ascii=False)
    if req.is_premium:
        settings.briefing_time = req.briefing_time
    else:
        settings.briefing_time = "06:00"
    settings.timezone = req.timezone
    db.commit()
    return settings.to_dict()


@app.post("/api/generate-briefing")
async def generate_briefing(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    settings = db.query(UserSettings).filter(UserSettings.user_id == current_user.id).first()
    if not settings:
        raise HTTPException(status_code=400, detail="Nincs beállítás a felhasználóhoz.")
    req = req_from_settings(settings)
    today = date.today().isoformat()
    sig = settings_signature(req)
    briefing_key = f"{today}__{sig}"
    existing = _load_briefing_json(briefing_key)
    if existing and existing.get("categories"):
        return existing
    return await _generate_briefing_core(req, briefing_key)


async def _generate_briefing_core(req: BriefingRequest, briefing_key: str) -> dict:
    today = briefing_key.split("__")[0]
    json_path = BRIEFINGS_DIR / f"{briefing_key}.json"

    # Ha már van érvényes briefing ehhez a kulcshoz, visszaadjuk azt
    existing = _load_briefing_json(briefing_key)
    if existing and existing.get("date") and existing.get("categories"):
        return existing

    # Cikkek gyűjtése országonként jelölve
    seen_links = load_seen_links()
    all_articles = []
    if req.is_premium and req.premium_feeds:
        for interest_feeds in req.premium_feeds.values():
            for url in interest_feeds:
                for art in fetch_feed(url, seen_links):
                    art["country"] = "custom"
                    all_articles.append(art)
    else:
        for country in req.countries:
            for url in BASIC_FEEDS.get(country, []):
                articles = fetch_feed(url, seen_links)
                print(f"  {country} | {url} : {len(articles)} cikk")
                for art in articles:
                    art["country"] = country
                    all_articles.append(art)
    print(f"Összes cikk: {len(all_articles)}")

    if not all_articles:
        raise HTTPException(status_code=404, detail="Nem sikerült cikkeket leszedni.")

    # Cikkek egyenletes keverése országonként (round-robin), max 150 a rankinghoz
    from collections import defaultdict
    by_country: dict[str, list] = defaultdict(list)
    for a in all_articles:
        by_country[a["country"]].append(a)
    interleaved = []
    max_rounds = 20
    for round_i in range(max_rounds):
        for country_articles in by_country.values():
            if round_i < len(country_articles):
                interleaved.append(country_articles[round_i])
        if len(interleaved) >= 300:
            break
    ranking_articles = interleaved[:300]

    # 1. lépés: rangsorolás és csoportosítás kategóriánként
    articles_for_ranking = "\n".join([
        f"[{i}] [ország:{a['country']}] ({a['source']}) {a['title']}"
        for i, a in enumerate(ranking_articles)
    ])

    interests_str = ", ".join(req.interests)
    example_categories = "\n".join(
        f'    "{interest}": [{{"indices": [0, 5, 12], "country_count": 3, "countries": ["usa","uk","germany"], "summary": "rövid összefoglaló"}}, ...]'
        for interest in req.interests
    )
    ranking_prompt = f"""Az alábbi hírcikkek különböző országok forrásaiból érkeztek. Minden cikknél jelölve van az ország.

A JSON kulcsai PONTOSAN ezek legyenek (MINDEN kategóriát töltsd ki): {req.interests}

Feladatod:
1. Azonosítsd azokat a híreket amelyek ugyanarról az eseményről szólnak (különböző országokból)
2. Csoportosítsd őket a megadott kategóriák szerint — MINDEN kategóriához adj legalább 3-5 sztorit
3. Rangsorold a sztorikat aszerint hány KÜLÖNBÖZŐ ORSZÁG foglalkozik ugyanazzal a témával
4. Ha egy kategóriához kevés direkt hír van, válaszd ki a legtematikusabb cikkeket

Válaszolj PONTOSAN ebben a JSON formátumban (MINDEN kategória szerepeljen):
{{
  "categories": {{
{example_categories}
  }}
}}

Cikkek:
{articles_for_ranking}"""

    ranking_response = await asyncio.get_running_loop().run_in_executor(
        None, lambda: client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": ranking_prompt}],
            response_format={"type": "json_object"},
            temperature=0.2,
        )
    )

    ranking_data = json.loads(ranking_response.choices[0].message.content)
    raw_categories = ranking_data.get("categories", {})
    # Normalizáljuk a kulcsokat: kisbetűs, strip
    categories_data = {k.lower().strip(): v for k, v in raw_categories.items()}
    print(f"GPT kategóriák: {list(categories_data.keys())}")
    print(f"Kért érdeklődési körök: {req.interests}")

    # Duplikáció-szűrés: ha egy sztori (indexek alapján) már szerepelt egy korábbi
    # kategóriában, a következő kategóriából kivesszük, hogy ne ismétlődjön a riportban
    used_story_indices: set[int] = set()

    def dedupe_stories(stories: list) -> list:
        kept = []
        for story in stories:
            indices = set(story.get("indices", []))
            if not indices:
                kept.append(story)
                continue
            overlap = len(indices & used_story_indices) / len(indices)
            if overlap >= 0.5:
                continue  # ugyanaz a sztori, már elhangzott egy korábbi témakörben
            used_story_indices.update(indices)
            kept.append(story)
        return kept

    # 2. lépés: minden kategóriához külön szöveges briefing — párhuzamosan (TTS később, egyben)
    voice = "nova"

    async def generate_category(interest: str):
        # Próbálj pontos egyezést, majd részleges egyezést
        key = interest.lower().strip()
        stories = categories_data.get(key, [])
        if not stories:
            for cat_key in categories_data:
                if key in cat_key or cat_key in key:
                    stories = categories_data[cat_key]
                    break
        if not stories:
            print(f"  [{interest}] Nincs találat a GPT kategóriákban, kihagyva.")
            return None
        if not stories:
            return None

        stories = dedupe_stories(stories)
        if not stories:
            return None

        top_text = ""
        used_sources = {}  # name -> link
        for story in stories[:5]:
            indices = story.get("indices", [])
            for i in indices:
                if i < len(ranking_articles):
                    a = ranking_articles[i]
                    if a["source"] not in used_sources:
                        used_sources[a["source"]] = a.get("link", "")
            countries = story.get("countries", [])
            country_count = story.get("country_count", len(set(ranking_articles[i]["country"] for i in indices if i < len(ranking_articles))))
            sources = list({ranking_articles[i]["source"] for i in indices if i < len(ranking_articles)})
            summaries = " ".join([ranking_articles[i]["summary"] for i in indices[:3] if i < len(ranking_articles)])
            top_text += (
                f"\n[{country_count} ország: {', '.join(countries[:5])} | Források: {', '.join(sources[:3])}]\n"
                f"Téma: {story['summary']}\n"
                f"Részletek: {summaries}\n"
            )

        briefing_prompt = f"""Te egy profi hír-elemző és rádióbemondó vagy.

Témakör: {interest}
Összefoglaló nyelve: {req.language}
Mai dátum: {today}

Az alábbi hírek a mai nap legnépszerűbb "{interest}" témájú sztorijai, több ország forrásai által megerősítve.

Készíts egy JSON választ a következő struktúrában:

{{
  "script": "A teljes audio összefoglaló {req.language} nyelven, MINIMUM 700 szó, ideálisan 800-900 szó. Természetes rádióbemondói stílus. Minden egyes hírnél — a tények ismertetése UTÁN — természetesen add hozzá: miért fontos ez a hír a hallgatónak, a világnak vagy a jövőnek? Ez legyen 1-3 mondat, tömör és értelmes, nem közhelyes. Ahol eltérő nézőpontok vannak, utalj rá. Folyó szöveg, felsorolás és linkek nélkül. Ha kevés a konkrét sztori, bővítsd ki a háttérrel, kontextussal és a lehetséges következményekkel.",
  "insights": [
    {{
      "story": "Sztori rövid neve (max 6 szó)",
      "why_it_matters": "1-3 mondat {req.language} nyelven: miért fontos ez a hír? Mi a valódi hatása — gazdasági, társadalmi, politikai vagy jövőbeli következmény?"
    }}
  ],
  "perspectives": [
    {{
      "story": "Sztori rövid neve (max 6 szó)",
      "sources": [
        {{
          "name": "Forrás neve",
          "tone": "positive|neutral|negative",
          "score": 0.0,
          "note": "1 mondatos leírás a forrás megközelítéséről {req.language} nyelven"
        }}
      ]
    }}
  ]
}}

Szabályok:
- A score: 0.0 = nagyon negatív, 0.5 = semleges, 1.0 = nagyon pozitív
- perspectives: csak ahol legalább 2 forrás eltérő nézőpontot képvisel
- insights: minden top sztorihoz kötelező

Top sztorik:
{top_text}"""

        loop = asyncio.get_running_loop()

        briefing_response = await loop.run_in_executor(None, lambda: client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": briefing_prompt}],
            response_format={"type": "json_object"},
            temperature=0.6,
        ))

        raw = json.loads(briefing_response.choices[0].message.content)
        script = raw.get("script", "").strip()
        perspectives = raw.get("perspectives", [])
        insights = raw.get("insights", [])

        return {
            "category": interest,
            "script": script,
            "story_count": len(stories[:5]),
            "insights": insights,
            "perspectives": perspectives,
            "sources": [{"name": k, "link": v} for k, v in used_sources.items()],
        }

    try:
        results = await asyncio.gather(*[generate_category(i) for i in req.interests])
        categories_result = [r for r in results if r is not None]
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Generálási hiba: {str(e)}")

    if not categories_result:
        raise HTTPException(status_code=500, detail="Nem sikerült összefoglalót generálni.")

    # 3. lépés: a kategóriák szövegéből EGY összefűzött, duplikáció-mentes hangfájl
    combined_script = "\n\n".join(
        f"{c['category'][:1].upper()}{c['category'][1:]}. {c['script']}" for c in categories_result
    )

    loop = asyncio.get_running_loop()
    chunks = split_for_tts(combined_script)
    try:
        tts_results = await asyncio.gather(*[
            loop.run_in_executor(None, lambda c=chunk: client.audio.speech.create(
                model="tts-1", voice=voice, input=c, response_format="mp3",
            )) for chunk in chunks
        ])
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Hangfájl generálási hiba: {str(e)}")

    combined_audio = b"".join(r.content for r in tts_results)

    combined_audio_path = BRIEFINGS_DIR / f"{briefing_key}-COMBINED.mp3"
    if R2_ENABLED:
        r2_put(combined_audio_path.name, combined_audio)
    else:
        combined_audio_path.write_bytes(combined_audio)

    # MP3 hossz becslése: tts-1 ~32kbps
    duration_seconds = round(len(combined_audio) * 8 / 32000)

    briefing_data = {
        "date": today,
        "key": briefing_key,
        "language": req.language,
        "interests": req.interests,
        "duration_seconds": duration_seconds,
        "categories": categories_result,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(briefing_data, f, ensure_ascii=False, indent=2)
    if R2_ENABLED:
        r2_put(f"{briefing_key}.json", json.dumps(briefing_data, ensure_ascii=False).encode("utf-8"), "application/json")

    # Mentjük a felhasznált cikkek linkjeit a deduplikációhoz
    used_links = {a["link"] for a in ranking_articles if a.get("link")}
    save_seen_links(seen_links, used_links)

    # Push értesítés
    db_push = next(get_db())
    try:
        cats = ", ".join(c["category"] for c in categories_result[:3])
        send_push_to_all("🎙️ Silexa – Napi briefing kész!", f"{cats} és más témák várnak.", db_push)
    finally:
        db_push.close()

    return briefing_data


def _load_briefing_json(date_str: str) -> dict | None:
    """Load briefing JSON from local disk or R2."""
    local = BRIEFINGS_DIR / f"{date_str}.json"
    if local.exists():
        with open(local, encoding="utf-8") as f:
            return json.load(f)
    if R2_ENABLED:
        data = r2_get(f"{date_str}.json")
        if data:
            parsed = json.loads(data.decode("utf-8"))
            # cache locally
            with open(local, "w", encoding="utf-8") as f:
                json.dump(parsed, f, ensure_ascii=False, indent=2)
            return parsed
    return None


def _user_signature(current_user: User, db: Session) -> str:
    settings = db.query(UserSettings).filter(UserSettings.user_id == current_user.id).first()
    if not settings:
        raise HTTPException(status_code=400, detail="Nincs beállítás a felhasználóhoz.")
    return settings_signature(req_from_settings(settings))


@app.get("/api/briefings")
async def list_briefings(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    sig = _user_signature(current_user, db)
    suffix = f"__{sig}.json"
    briefings = []
    keys = {p.stem for p in BRIEFINGS_DIR.glob(f"*{suffix}")}

    if R2_ENABLED:
        try:
            paginator = r2.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=R2_BUCKET):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith(suffix):
                        keys.add(key[:-5])  # strip .json
        except Exception:
            pass

    for key in sorted(keys, reverse=True):
        data = _load_briefing_json(key)
        if not data:
            continue
        categories = data.get("categories", [])
        preview = categories[0]["script"][:120] + "..." if categories else ""
        briefings.append({
            "date": data["date"],
            "language": data.get("language", ""),
            "interests": data.get("interests", []),
            "categories": [c["category"] for c in categories],
            "preview": preview,
        })
    return {"briefings": briefings, "v": "2"}


@app.get("/api/briefings/{briefing_date}")
async def get_briefing(briefing_date: str, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    sig = _user_signature(current_user, db)
    data = _load_briefing_json(f"{briefing_date}__{sig}")
    if not data:
        raise HTTPException(status_code=404, detail="Nincs ilyen briefing.")
    return data


@app.get("/api/briefings/{briefing_date}/audio")
async def get_briefing_audio(briefing_date: str, token: str = ""):
    db = next(get_db())
    try:
        user = get_user_from_token(token, db)
        sig = _user_signature(user, db)
    finally:
        db.close()
    key = f"{briefing_date}__{sig}-COMBINED.mp3"
    if R2_ENABLED:
        data = r2_get(key)
        if not data:
            raise HTTPException(status_code=404, detail="Nincs hanganyag.")
        return StreamingResponse(io.BytesIO(data), media_type="audio/mpeg",
                                  headers={"Content-Length": str(len(data)), "Accept-Ranges": "bytes"})
    else:
        audio_path = BRIEFINGS_DIR / key
        if not audio_path.exists():
            raise HTTPException(status_code=404, detail="Nincs hanganyag.")
        return FileResponse(audio_path, media_type="audio/mpeg")


@app.delete("/api/briefings/{briefing_date}")
async def delete_briefing(briefing_date: str, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    sig = _user_signature(current_user, db)
    key = f"{briefing_date}__{sig}"
    (BRIEFINGS_DIR / f"{key}.json").unlink(missing_ok=True)
    (BRIEFINGS_DIR / f"{key}-COMBINED.mp3").unlink(missing_ok=True)
    if R2_ENABLED:
        r2_delete(f"{key}.json")
        r2_delete(f"{key}-COMBINED.mp3")
    return {"ok": True}


@app.post("/api/transcribe")
async def transcribe(audio: UploadFile = File(...)):
    content = await audio.read()
    if not content:
        raise HTTPException(status_code=400, detail="Üres hangfájl.")
    audio_file = io.BytesIO(content)
    audio_file.name = audio.filename or "recording.webm"
    transcript = client.audio.transcriptions.create(
        model="whisper-1",
        file=audio_file,
        language="hu",
    )
    return {"text": transcript.text}


from fastapi.responses import RedirectResponse

@app.get("/")
async def root():
    return RedirectResponse(url="/index.html")

app.mount("/", StaticFiles(directory="static", html=True), name="static")
