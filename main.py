import os
import io
import json
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
from database import User, UserSettings, PushSubscription, create_tables, get_db
from auth import hash_password, verify_password, create_token, get_current_user

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
    """Ütemezett napi generálás: regisztrált userek briefingei."""
    config = load_schedule_config()
    today = date.today().isoformat()
    json_path = BRIEFINGS_DIR / f"{today}.json"
    already_exists = json_path.exists() or (R2_ENABLED and r2_exists(f"{today}.json"))
    if not already_exists and config.get("interests"):
        print(f"Briefing generálás: {today}")
        req = BriefingRequest(
            interests=config.get("interests", ["világ", "közélet"]),
            language=config.get("language", "magyar"),
            premium_feeds=config.get("premium_feeds", {}),
            is_premium=config.get("is_premium", False),
            countries=config.get("countries", ALL_COUNTRIES),
        )
        try:
            await generate_briefing(req)
        except Exception as e:
            print(f"Briefing hiba: {e}")


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
    print(f"Ütemezés beállítva: {hour}:{minute} ({tz})")


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
    """Visszaadja a legfrissebb elérhető briefinget az adott témakörre (onboarding preview)."""
    config = load_schedule_config()
    safe = interest.replace("/", "-").replace(" ", "_")
    safe_tz = config.get("timezone", "Europe/Budapest").replace("/", "_")
    hour = int(config.get("briefing_time", "06:00").split(":")[0])
    duration = config.get("duration_minutes", 5)
    lang = config.get("language", "magyar").replace(" ", "_")

    found_key = None
    for days_back in range(7):
        d = (datetime.now(timezone.utc) - timedelta(days=days_back)).date().isoformat()
        key = f"{d}-{safe}-{lang}-{safe_tz}-{hour:02d}-{duration}.mp3"
        if R2_ENABLED:
            if r2_exists(key):
                found_key = key
                break
        else:
            if (BRIEFINGS_DIR / key).exists():
                found_key = key
                break

    if not found_key:
        raise HTTPException(status_code=404, detail="Még nincs elérhető briefing ehhez a témához.")

    txt_key = found_key.replace(".mp3", ".txt")
    if R2_ENABLED:
        txt_data = r2_get(txt_key)
        script_preview = txt_data.decode("utf-8") if txt_data else "..."
    else:
        txt_path = BRIEFINGS_DIR / txt_key
        script_preview = txt_path.read_text(encoding="utf-8") if txt_path.exists() else "..."

    return {
        "preview_id": found_key.replace(".mp3", ""),
        "script": script_preview,
        "interest": interest,
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
        return StreamingResponse(io.BytesIO(data), media_type="audio/mpeg")
    else:
        audio_path = BRIEFINGS_DIR / key
        if not audio_path.exists():
            raise HTTPException(status_code=404, detail="Nincs hanganyag.")
        return FileResponse(audio_path, media_type="audio/mpeg")


ADMIN_SECRET = os.getenv("ADMIN_SECRET", "silexa-admin")
DEMO_INTERESTS = ["technológia", "üzlet", "befektetés", "tudomány", "világpolitika", "sport"]

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
        for page in paginator.paginate(Bucket=R2_BUCKET):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                size_kb = round(obj["Size"] / 1024, 1)
                last_modified = obj["LastModified"].isoformat()
                # date from filename: YYYY-MM-DD-...
                date = key[:10] if len(key) >= 10 and key[4] == "-" else "unknown"
                files.append({"key": key, "size_kb": size_kb, "date": date, "last_modified": last_modified})
        files.sort(key=lambda f: f["date"], reverse=True)
        return {"files": files}
    except Exception as e:
        return {"files": [], "error": str(e)}


@app.get("/api/admin-r2-delete")
async def admin_r2_delete(key: str, secret: str = ""):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Tiltott.")
    r2_delete(key)
    return {"ok": True, "deleted": key}


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
    if R2_ENABLED:
        try:
            resp = r2.list_objects_v2(Bucket=R2_BUCKET, Prefix=f"{today}-")
            for obj in resp.get("Contents", []):
                r2_delete(obj["Key"])
                deleted.append(obj["Key"])
        except ClientError:
            pass
    else:
        json_path = BRIEFINGS_DIR / f"{today}.json"
        if json_path.exists():
            json_path.unlink()
            deleted.append(json_path.name)
        for f in list(BRIEFINGS_DIR.glob(f"{today}-*.mp3")) + list(BRIEFINGS_DIR.glob(f"{today}-*.txt")):
            f.unlink()
            deleted.append(f.name)
    json_path = BRIEFINGS_DIR / f"{today}.json"
    if json_path.exists():
        json_path.unlink()
        deleted.append(json_path.name)
    return {"ok": True, "deleted": deleted}


@app.get("/api/ping")
async def ping():
    return {"pong": True}

@app.get("/api/admin-generate")
async def admin_generate_now(secret: str = ""):
    if secret != ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Tiltott.")
    asyncio.create_task(generate_briefing(BriefingRequest(
        interests=DEMO_INTERESTS,
        language="magyar",
        countries=ALL_COUNTRIES,
    )))
    return {"ok": True, "message": "Generálás elindítva a háttérben."}


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
async def generate_briefing(req: BriefingRequest):
    today = date.today().isoformat()
    json_path = BRIEFINGS_DIR / f"{today}.json"

    # Ha már van mai briefing, visszaadjuk azt
    if json_path.exists():
        with open(json_path, encoding="utf-8") as f:
            data = json.load(f)
        if data.get("categories"):
            return data

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
    first_interest = req.interests[0] if req.interests else "általános"
    ranking_prompt = f"""Az alábbi hírcikkek különböző országok forrásaiból érkeztek. Minden cikknél jelölve van az ország.

A JSON kulcsai PONTOSAN ezek legyenek: {req.interests}

Feladatod:
1. Azonosítsd azokat a híreket amelyek ugyanarról az eseményről szólnak (különböző országokból)
2. Csoportosítsd őket a megadott kategóriák szerint
3. Rangsorold a sztorikat aszerint hány KÜLÖNBÖZŐ ORSZÁG foglalkozik ugyanazzal a témával — nem az összes cikk száma számít, hanem a különböző országok száma
4. Minden kategóriából a top 5 olyan sztori amely a legtöbb különböző országban jelent meg

Válaszolj PONTOSAN ebben a JSON formátumban:
{{
  "categories": {{
    "{first_interest}": [
      {{"indices": [0, 5, 12], "country_count": 3, "countries": ["usa","uk","germany"], "summary": "rövid összefoglaló"}},
      ...
    ]
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

    # 2. lépés: minden kategóriához külön briefing + TTS — párhuzamosan
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
            # Ha semmi nem egyezik, vegyük az első elérhető kategóriát
            for cat_key, cat_stories in categories_data.items():
                if cat_stories:
                    stories = cat_stories
                    break
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

        tts_response = await loop.run_in_executor(None, lambda: client.audio.speech.create(
            model="tts-1-hd",
            voice=voice,
            input=script,
            response_format="mp3",
        ))

        sched = load_schedule_config()
        safe_interest = interest.replace("/", "-").replace(" ", "_")
        safe_tz = sched.get("timezone", "Europe/Budapest").replace("/", "_")
        hour = int(sched.get("briefing_time", "06:00").split(":")[0])
        duration = sched.get("duration_minutes", 5)
        lang = req.language.replace(" ", "_")
        category_audio_path = BRIEFINGS_DIR / f"{today}-{safe_interest}-{lang}-{safe_tz}-{hour:02d}-{duration}.mp3"
        audio_key = category_audio_path.name
        if R2_ENABLED:
            r2_put(audio_key, tts_response.content)
            r2_put(audio_key.replace(".mp3", ".txt"), script[:400].encode("utf-8"), "text/plain")
        else:
            category_audio_path.write_bytes(tts_response.content)
            category_audio_path.with_suffix(".txt").write_text(script[:400] + "...", encoding="utf-8")

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

    briefing_data = {
        "date": today,
        "language": req.language,
        "interests": req.interests,
        "categories": categories_result,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(briefing_data, f, ensure_ascii=False, indent=2)
    if R2_ENABLED:
        r2_put(f"{today}.json", json.dumps(briefing_data, ensure_ascii=False).encode("utf-8"), "application/json")

    # Mentjük a felhasznált cikkek linkjeit a deduplikációhoz
    used_links = {a["link"] for a in ranking_articles if a.get("link")}
    save_seen_links(seen_links, used_links)

    # Frissítjük a schedule config-ot az aktuális beállításokkal (ütemező számára)
    existing_config = load_schedule_config()
    existing_config.update({
        "interests": req.interests,
        "language": req.language,
        "countries": req.countries,
        "is_premium": req.is_premium,
        "premium_feeds": req.premium_feeds,
    })
    save_schedule_config(existing_config)

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


@app.get("/api/briefings")
async def list_briefings():
    briefings = []
    # Collect date strings from local files
    local_dates = {p.stem for p in BRIEFINGS_DIR.glob("[0-9]*.json")}

    # Also collect from R2
    if R2_ENABLED:
        try:
            paginator = r2.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=R2_BUCKET):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith(".json") and len(key) == 15 and key[4] == "-" and key[7] == "-":
                        local_dates.add(key[:-5])  # strip .json
        except Exception:
            pass

    for date_str in sorted(local_dates, reverse=True):
        data = _load_briefing_json(date_str)
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
async def get_briefing(briefing_date: str):
    data = _load_briefing_json(briefing_date)
    if not data:
        raise HTTPException(status_code=404, detail="Nincs ilyen briefing.")
    return data


@app.get("/api/briefings/{briefing_date}/audio/{category}")
async def get_briefing_audio(briefing_date: str, category: str):
    safe = category.replace("/", "-").replace(" ", "_")
    if R2_ENABLED:
        # R2: listázzuk a matching objektumokat
        try:
            resp = r2.list_objects_v2(Bucket=R2_BUCKET, Prefix=f"{briefing_date}-{safe}-")
            objects = resp.get("Contents", [])
            mp3s = [o["Key"] for o in objects if o["Key"].endswith(".mp3")]
        except ClientError:
            mp3s = []
        if not mp3s:
            raise HTTPException(status_code=404, detail="Nincs hanganyag.")
        data = r2_get(mp3s[0])
        if not data:
            raise HTTPException(status_code=404, detail="Nincs hanganyag.")
        return StreamingResponse(io.BytesIO(data), media_type="audio/mpeg")
    else:
        matches = list(BRIEFINGS_DIR.glob(f"{briefing_date}-{safe}-*.mp3"))
        if not matches:
            raise HTTPException(status_code=404, detail="Nincs hanganyag.")
        return FileResponse(matches[0], media_type="audio/mpeg")


@app.delete("/api/briefings/{briefing_date}")
async def delete_briefing(briefing_date: str):
    json_path = BRIEFINGS_DIR / f"{briefing_date}.json"
    audio_path = BRIEFINGS_DIR / f"{briefing_date}.mp3"
    json_path.unlink(missing_ok=True)
    audio_path.unlink(missing_ok=True)
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
    return RedirectResponse(url="/onboarding.html")

app.mount("/", StaticFiles(directory="static", html=True), name="static")
