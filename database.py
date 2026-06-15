import json
import os
from datetime import datetime, timezone
from pathlib import Path
from sqlalchemy import create_engine, Column, Integer, String, Boolean, Text, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker, Session

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL:
    # Railway PostgreSQL: postgresql:// -> postgresql+psycopg2://
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg2://", 1)
    elif DATABASE_URL.startswith("postgresql://"):
        DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://", 1)
    engine = create_engine(DATABASE_URL)
else:
    DB_PATH = Path("briefings/silexa.db")
    engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    status = Column(String, default="freemium", nullable=False)  # freemium | basic | premium | admin
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class UserSettings(Base):
    __tablename__ = "user_settings"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, unique=True, index=True, nullable=False)
    language = Column(String, default="magyar")
    voice = Column(String, default="nova")
    interests = Column(Text, default='["világ","közélet"]')   # JSON
    countries = Column(Text, default='["usa","uk","germany","france","brazil","italy","hungary"]')
    is_premium = Column(Boolean, default=False)
    premium_feeds = Column(Text, default="{}")
    briefing_time = Column(String, default="06:00")
    timezone = Column(String, default="Europe/Budapest")

    def to_dict(self) -> dict:
        return {
            "language": self.language,
            "voice": self.voice,
            "interests": json.loads(self.interests),
            "countries": json.loads(self.countries),
            "is_premium": self.is_premium,
            "premium_feeds": json.loads(self.premium_feeds),
            "briefing_time": self.briefing_time,
            "timezone": self.timezone,
        }


class FunnelEvent(Base):
    __tablename__ = "funnel_events"

    id = Column(Integer, primary_key=True)
    event = Column(String, nullable=False, index=True)  # landing_view | onboarding_start | registered
    session_id = Column(String, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class PushSubscription(Base):
    __tablename__ = "push_subscriptions"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, index=True, nullable=False)
    endpoint = Column(Text, unique=True, nullable=False)
    p256dh = Column(Text, nullable=False)
    auth = Column(Text, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


def create_tables():
    Base.metadata.create_all(bind=engine)
    _migrate()

def _migrate():
    """Add columns that may not exist in older DB versions."""
    with engine.connect() as conn:
        try:
            conn.execute(__import__("sqlalchemy").text(
                "ALTER TABLE users ADD COLUMN status VARCHAR DEFAULT 'freemium' NOT NULL"
            ))
            conn.commit()
        except Exception:
            conn.rollback()  # column already exists — safe to ignore


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
