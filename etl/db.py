"""
etl.db contains our database table definitions and sqlachemy engine/session
"""
import os
import uuid
from . import config
from datetime import datetime
from sqlalchemy import ForeignKey, Index, Integer, Float, Text, func, UniqueConstraint, create_engine
from sqlalchemy.orm import declarative_base, relationship, mapped_column, Mapped, sessionmaker
from sqlalchemy.engine.url import URL
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID

Base = declarative_base()

class FetchMetadata(Base):
    __tablename__ = "fetch_metadata"

    id: Mapped[uuid.UUID] = mapped_column(UUID, primary_key=True, default=uuid.uuid4)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=func.now())
    request_timestamp: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False)
    request_params: Mapped[dict] = mapped_column(JSONB, nullable=False)
    request_url: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, server_default="pending")
    response_status: Mapped[int | None] = mapped_column(Integer)
    response_data: Mapped[dict | None] = mapped_column(JSONB)

    observations = relationship("Observation", back_populates="fetch", passive_deletes=True)

    
class Observation(Base):
    __tablename__ = "weather_observations"

    id: Mapped[uuid.UUID] = mapped_column(UUID, primary_key=True, default=uuid.uuid4)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, default=func.now(), onupdate=func.now())
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    timezone: Mapped[str | None] = mapped_column(Text)
    temperature: Mapped[float | None] = mapped_column(Float)
    precipitation: Mapped[float | None] = mapped_column(Float)
    wind_speed: Mapped[float | None] = mapped_column(Float)
    fetch_id: Mapped[uuid.UUID | None] = mapped_column(UUID, ForeignKey("fetch_metadata.id", ondelete="CASCADE"))

    fetch = relationship("FetchMetadata", back_populates="observations")

    __table_args__ = (
        UniqueConstraint("latitude", "longitude", "timestamp", name="u_loc_time"),
        Index("ix_obs:loc:ts", "latitude", "longitude", "timestamp"),
    )

def get_db_url() -> str:
    return os.environ.get("DATABASE_URL") or URL.create(
        drivername="postgresql+psycopg2",
        username=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ.get("DB_HOST", "localhost"),
        port=int(os.environ.get("DB_PORT", "5432")),
        database=os.environ["DB_NAME"],
    ).render_as_string(hide_password=False)

engine = create_engine(
    get_db_url(),
    future=True,
    echo=os.environ.get("DEBUG", False)
)

SessionLocal = sessionmaker(
    bind=engine, expire_on_commit=False,
    future=True
)