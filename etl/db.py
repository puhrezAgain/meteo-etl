"""
etl.db contains our database table definitions 
"""
from sqlalchemy import ForeignKey, Index, Integer, Float, Text, func, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID
from sqlalchemy.orm import declarative_base, relationship, mapped_column, Mapped
from datetime import datetime
import uuid

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
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    timezone: Mapped[str | None] = mapped_column(Text)
    temperature: Mapped[float | None] = mapped_column(Float)
    precipitation: Mapped[float | None] = mapped_column(Float)
    wind_speed: Mapped[float | None] = mapped_column(Float)
    fetch_id: Mapped[uuid.UUID | None] = mapped_column(UUID, ForeignKey("fetch_metadata.id", ondelete="CASCADE"))

    fetch = relationship("FetchMetadata", back_populates="observation")

    __table_args__ = (
        UniqueConstraint("latitude", "longitude", "timestamp", name="u_loc_time"),
        Index("ix_obs:loc:ts", "latitude", "longitude", "timestamp"),
    )