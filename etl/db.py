"""
etl.db contains our database table definitions and sqlachemy engine/session
"""

import uuid
from datetime import datetime
from enum import Enum
from sqlalchemy import (
    ForeignKey,
    Index,
    Integer,
    Float,
    Text,
    func,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.orm import (
    declarative_base,
    relationship,
    mapped_column,
    Mapped,
    sessionmaker,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID, ENUM as SQLENUM
from . import config

Base = declarative_base()


class FetchStatus(str, Enum):
    PENDING = "pending"
    ERROR = "error"
    SUCCESS = "success"


class FetchMetadata(Base):
    """
    FetchMetadata represents metadata about a fetch job, allowing introspection and observability
    """

    __tablename__ = "fetch_metadata"

    id: Mapped[uuid.UUID] = mapped_column(UUID, primary_key=True, default=uuid.uuid4)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=func.now()
    )
    request_timestamp: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False)
    request_params: Mapped[dict] = mapped_column(JSONB, nullable=False)
    request_url: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[FetchStatus] = mapped_column(
        SQLENUM(FetchStatus, name="fetch_status"),
        default=FetchStatus.PENDING,
        server_default=FetchStatus.PENDING,
    )
    response_status: Mapped[int | None] = mapped_column(Integer)
    response_data: Mapped[dict | None] = mapped_column(JSONB)

    observations = relationship(
        "Observation", back_populates="fetch", passive_deletes=True
    )


class Observation(Base):
    """
    Observation represents weather information about a particular location (longitude and latitude)
    """

    __tablename__ = "weather_observations"

    id: Mapped[uuid.UUID] = mapped_column(UUID, primary_key=True, default=uuid.uuid4)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP, nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP, nullable=False, default=func.now(), onupdate=func.now()
    )
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
    timezone: Mapped[str | None] = mapped_column(Text)
    temperature: Mapped[float | None] = mapped_column(Float)
    precipitation: Mapped[float | None] = mapped_column(Float)
    wind_speed: Mapped[float | None] = mapped_column(Float)
    fetch_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID, ForeignKey("fetch_metadata.id", ondelete="CASCADE")
    )

    fetch = relationship("FetchMetadata", back_populates="observations")

    __table_args__ = (
        UniqueConstraint("latitude", "longitude", "timestamp", name="u_loc_time"),
        Index("ix_obs:loc:ts", "latitude", "longitude", "timestamp"),
    )


engine = create_engine(config.DB_URL, echo=config.DEBUG)

SessionLocal = sessionmaker(bind=engine)
