"""

etl.models centralizes the pydantic models to be used throughout the application

"""

import uuid
from pydantic import BaseModel, field_serializer
from datetime import datetime
from pathlib import Path
from typing import Sequence, List, Optional
from .db import FetchStatus


class BaseParamModel(BaseModel):
    pass


class MeteoParams(BaseParamModel):
    longitude: float
    latitude: float


class FetchUpdate(BaseModel):
    fetch_id: uuid.UUID
    response_status: int
    error_data: Optional[dict] = None
    status: FetchStatus = FetchStatus.PENDING
    payload_path: Optional[Path] = None

    @field_serializer("payload_path")
    def serialize_payload_path(self, value: Path) -> str:
        return str(value)

    model_config = {"extra": "allow"}


class WeatherRecord(BaseModel):
    latitude: float
    longitude: float
    timestamp: datetime
    temperature: Optional[float]
    precipitation: Optional[float]
    soil_temperature: Optional[float]
    soil_moisture: Optional[float]
    wind_speed: Optional[float]
    wind_direction: Optional[float]
    cloud_cover: Optional[float]


class RawMeteo(BaseModel):
    time: List[datetime]
    temperature_2m: List[Optional[float]]
    precipitation: List[Optional[float]]
    soil_temperature_18cm: List[Optional[float]]
    soil_moisture_9_to_27cm: List[Optional[float]]
    wind_speed_10m: List[Optional[float]]
    wind_direction_10m: List[Optional[float]]
    cloud_cover: List[Optional[float]]


class BasePayload(BaseModel):
    def to_records(self) -> Sequence[WeatherRecord]:
        raise NotImplementedError


class MeteoPayload(BasePayload):
    hourly: RawMeteo
    latitude: float
    longitude: float

    def to_records(self) -> Sequence[WeatherRecord]:
        payload = self.hourly
        num_records = len(payload.time)

        return [
            WeatherRecord(
                latitude=round(self.latitude, 1),
                longitude=round(self.longitude, 1),
                timestamp=payload.time[i],
                temperature=payload.temperature_2m[i],
                precipitation=payload.precipitation[i],
                soil_temperature=payload.soil_temperature_18cm[i],
                soil_moisture=payload.soil_moisture_9_to_27cm[i],
                wind_speed=payload.wind_speed_10m[i],
                wind_direction=payload.wind_direction_10m[i],
                cloud_cover=payload.cloud_cover[i],
            )
            for i in range(num_records)
        ]
