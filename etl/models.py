"""

etl.models centralizes the pydantic models to be used throughout the application

"""
from pydantic import BaseModel
from datetime import datetime
from typing import Sequence, List

class BaseParamModel(BaseModel):
    pass

class MeteoParams(BaseParamModel):
    longitude: float
    latitude: float

class PartialFetchRecord(BaseModel):
    request_timestamp: datetime
    request_url: str
    request_params: dict

class FetchRecord(PartialFetchRecord):
    response_body: dict
    response_status: int
    
class WeatherRecord(BaseModel):
    latitude: float
    longitude: float
    timestamp: datetime
    temperature: float
    precipitation: float
    soil_temperature:  float
    soil_moisture:  float
    wind_speed:  float
    wind_direction:  float
    cloud_cover: float
 

    
class RawMeteo(BaseModel):
    time: List[datetime]
    temperature_2m: List[float]
    precipitation: List[float]
    soil_temperature_18cm: List[float]
    soil_moisture_9_to_27cm: List[float]
    wind_speed_10m: List[float]
    wind_direction_10m: List[float]
    cloud_cover: List[float]


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


