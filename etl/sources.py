"""

etl.sources contains the classes related to different sources of data

Here we use a Template pattern around the BaseSource class to facilitate ç
the addition of new data sources requiring particular hardcoded parameters 
in addition to standard url differences and payload differences
"""

from abc import ABC, abstractmethod
from typing import Type, ClassVar, Mapping
from types import MappingProxyType
from .constants import SourceName, APP_NAME
from .models import BasePayload, BaseParamModel, MeteoPayload, MeteoParams, FetchRecord
from .extract import run_extractor

SOURCE_REGISTRY: dict[str, type] = {}

def registre_source(name: str):
    def _decorator(cls):
        SOURCE_REGISTRY[name] = cls
        return cls
    return _decorator

class BaseSource(ABC):
    URL: ClassVar[str] 
    PAYLOAD_MODEL: ClassVar[Type[BasePayload]] 
    REQUEST_PARAM_MODEL: ClassVar[Type[BaseParamModel]] 
    STATIC_PARAMS: ClassVar[Mapping[str, str]] 
    params: BaseParamModel
    data: BasePayload
    raw_data: dict

    @classmethod
    def fetch_data(cls, url, params):
        return run_extractor(url, {
            **cls.STATIC_PARAMS,
            **params,
        }, user_agent=APP_NAME)    
            
    @classmethod
    def parse_payload(cls, payload: dict) -> BasePayload:
        return cls.PAYLOAD_MODEL.model_validate(payload)

    def __init__(self, params: dict):
        self.params = self.REQUEST_PARAM_MODEL.model_validate(params)

    def run_extractor(self):
        self.raw_data = self.fetch_data(self.URL, self.params.model_dump())
        return self.raw_data
    
    def run_transform(self):
        self.data = self.parse_payload(self.raw_data)
        return self.data.to_records()
    
    def extract_and_transform(self):
        self.run_extractor()
        return self.run_transform()
    
    @abstractmethod
    def create_fetch_metadata(self, raw_metadata, raw_data):
        pass

@registre_source(SourceName.METEO)   
class MeteoSource(BaseSource):
    URL = "https://api.open-meteo.com/v1/forecast"
    PAYLOAD_MODEL: ClassVar[Type[BasePayload]] = MeteoPayload
    REQUEST_PARAM_MODEL: ClassVar[Type[BaseParamModel]] = MeteoParams
    STATIC_PARAMS = MappingProxyType({
        "hourly": "temperature_2m,precipitation,soil_temperature_18cm,soil_moisture_9_to_27cm,wind_speed_10m,wind_direction_10m,cloud_cover",
    })

    def create_fetch_metadata(self, raw_metadata, raw_data):
        raise NotImplementedError
    
def create_source(name: SourceName, params: dict) -> BaseSource:
    return SOURCE_REGISTRY[name](params)