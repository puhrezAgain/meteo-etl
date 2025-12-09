"""

etl.sources contains the classes related to different sources of data

Here we use a Template pattern around the BaseSource class to facilitate ç
the addition of new data sources requiring particular hardcoded parameters
in addition to standard url differences and payload differences
"""

from abc import ABC, abstractmethod
from typing import Type, ClassVar, Mapping, Sequence
from enum import Enum
from types import MappingProxyType
from .config import APP_NAME
from .models import (
    BasePayload,
    BaseParamModel,
    MeteoPayload,
    MeteoParams,
    WeatherRecord,
)
from .extract import run_extractor


class SourceName(str, Enum):
    METEO = "etl_meteo"


class BaseSource(ABC):
    NAME: SourceName
    URL: ClassVar[str]
    PAYLOAD_MODEL: ClassVar[Type[BasePayload]]
    REQUEST_PARAM_MODEL: ClassVar[Type[BaseParamModel]]
    STATIC_PARAMS: ClassVar[Mapping[str, str]]
    _params: BaseParamModel
    _extra_params: dict
    data: BasePayload
    raw_data: dict

    def __init__(self, required_params: dict, **extra_params):
        self._params = self.REQUEST_PARAM_MODEL.model_validate(required_params)
        self._extra_params = extra_params

    @property
    def params(self) -> dict:
        return {**self.STATIC_PARAMS, **self._params.model_dump(), **self._extra_params}

    def run_extractor(self, **runtime_params) -> dict:
        self.raw_data = run_extractor(
            self.URL,
            {**self.params, **runtime_params},
            user_agent=f"{APP_NAME}_{self.NAME}",
        )
        return self.raw_data

    def run_transform(self) -> Sequence[WeatherRecord]:
        self.data = self.PAYLOAD_MODEL.model_validate(self.raw_data)
        return self.data.to_records()

    def extract_and_transform(self, **extra_params) -> Sequence[WeatherRecord]:
        self.run_extractor(**extra_params)
        return self.run_transform()


SOURCE_REGISTRY: dict[SourceName, Type[BaseSource]] = {}


def register_source(name: SourceName):
    def _decorator(cls: Type[BaseSource]):
        SOURCE_REGISTRY[name] = cls
        return cls

    return _decorator


@register_source(SourceName.METEO)
class MeteoSource(BaseSource):
    NAME = SourceName.METEO
    URL = "https://api.open-meteo.com/v1/forecast"
    PAYLOAD_MODEL: ClassVar[Type[BasePayload]] = MeteoPayload
    REQUEST_PARAM_MODEL: ClassVar[Type[BaseParamModel]] = MeteoParams
    STATIC_PARAMS = MappingProxyType(
        {
            "hourly": "temperature_2m,precipitation,soil_temperature_18cm,soil_moisture_9_to_27cm,wind_speed_10m,wind_direction_10m,cloud_cover",
        }
    )


def create_source(name: SourceName, params: dict) -> BaseSource:
    return SOURCE_REGISTRY[name](params)
