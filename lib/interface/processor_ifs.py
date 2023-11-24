import logging
from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import Any, Literal, Mapping, Sequence

from pandas import DataFrame


class ProcessorIfs(metaclass=ABCMeta):
    def __init__(self, name: str):
        self._name = name
        self.logger = logging.getLogger(__name__)

    def run(
        self, date: datetime, *args, **kwargs
    ) -> Mapping[Literal["data"], Sequence[Mapping[str, Any]]]:
        self.logger.info("Start Preprocessing")
        data: DataFrame = self._preprocess(date, *args, **kwargs)
        self.logger.info("Start Processing")
        data = self._process(data, date, *args, **kwargs)
        self.logger.info("Start Postprocessing")
        data: Sequence[Mapping[str, Any]] = self._postprocess(
            data, date, *args, **kwargs
        )
        self.logger.info(f"{self._name} result: {len(data)}")
        return {
            "data": data,
        }

    @abstractmethod
    def _preprocess(self, date: datetime, *args, **kwargs) -> DataFrame:
        pass

    @abstractmethod
    def _process(self, data: DataFrame, date: datetime, *args, **kwargs) -> DataFrame:
        pass

    @abstractmethod
    def _postprocess(
        self, data: DataFrame, date: datetime, *args, **kwargs
    ) -> Sequence[Mapping[str, Any]]:
        pass
