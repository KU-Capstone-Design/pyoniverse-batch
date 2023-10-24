import logging
import os
from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import Any, Literal, Mapping, Sequence

from pandas import DataFrame


class ProcessorIfs(metaclass=ABCMeta):
    def __init__(self, name: str):
        self._name = name
        self.logger = logging.getLogger(f"batch.processor.{name}")
        self.__configure_logger()

    def run(
        self, date: datetime, *args, **kwargs
    ) -> Mapping[Literal["data", "updated"], Sequence[Mapping[str, Any]]]:
        self.logger.info("Start Preprocessing")
        data: DataFrame = self._preprocess(date, *args, **kwargs)
        self.logger.info("Done Preprocessing")
        self.logger.info("Start Processing")
        data = self._process(data, date, *args, **kwargs)
        self.logger.info("Done Processing")
        self.logger.info("Start Postprocessing")
        data: Sequence[Mapping[str, Any]] = self._postprocess(
            data, date, *args, **kwargs
        )
        self.logger.info("Done Postprocessing")
        self.logger.info("Find updated data")
        updated = self.__find_updated(data)
        self.logger.info(f"Result: {len(data)}")
        return {
            "data": data,
            "updated": updated,
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

    def __configure_logger(self):
        match os.getenv("STAGE"):
            case "prod":
                self.logger.setLevel(logging.INFO)
            case _:
                self.logger.setLevel(logging.DEBUG)

    def __find_updated(
        self, data: Sequence[Mapping[str, Any]]
    ) -> Sequence[Mapping[str, Any]]:
        updated = []
        if self._name == "brands":
            # id 기준 업데이트
            for datum in data:
                updated.append({"id": datum["id"], "status": 2})
        else:
            for datum in data:
                updated.append(
                    {
                        "crawled_infos": datum["crawled_infos"],
                        "status": 2,  # 현재 업데이트 되는 데이터의 status는 2로 맞춘다.
                    }
                )
        return updated
