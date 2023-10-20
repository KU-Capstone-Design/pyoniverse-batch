import logging
import os
from abc import ABCMeta, abstractmethod
from typing import Any, Mapping, Sequence

from pandas import DataFrame


class ProcessorIfs(metaclass=ABCMeta):
    def __init__(self, name: str):
        self._name = name
        self.logger = logging.getLogger(f"batch.processor.{name}")
        self.__configure_logger()

    def run(self, *args, **kwargs) -> Mapping[str, Sequence[Mapping[str, Any]]]:
        self.logger.info("Start Preprocessing")
        data: DataFrame = self._preprocess(*args, **kwargs)
        self.logger.info("Done Preprocessing")
        self.logger.info("Start Processing")
        data = self._process(data, *args, **kwargs)
        self.logger.info("Done Processing")
        self.logger.info("Start Postprocessing")
        data: Sequence[Mapping[str, Any]] = self._postprocess(data, *args, **kwargs)
        self.logger.info("Done Postprocessing")
        self.logger.info("Find updated data")
        updated = self.__find_updated(data)
        self.logger.info(f"Result: {len(data)}")
        return {
            "data": data,
            "updated": updated,
        }

    @abstractmethod
    def _preprocess(self, *args, **kwargs) -> DataFrame:
        pass

    @abstractmethod
    def _process(self, data: DataFrame, *args, **kwargs) -> DataFrame:
        pass

    @abstractmethod
    def _postprocess(
        self, data: DataFrame, *args, **kwargs
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
                updated.append({"id": datum["id"]})
        else:
            for datum in data:
                updated.append(
                    {
                        "crawled_infos.spider": [
                            x["spider"] for x in datum["crawled_infos"]
                        ],
                        "crawled_infos.id": [x["id"] for x in datum["crawled_infos"]],
                    }
                )
        return updated
