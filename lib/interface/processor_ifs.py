import logging
import os
from abc import ABCMeta, abstractmethod
from typing import Any, Mapping, Sequence

from pandas import DataFrame


class ProcessorIfs(metaclass=ABCMeta):
    def __init__(self, name: str):
        self.logger = logging.getLogger(f"processor.{name}")
        self.__configure_logger()

    def run(self, *args, **kwargs) -> DataFrame:
        self.logger.info("Start Preprocessing")
        data: DataFrame = self._preprocess(*args, **kwargs)
        self.logger.info("Done Preprocessing")
        self.logger.info("Start Processing")
        data = self._process(data, *args, **kwargs)
        self.logger.info("Done Processing")
        self.logger.info("Start Postprocessing")
        data = self._postprocess(data, *args, **kwargs)
        self.logger.info("Done Postprocessing")
        self.logger.info(f"Result: {len(data)}")
        return data

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
