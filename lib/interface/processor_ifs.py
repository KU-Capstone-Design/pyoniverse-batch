from abc import ABCMeta, abstractmethod
from typing import Any, Mapping, Sequence

from pandas import DataFrame


class ProcessorIfs(metaclass=ABCMeta):
    def run(self, data: Sequence[Mapping[str, Any]], *args, **kwargs) -> DataFrame:
        data: DataFrame = self._preprocess(data, *args, **kwargs)
        data = self._run(data, *args, **kwargs)
        data = self._postprocess(data, *args, **kwargs)
        return data

    @abstractmethod
    def _preprocess(
        self, data: Sequence[Mapping[str, Any]], *args, **kwargs
    ) -> DataFrame:
        pass

    @abstractmethod
    def _run(self, data: DataFrame, *args, **kwargs) -> DataFrame:
        pass

    @abstractmethod
    def _postprocess(self, data: DataFrame, *args, **kwargs) -> DataFrame:
        pass
