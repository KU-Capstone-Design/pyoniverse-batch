from typing import Any, Mapping, Sequence

from pandas import DataFrame

from lib.interface.processor_ifs import ProcessorIfs


class BrandProcessor(ProcessorIfs):
    def _preprocess(
        self, data: Sequence[Mapping[str, Any]], *args, **kwargs
    ) -> DataFrame:
        pass

    def _run(self, data: DataFrame, *args, **kwargs) -> DataFrame:
        pass

    def _postprocess(self, data: DataFrame, *args, **kwargs) -> DataFrame:
        pass
