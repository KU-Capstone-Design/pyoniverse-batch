import os
from datetime import datetime
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse

import pandas as pd
from pandas import DataFrame

from lib.db.factory import RepositoryFactory
from lib.domain.event.model.crawled_event_schema import CrawledBrandEventSchema
from lib.domain.event.model.service_event_schema import ServiceBrandEventSchema
from lib.interface.processor_ifs import ProcessorIfs
from lib.interface.repository_ifs import RepositoryIfs


class EventProcessor(ProcessorIfs):
    def __init__(self, *args, **kwargs):
        super().__init__("events")
        self.__repository: RepositoryIfs = RepositoryFactory.get_instance(
            _type="mongo", db_name=os.getenv("MONGO_CRAWLING_DB")
        )

    def _preprocess(self, date: datetime, *args, **kwargs) -> DataFrame:
        data = self.__repository.find(
            rel_name=self._name,
            project={"_id": False, "created_at": False, "updated_at": False},
        )
        errors = CrawledBrandEventSchema().validate(data, many=True)
        if errors:
            self.logger.error(f"Crawling Event Schema Validation Error: {len(errors)}")
            raise RuntimeError(f"Crawling Event Schema Validation Error: {len(errors)}")
        self.logger.info(f"Initial data: {len(data)}")
        return DataFrame(data)

    def _process(self, data: DataFrame, date: datetime, *args, **kwargs) -> DataFrame:
        data["name"] = data["name"].map(lambda x: x.lower())
        # Replace Image Url
        data["image"] = data["image"].map(
            lambda x: {
                "thumb": os.getenv("IMAGE_DOMAIN") + urlparse(x["thumb"]).path[4:]
                if x["thumb"]
                else None,
                "others": [
                    os.getenv("IMAGE_DOMAIN") + urlparse(y).path[4:]
                    for y in x["others"]
                ],
            }
        )
        data = data[data["image"].map(lambda x: pd.notna(x["thumb"]))]
        data["brand"] = data["crawled_info"].map(lambda x: x["brand"])

        # 2. crawled_infos
        data.rename(columns={"crawled_info": "crawled_infos"}, inplace=True)
        data["crawled_infos"] = data["crawled_infos"].map(lambda x: [x])
        return data

    def _postprocess(
        self, data: DataFrame, date: datetime, *args, **kwargs
    ) -> Sequence[Mapping[str, Any]]:
        data = data[
            [
                "brand",
                "name",
                "image",
                "description",
                "crawled_infos",
                "start_at",
                "end_at",
            ]
        ]
        data = data.to_dict("records")
        errors = ServiceBrandEventSchema().validate(data, many=True)
        if errors:
            self.logger.error(f"Service Event Schema Validation Error: {len(errors)}")
            raise RuntimeError(f"Service Event Schema Validation Error: {len(errors)}")
        return data
