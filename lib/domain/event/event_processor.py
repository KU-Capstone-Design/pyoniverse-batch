import logging
import os
from datetime import datetime
from typing import Type
from urllib.parse import urlparse

from pandas import DataFrame

from lib.db import Repository, RepositoryFactory
from lib.processor.event.schema.crawled_event_schema import CrawledBrandEventSchema


class EventProcessor:
    logger = logging.getLogger("EventProcessor")

    def __init__(self, *args, **kwargs):
        raise NotImplementedError("This class is not meant to be instantiated")

    @classmethod
    def run(cls, *args, **kwargs):
        cls.logger.info("Start")
        repository: Type[Repository] = RepositoryFactory.get_repository(
            "MongoRepository"
        )
        data = cls.__start(repository, *args, **kwargs)
        cls.logger.info(f"Process {len(data)} items")
        data = cls.__process(data, *args, **kwargs)
        cls.logger.info(f"End {len(data)} items")
        cls.__end(data, repository, *args, **kwargs)

    @classmethod
    def __start(cls, repository: Type[Repository], *args, **kwargs):
        data = repository.find_all("events")
        cls.logger.info(f"Data: {len(data)}")

        errors = CrawledBrandEventSchema().validate(data, many=True)
        for error, reason in errors.items():
            cls.logger.error(f"{error}: {reason}")
        cls.logger.info(f"Errors: {len(errors)}")
        res = []
        for idx, d in enumerate(data):
            if idx not in errors:
                res.append(d)
        return res

    @classmethod
    def __process(cls, data, *args, **kwargs):
        df = DataFrame(data)
        df.drop(
            columns=["_id", "created_at", "updated_at"], inplace=True, errors="ignore"
        )

        df["name"] = df["name"].map(lambda x: x.lower())

        # Discard previous event
        cur_timestamp = int(
            datetime.strptime(
                f"{datetime.now().year}-{datetime.now().month}-01", "%Y-%m-%d"
            ).timestamp()
        )
        df = df[df["written_at"].map(lambda x: x >= cur_timestamp)]

        # Replace Image Url
        df["image"] = df["image"].map(
            lambda x: {
                "thumb": os.getenv("CLOUDFRONT_URL") + urlparse(x["thumb"]).path
                if x["thumb"]
                else None,
                "others": [
                    os.getenv("CLOUDFRONT_URL") + urlparse(y).path for y in x["others"]
                ],
            }
        )
        df = cls.__convert(df)
        return df

    @classmethod
    def __convert(cls, df):
        # TODO : Status 처리
        df["status"] = 1

        # 1. brand
        df["brand"] = df["crawled_info"].map(lambda x: x["brand"])

        # 2. crawled_infos
        df.rename(columns={"crawled_info": "crawled_infos"}, inplace=True)
        df["crawled_infos"] = df["crawled_infos"].map(lambda x: [x])

        # 3. image
        df["image"] = df["image"].map(
            lambda x: {"thumb": x["thumb"], "others": x["others"]}
        )

        df = df[
            [
                "status",
                "written_at",
                "name",
                "brand",
                "image",
                "description",
                "crawled_infos",
            ]
        ]
        return df

    @classmethod
    def __end(cls, data: DataFrame, repository: Type[Repository], *args, **kwargs):
        filters = data["crawled_infos"].map(
            lambda x: {
                "$or": [
                    {"crawled_infos.spider": y["spider"], "crawled_infos.id": y["id"]}
                    for y in x
                ]
            }
        )
        data = data.to_dict("records")
        filters = filters.to_list()
        repository.save_all("events", data, filters)

    @classmethod
    def test_start(cls, repository: Type[Repository], *args, **kwargs):
        if os.getenv("STAGE") != "test":
            raise RuntimeError("This method is only for test")
        return cls.__start(repository, *args, **kwargs)
