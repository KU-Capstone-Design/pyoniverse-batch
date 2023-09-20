import json
import logging
import os
from typing import Type

from pandas import DataFrame

from lib.db import Repository, RepositoryFactory
from lib.product.schema.crawled_product_schema import CrawledProductSchema


class ProductProcessor:
    logger = logging.getLogger("ProductProcessor")

    def __init__(self, *args, **kwargs):
        raise NotImplementedError("This class is not meant to be instantiated")

    @classmethod
    def run(cls, *args, **kwargs):
        cls.logger.info("Start")
        repository = RepositoryFactory.get_repository("MongoRepository")
        data = cls.__start(repository, *args, **kwargs)
        cls.logger.info(f"Process {len(data)} items")
        data = cls.__process(data, *args, **kwargs)

        cls.logger.info(f"End {len(data)} items")
        cls.__end(data, repository, *args, **kwargs)

    @classmethod
    def __start(cls, repository: Type[Repository], *args, **kwargs):
        hint = json.loads(os.getenv("MONGO_READ_HINT"))
        hint = [(k, v) for k, v in hint.items()]
        data = repository.find_all("products", hint=hint)

        errors = CrawledProductSchema().validate(data=data, many=True)
        for error, reason in errors.items():
            cls.logger.error(f"Error: {error} - {reason}")
            del data[error]
        return data

    @classmethod
    def __process(cls, data, *args, **kwargs):
        # 1. 데이터 정제
        # 2. 이름 기준으로 병합
        # 3. 상용 데이터로 변환
        df = DataFrame(data)

        # 1. 데이터 정제
        df = cls.__normalize(df)

        # 2. 이름 기준으로 병합
        df = cls.__merge(df)

        # 3. 상용 데이터로 변환
        df = cls.__convert(df)

        return df.to_dict(orient="records")

    @classmethod
    def __end(cls, data, repository: Type[Repository], *args, **kwargs):
        """
        저장
        """
        repository.save_all("products", data, filters=[{"id": d["id"]} for d in data])
