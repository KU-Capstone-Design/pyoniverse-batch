import logging
import os
from abc import ABCMeta, abstractmethod
from typing import List, Type

from overrides import override
from pymongo import MongoClient, ReadPreference, UpdateOne, WriteConcern


class Repository(metaclass=ABCMeta):
    logger = logging.getLogger("Repository")

    def __init__(self, *args, **kwargs):
        raise NotImplementedError("This class is not for instantiation")

    @classmethod
    @abstractmethod
    def find_all(cls, *args, **kwargs) -> List[dict]:
        """
        :param args: args[0] = collection or table name
        :param kwargs:
        :return:
        """
        pass

    @classmethod
    @abstractmethod
    def save_all(cls, name: str, /, data: List[dict], filters: List[dict], **kwargs):
        """
        :param data: List[dict]
        :param filters: List[dict]
        -> len(data) == len(filters)
        :param kwargs:
        :return:
        """
        pass


class MongoRepository(Repository):
    __client = MongoClient(os.getenv("MONGO_URI"))

    @classmethod
    def reader(cls):
        return cls.__client.get_database(
            os.getenv("MONGO_READ_DB"),
            read_preference=ReadPreference.SECONDARY_PREFERRED,
        )

    @classmethod
    def writer(cls):
        return cls.__client.get_database(
            os.getenv("MONGO_WRITE_DB"), write_concern=WriteConcern(w="majority")
        )

    @classmethod
    @override
    def find_all(cls, *args, **kwargs) -> List[dict]:
        """
        collection: str
        hint: [(id, 1)] 형식
        filter: dict
        projection: dict
        """
        collection = args[0]
        hint = kwargs.get("hint", None)
        filter = kwargs.get("filter", {})
        projection = kwargs.get("projection", {})

        if projection:
            projection["_id"] = True

        cls.logger.info(
            f"collection: {collection}\nfilter: {filter}\nprojection: {projection}\nhint: {hint}"
        )

        res = []
        buffer = list(
            cls.reader()
            .get_collection(collection)
            .find(filter, projection, hint=hint)
            .sort("_id", 1)
            .limit(1000)
        )
        while buffer:
            res += buffer
            cursor = res[-1]["_id"]
            buffer = list(
                cls.reader()
                .get_collection(collection)
                .find({"_id": {"$gt": cursor}}, projection, hint=hint)
                .sort("_id", 1)
                .limit(1000)
            )
        return res

    @classmethod
    @override
    def save_all(cls, name, /, data: List[dict], filters: List[dict], **kwargs):
        """
        :param name: collection or table name
        :param data: List[dict]
        :param filters: List[dict]
        :param kwargs:
        hint: [(id, 1)] 형식
        :return:
        """
        # TODO : ID AUTO INCREMENT - Atlas 에서 지원하는 방식으로 처리하기
        buffer = []
        for d, f in zip(data, filters):
            buffer.append(
                UpdateOne(f, {"$set": d}, upsert=True, hint=kwargs.get("hint", None))
            )

        matched_count, modified_count, inserted_count = 0, 0, 0
        for idx in range(0, len(buffer), 1000):
            res = cls.writer().get_collection(name).bulk_write(buffer[idx : idx + 1000])
            matched_count += res.matched_count
            modified_count += res.modified_count
            inserted_count += res.upserted_count
        cls.logger.info(
            f"matched: {matched_count}, modified: {modified_count}, inserted: {inserted_count}"
        )


class RepositoryFactory:
    def __init__(self, *args, **kwargs):
        raise NotImplementedError("This class is not for instantiation")

    @staticmethod
    def get_repository(name: str) -> Type[Repository]:
        match name:
            case "MongoRepository":
                return MongoRepository
            case _:
                raise NotImplementedError(f"{name} is not supported")
