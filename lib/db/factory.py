import logging
import os
from typing import Literal

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

from lib.db.mongo_repository import MongoRepository
from lib.interface.repository_ifs import RepositoryIfs


class RepositoryFactory:
    logger = logging.getLogger(__name__)
    __clients = {}

    @classmethod
    def get_instance(cls, _type: Literal["mongo"], db_name: str) -> RepositoryIfs:
        match _type:
            case "mongo":
                return MongoRepository(client=cls.__get_client(_type), db_name=db_name)
            case _:
                raise NotImplementedError

    @classmethod
    def __get_client(cls, _type: Literal["mongo"]):
        if _type not in cls.__clients:
            cls.logger.info(f"Connecting {_type}...")
            match _type:
                case "mongo":
                    try:
                        client: MongoClient = MongoClient(os.getenv("MONGO_URI"))
                        client.admin.command("ping")
                    except ConnectionFailure:
                        raise RuntimeError(
                            f"Can't connect to mongo Server: {os.getenv('MONGO_URI')}"
                        )
                    else:
                        cls.logger.info(f"Connect to {_type} Client")
                        cls.__clients[_type] = client
                case _:
                    raise NotImplementedError
        return cls.__clients[_type]
