from typing import Literal

from lib.db.mongo_repository import MongoRepository
from lib.interface.repository_ifs import RepositoryIfs


class RepositoryFactory:
    @classmethod
    def get_instance(cls, _type: Literal["mongo"], db_name: str) -> RepositoryIfs:
        match _type:
            case "mongo":
                return MongoRepository(db_name=db_name)
            case _:
                raise NotImplementedError
