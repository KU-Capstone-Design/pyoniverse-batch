import os

from pymongo import MongoClient

from tests.mock import env


def test_mongo_repository(env):
    from lib.db.mongo_repository import MongoRepository

    # given
    db_name = "constant"
    rel_name = "brands"
    client = MongoClient(os.getenv("MONGO_URI"))
    repository = MongoRepository(client=client, db_name=db_name)
    # when
    cu_data = repository.find_one(
        rel_name=rel_name,
        filter={"slug": "cu"},
        project={"slug": True},
        hint={"slug": 1},
    )
    # then
    assert cu_data is not None
