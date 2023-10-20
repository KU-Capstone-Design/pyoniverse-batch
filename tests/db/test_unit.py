import os

import pytest
from pymongo import MongoClient


@pytest.fixture
def env():
    import os
    import dotenv

    while "tests" not in os.listdir():
        os.chdir("..")
    dotenv.load_dotenv()


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
