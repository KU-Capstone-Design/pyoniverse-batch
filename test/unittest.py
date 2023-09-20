import json
import os

import dotenv
import pytest


@pytest.fixture
def env():
    dotenv.load_dotenv()


@pytest.fixture
def mongo_repository(env):
    from lib.db import RepositoryFactory
    return RepositoryFactory.get_repository("MongoRepository")


@pytest.fixture
def hint(env):
    read_hint = os.getenv("MONGO_READ_HINT")
    write_hint = os.getenv("MONGO_WRITE_HINT")
    read_hint = [(k, v) for k, v in json.loads(read_hint).items()]
    write_hint = [(k, v) for k, v in json.loads(write_hint).items()]
    return read_hint, write_hint


@pytest.fixture
def logger():
    import logging
    logger = logging.getLogger("test")
    logger.setLevel(logging.ERROR)
    return logger


def test_parse_hint(env):
    read_hint = os.getenv("MONGO_READ_HINT")
    write_hint = os.getenv("MONGO_WRITE_HINT")
    assert json.loads(read_hint) == {"_id": 1}
    assert json.loads(write_hint) == {"id": 1}


def test_schema_validation(hint, mongo_repository, logger):
    data = mongo_repository.find_all("products", hint=hint[0])
    from lib.product.schema.crawled_product_schema import CrawledProductSchema
    errors = CrawledProductSchema().validate(data=data, many=True)
    assert len(errors) == 0
