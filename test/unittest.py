import json
import os

import dotenv
import pytest
from pandas import DataFrame


if "main.py" not in os.listdir():
    os.chdir("..")


@pytest.fixture
def env():
    dotenv.load_dotenv()
    os.environ["STAGE"] = "test"


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
    return logger


@pytest.fixture
def data():
    with open("test/test_product_data.json", "r") as f:
        data = json.load(f)
    return data


@pytest.fixture
def data_post_merged():
    with open("test/post_merged_product.json", "r") as f:
        data = json.load(f)
    return data


@pytest.fixture
def data_converted():
    with open("test/data_converted.json", "r") as f:
        data = json.load(f)
    return data


@pytest.fixture
def images():
    return [
        {
            "thumb": "s3://pyoniverse-image/products/00048b1d6712f5f3361376c384c53399fb892738.webp",
            "others": [],
        },
        {
            "thumb": "s3://pyoniverse-image/products/0023d2a56b6811336d2191a1893623a514c2c144.webp",
            "others": [],
        },
    ]


def test_parse_hint(env):
    read_hint = os.getenv("MONGO_READ_HINT")
    write_hint = os.getenv("MONGO_WRITE_HINT")
    assert json.loads(read_hint) == {"_id": 1}
    assert json.loads(write_hint) == {"crawled_infos.spider": 1, "crawled_infos.id": 1}


def test_schema_validation(hint, mongo_repository, logger):
    data = mongo_repository.find_all("products", hint=hint[0])
    from lib.product.schema.crawled_product_schema import CrawledProductSchema

    errors = CrawledProductSchema().validate(data=data, many=True)
    assert len(errors) == 0


def test_product_processor_start(env, mongo_repository, logger):
    from lib.product.product_processor import ProductProcessor

    data = ProductProcessor.test_start(mongo_repository)
    logger.info(f"Data: {len(data)}")
    # df = DataFrame(data)
    # df.drop(columns=["_id"], inplace=True)
    # data = df.to_dict(orient="records")
    # with open("test_product_data.json", "w") as f:
    #     json.dump(data, f, ensure_ascii=False, indent=4)
    assert len(data) > 0


def test_product_processor_normalize(env, data, logger):
    from lib.product.product_processor import ProductProcessor

    df = DataFrame(data)
    df = ProductProcessor.test_normalize(df)
    logger.info(df.head())
    with open("test_product_data_normalized.json", "w") as f:
        json.dump(df.to_dict(orient="records"), f, ensure_ascii=False, indent=4)
    assert df["name"].isna().sum() == 0


def test_product_processor_merge(env, data, logger):
    from lib.product.product_processor import ProductProcessor

    df = DataFrame(data)
    df = ProductProcessor.test_normalize(df)
    df = ProductProcessor.test_merge(df)
    logger.info(df.head())
    assert len(df) > 0


def test_product_processor_get_largest_img(env, images, logger):
    from lib.product.product_processor import ProductProcessor

    res = ProductProcessor.test_get_largest_image(images)

    assert (
        res
        == "s3://pyoniverse-image/products/00048b1d6712f5f3361376c384c53399fb892738.webp"
    )


def test_product_processor_get_category(env, data, logger):
    from lib.product.product_processor import ProductProcessor

    df = DataFrame(data)
    df = ProductProcessor.test_normalize(df)
    df = ProductProcessor.test_merge(df)
    res = ProductProcessor.test_get_category(df.iloc[0])
    assert res is not None


def test_product_processor_collect_by_brand(env, data, logger):
    from lib.product.product_processor import ProductProcessor

    df = DataFrame(data)
    df = ProductProcessor.test_normalize(df)
    df = ProductProcessor.test_merge(df)
    res = ProductProcessor.test_collect_by_brand(df.iloc[0])
    assert res is not None


def test_product_processor_post_merge(env, mongo_repository, logger):
    from lib.product.product_processor import ProductProcessor

    data = ProductProcessor.test_start(mongo_repository)
    df = DataFrame(data)
    df.drop(columns=["_id", "created_at", "updated_at"], inplace=True, errors="ignore")
    df = ProductProcessor.test_normalize(df)
    df = ProductProcessor.test_merge(df)
    df = ProductProcessor.test_post_merge(df)
    logger.info(df.head())
    with open("post_merged_product.json", "w") as fd:
        json.dump(df.to_dict(orient="records"), fd, ensure_ascii=False, indent=4)
    assert len(df) > 0


def test_product_processor_convert(env, data_post_merged, logger):
    from lib.product.product_processor import ProductProcessor

    df = DataFrame(data_post_merged)
    df = ProductProcessor.test_convert(df)
    with open("data_converted.json", "w") as fd:
        json.dump(df.to_dict(orient="records"), fd, ensure_ascii=False, indent=4)
    assert len(df) > 0


def test_product_processor_end(env, data_converted, mongo_repository, logger):
    from lib.product.product_processor import ProductProcessor

    df = DataFrame(data_converted)
    df = ProductProcessor.test_end(df, mongo_repository)
