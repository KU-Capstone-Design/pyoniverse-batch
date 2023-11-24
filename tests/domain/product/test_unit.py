from datetime import datetime

import pytest
from pandas import DataFrame, Series

from lib.domain.product.processor import ProductProcessor
from tests.mock import env


@pytest.fixture
def date():
    return datetime.strptime("2023-10-23", "%Y-%m-%d")


def test_preprocess(env, date):
    # given
    processor = ProductProcessor()
    # when
    data = processor._preprocess(date=date)
    # then
    assert len(data) > 0


def test_process(env, date):
    # given
    processor = ProductProcessor()
    # when
    data = processor._preprocess(date=date)
    data = processor._process(data, date=date)
    # then
    assert len(data) > 0


def test_postprocess(env, date):
    # given
    processor = ProductProcessor()
    # when
    data = processor._preprocess(date=date)
    data = processor._process(data, date=date)
    data = processor._postprocess(data, date=date)
    # then
    assert len(data) > 0


def test_erase_default_image(env, date):
    # given
    default_images = {"products/be780835fd93417525fe2304be3b8c917902348d.webp"}
    processor = ProductProcessor()
    data = processor._preprocess(date=date)
    data = processor._process(data, date=date)
    data = processor._postprocess(data, date=date)

    # when
    images = DataFrame(data)["image"]
    # then
    assert images.map(lambda x: "/".join(x.split("/")[-2:]) not in default_images).all(bool_only=True)
