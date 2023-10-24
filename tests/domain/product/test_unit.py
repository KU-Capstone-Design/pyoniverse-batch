from datetime import datetime

import pytest

from lib.domain.product.processor import ProductProcessor


@pytest.fixture
def env():
    import os
    import dotenv

    while "tests" not in os.listdir():
        os.chdir("..")
    dotenv.load_dotenv()


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
