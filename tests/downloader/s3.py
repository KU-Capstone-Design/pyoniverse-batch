import os

import pytest

from lib.domain.product.model.service_product_schema import ServiceProductSchema
from lib.downloader.s3_downloader import S3Downloader


@pytest.fixture
def env():
    import os
    import dotenv

    while "tests" not in os.listdir():
        os.chdir("..")
    dotenv.load_dotenv()


def test_s3_downloader(env):
    # given
    downloader = S3Downloader()
    # when
    data = list(
        downloader.download(
            db_name=os.getenv("MONGO_SERVICE_DB"),
            rel_name="products",
            date="2023-10-23",
        )
    )
    errors = ServiceProductSchema().validate(data, many=True)
    # then
    assert len(data) > 0
    assert not errors
