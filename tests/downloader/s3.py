import os
from datetime import datetime

from lib.domain.product.model.service_product_schema import ServiceProductSchema
from lib.downloader.s3_downloader import S3Downloader
from tests.mock import env


def test_s3_downloader(env):
    # given
    downloader = S3Downloader()
    # when
    data = list(
        downloader.download(
            db_name=os.getenv("MONGO_SERVICE_DB"),
            rel_name="products",
            date=datetime.strptime("2023-11-01", "%Y-%m-%d"),
        )
    )
    errors = ServiceProductSchema().validate(data, many=True)
    # then
    assert len(data) > 0
    assert not errors
