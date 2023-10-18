import pytest

from lib.out.s3.s3 import S3Sender


@pytest.fixture
def env():
    import os
    import dotenv

    while "tests" not in os.listdir():
        os.chdir("..")
    dotenv.load_dotenv()


def test_s3_sender(env):
    # given
    sender = S3Sender()
    data = [{"key": "val"} for _ in range(1000)]
    # when
    res = sender.send(rel_name="test", data=data)
    # then
    assert res is True
