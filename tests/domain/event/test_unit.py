import pytest

from lib.domain.event.processor import EventProcessor


@pytest.fixture
def env():
    import os
    import dotenv

    while "tests" not in os.listdir():
        os.chdir("..")
    dotenv.load_dotenv()


def test_preprocess(env):
    # given
    processor = EventProcessor()
    # when
    data = processor._preprocess()
    # then
    assert len(data) > 0
