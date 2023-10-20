import pytest

from lib.engine import Engine


@pytest.fixture
def env():
    import os
    import dotenv

    while "tests" not in os.listdir():
        os.chdir("..")
    dotenv.load_dotenv()


def test_engine(env):
    # given
    engine = Engine(stage="test", date="2023-10-20")
    # when
    res = engine.run()
    # then
    assert res is True
