import pytest


@pytest.fixture
def env():
    import os
    import dotenv

    while "tests" not in os.listdir():
        os.chdir("..")
    dotenv.load_dotenv()


def test_engine(env):
    from lib.engine import Engine

    # given
    engine = Engine(stage="test", date="2023-10-20")
    # when
    try:
        engine.run()
        assert True
    # then
    except Exception:
        assert False
