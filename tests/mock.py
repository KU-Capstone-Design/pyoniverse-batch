import pytest

from lib.dependency_injector.injector import MainInjector


@pytest.fixture
def env():
    import os
    import dotenv

    while "tests" not in os.listdir():
        os.chdir("..")
    dotenv.load_dotenv()
    os.environ["MONGO_SERVICE_DB"] = "test"


@pytest.fixture
def injector(env):
    injector = MainInjector()
    injector.inject()
