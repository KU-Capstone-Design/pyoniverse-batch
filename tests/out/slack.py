import pytest

from lib.error import BaseError
from lib.out.sender.slack.model.enum import MessageTypeEnum
from lib.out.sender.slack import SlackSender


@pytest.fixture
def env():
    import os
    import dotenv

    while "tests" not in os.listdir():
        os.chdir("..")
    dotenv.load_dotenv()


def test_slack(env):
    # given
    sender = SlackSender()
    messages = {
        MessageTypeEnum.SUCCESS: {
            "data": {"hello": "world"},
            "error": None,
        },
        MessageTypeEnum.ERROR: {
            "data": None,
            "error": BaseError(message="Test Error", reason="Test Reason"),
        },
        MessageTypeEnum.DEBUG: {
            "data": {"hello": "world"},
            "error": BaseError(message="Test Error", reason="Test Reason"),
        },
        MessageTypeEnum.TEST: {
            "data": {"hello": "world"},
            "error": BaseError(message="Test Error", reason="Test Reason"),
        },
    }
    # when & then
    for _type, body in messages.items():
        try:
            sender.send(_type, data=body["data"], error=body["error"])
        except Exception:
            assert False
    assert True
