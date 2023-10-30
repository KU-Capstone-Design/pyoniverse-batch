from datetime import datetime

import pytest

from lib.out.sender.event.event import EventSender


@pytest.fixture
def env():
    import os
    import dotenv

    while "tests" not in os.listdir():
        os.chdir("..")
    dotenv.load_dotenv()


def test_send_finish_event(env):
    # given
    sender = EventSender()
    # when & then
    try:
        assert sender.send(event_type="finished", date=datetime.utcnow()) is True
    except Exception:
        assert False
