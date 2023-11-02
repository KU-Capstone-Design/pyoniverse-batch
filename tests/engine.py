from tests.mock import env, injector


def test_engine(env, injector):
    from lib.engine import Engine

    # given
    engine = Engine(stage="test", date="2023-10-20", domain="all")
    # when
    try:
        engine.run()
        assert True
    # then
    except Exception:
        assert False
