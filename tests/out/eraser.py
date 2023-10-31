import pytest


@pytest.fixture
def env():
    import os
    import dotenv

    while "tests" not in os.listdir():
        os.chdir("..")
    dotenv.load_dotenv()


# def test_s3eraser(env):
#     from lib.out.eraser.s3_eraser import S3Eraser
#
#     # given
#     eraser = S3Eraser()
#     # when
#     try:
#         eraser.erase()
#     except Exception:
#         assert False
#     # then
#     assert True
