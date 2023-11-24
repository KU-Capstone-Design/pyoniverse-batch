# def test_s3_sender(env):
#     # given
#     sender = S3Sender()
#     result = {"data": [{"key": "val"} for _ in range(1000)], "updated": [{"test": "v"}]}
#     # when
#     try:
#         res = sender.send(rel_name="test", result=result)
#     except Exception:
#         assert False
#     # then
#     assert True
