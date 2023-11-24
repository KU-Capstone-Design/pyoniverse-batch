import logging

import boto3


class S3Eraser:
    def __init__(self, bucket: str, key: str):
        self.logger = logging.getLogger(__name__)
        self.__bucket = bucket
        self.__key = key
        if not bucket or not key:
            self.logger.error(f"bucket: {bucket}, key: {key} shouldn't be none")
            raise RuntimeError(f"bucket: {bucket}, key: {key} shouldn't be none")

    def erase(self):
        s3 = boto3.resource("s3")
        try:
            bucket = s3.Bucket(self.__bucket)
            bucket.objects.filter(Prefix=self.__key).delete()
        except Exception as e:
            self.logger.error(f"Fail to erase {self.__bucket}/{self.__key} in S3")
            raise RuntimeError(f"Fail to erase {self.__bucket}/{self.__key} in S3")
