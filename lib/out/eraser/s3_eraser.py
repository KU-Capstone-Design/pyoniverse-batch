import os

import boto3


class S3Eraser:
    def __init__(self):
        self.__bucket = os.getenv("S3_BUCKET")
        self.__key = os.getenv("S3_KEY")

    def erase(self):
        s3 = boto3.resource("s3")
        try:
            bucket = s3.Bucket(self.__bucket)
            bucket.objects.filter(Prefix=self.__key).delete()
        except Exception as e:
            raise e
