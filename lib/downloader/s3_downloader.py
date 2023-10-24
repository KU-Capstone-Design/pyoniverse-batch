import os
from io import BytesIO
from itertools import chain
from typing import Iterator

import boto3
from boto3_type_annotations.s3 import ServiceResource
from bson import decode_iter


class S3Downloader:
    def __init__(self):
        self.__s3: ServiceResource = boto3.resource("s3")

    def download(self, db_name: str, rel_name: str, date: str) -> Iterator[dict]:
        key = f"{date}/{db_name}/{rel_name}"
        data = []
        bucket = os.getenv("S3_BACKUP_BUCKET")
        if not bucket:
            raise KeyError(f"{bucket} doesn't exist")
        for obj in self.__s3.Bucket(bucket).objects.filter(Prefix=key):
            if not obj.key.endswith(".bson"):
                continue
            with BytesIO() as bytes_io:
                obj = self.__s3.Object(bucket, obj.key)
                obj.download_fileobj(bytes_io)
                bytes_io.seek(0)
                data.append(decode_iter(bytes_io.read()))
        return chain.from_iterable(data)
