import json
import logging
import os
from typing import Any, Literal, Mapping, NoReturn, Sequence

import boto3
from boto3_type_annotations.s3 import Client


class S3Sender:
    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger("scrapy.sender")

    def send(
        self,
        rel_name: str,
        result: Mapping[Literal["data", "updated"], Sequence[Mapping[str, Any]]],
        *args,
        **kwargs,
    ) -> NoReturn:
        """
        Data를 100개씩 쪼개 {rel_name}_{idx}.json으로 전송
        """
        data = result["data"]
        updated = result["updated"]
        try:
            s3: Client = boto3.client("s3")
            for idx, p in enumerate(range(0, len(data), 100)):
                buffer = data[p : p + 100]
                body: bytes = json.dumps(buffer, ensure_ascii=False).encode()
                s3.put_object(
                    Bucket=os.getenv("S3_BUCKET"),
                    Key=f"{os.getenv('S3_KEY')}/{rel_name}_{idx}.json",
                    Body=body,
                )
            s3.put_object(
                Bucket=os.getenv("S3_BUCKET"),
                Key=f"{os.getenv('S3_KEY')}/{rel_name}_updated.json",
                Body=json.dumps(updated, ensure_ascii=False).encode(),
            )
        except Exception as e:
            raise e
