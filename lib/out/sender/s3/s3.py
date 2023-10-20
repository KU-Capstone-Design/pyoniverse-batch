import json
import logging
import os
from typing import Any, List, Literal, Mapping, Sequence

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
    ) -> List[str]:
        """
        Data를 100개씩 쪼개 {rel_name}_{idx}.json으로 전송
        """
        data = result["data"]
        updated = result["updated"]
        result = []
        try:
            s3: Client = boto3.client("s3")
            for idx, p in enumerate(range(0, len(data), 100)):
                buffer = data[p : p + 100]
                body: bytes = json.dumps(buffer, ensure_ascii=False).encode()
                key = f"{os.getenv('S3_KEY')}/{rel_name}_{idx}.json"
                s3.put_object(
                    Bucket=os.getenv("S3_BUCKET"),
                    Key=key,
                    Body=body,
                )
                result.append(key)

            key = f"{os.getenv('S3_KEY')}/{rel_name}_updated.json"
            s3.put_object(
                Bucket=os.getenv("S3_BUCKET"),
                Key=key,
                Body=json.dumps(updated, ensure_ascii=False).encode(),
            )
            result.append(key)
        except Exception as e:
            raise e
        else:
            return result
