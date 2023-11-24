import json
import logging
import os
from typing import Any, List, Literal, Mapping, Sequence

import boto3
from boto3_type_annotations.s3 import Client


class S3Sender:
    def __init__(self, *args, **kwargs):
        self.logger = logging.getLogger(__name__)
        self.__bucket = os.getenv("S3_BUCKET")
        self.__key = os.getenv("S3_KEY")

        if not self.__bucket or not self.__key:
            self.logger.error(
                f"bucket: {self.__bucket}, key: {self.__key} shouldn't be none"
            )
            raise RuntimeError(
                f"bucket: {self.__bucket}, key: {self.__key} shouldn't be none"
            )

    def send(
        self,
        rel_name: str,
        result: Mapping[Literal["data"], Sequence[Mapping[str, Any]]],
        *args,
        **kwargs,
    ) -> List[str]:
        """
        Data를 100개씩 쪼개 {rel_name}_{idx}.json으로 전송
        """
        data = result["data"]
        result = []
        try:
            s3: Client = boto3.client("s3")
            for idx, p in enumerate(range(0, len(data), 100)):
                buffer = data[p : p + 100]
                body: bytes = json.dumps(buffer, ensure_ascii=False).encode()
                key = f"{self.__key}/{rel_name}_{idx}.json"
                s3.put_object(
                    Bucket=self.__bucket,
                    Key=key,
                    Body=body,
                )
                result.append(key)
        except Exception as e:
            self.logger.error("Fail to upload result to s3")
            raise RuntimeError("Fail to upload result to s3")
        else:
            return result
