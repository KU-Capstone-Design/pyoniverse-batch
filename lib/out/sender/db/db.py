import json
import os
from dataclasses import asdict
from datetime import datetime
from time import sleep
from typing import NoReturn, Sequence

import boto3
from boto3_type_annotations.sqs import Client

from lib.out.sender.db.model.message import Message


class DBSender:
    def send(
        self,
        date: datetime,
        db_name: str,
        rel_name: str,
        data: Sequence[str],
    ) -> NoReturn:
        try:
            sqs_client: Client = boto3.client("sqs")
            sqs_queue_url: str = sqs_client.get_queue_url(
                QueueName=os.getenv("DB_QUEUE_NAME")
            )["QueueUrl"]
            for p in range(0, len(data), 10):
                # 나눠서 보내기
                message = Message(
                    date=datetime.strftime(date, "%Y-%m-%d"),
                    db_name=db_name,
                    rel_name=rel_name,
                    origin="transform",
                    data=data[p : p + 10],
                )
                sqs_client.send_message(
                    QueueUrl=sqs_queue_url,
                    MessageBody=json.dumps(asdict(message)),
                )
                sleep(10)
        except Exception as e:
            raise e
