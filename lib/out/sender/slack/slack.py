import json
import os
from dataclasses import asdict
from typing import Any, Dict, NoReturn

import boto3
from boto3_type_annotations.sqs import Client

from lib.error import BaseError
from lib.out.sender.slack.model.enum.message_enum import MessageTypeEnum
from lib.out.sender.slack.model.message import Message


class SlackSender:
    def send(
        self,
        message_type: MessageTypeEnum,
        data: Dict[str, Any] = None,
        error: BaseError = None,
    ) -> NoReturn:
        res: Message = self.__convert(message_type, data, error)
        try:
            sqs_client: Client = boto3.client("sqs")
            sqs_queue_url: str = sqs_client.get_queue_url(
                QueueName=os.getenv("QUEUE_NAME")
            )["QueueUrl"]
            sqs_client.send_message(
                QueueUrl=sqs_queue_url,
                MessageBody=json.dumps(asdict(res)),
            )
        except Exception as e:
            raise e

    def __convert(
        self,
        message_type: MessageTypeEnum,
        data: Dict[str, Any] = None,
        error: BaseError = None,
    ) -> Message:
        msg: Dict[str, Any] = {
            "type": message_type.value,
            "source": "pyoniverse-etl-transform",
            "cc": ["윤영로"],
        }
        match message_type:
            case MessageTypeEnum.SUCCESS:
                msg["text"] = "Success"
                msg["ps"] = data
            case MessageTypeEnum.ERROR:
                msg["text"] = error.message
                msg["ps"] = {"reason": error.reason}
            case MessageTypeEnum.DEBUG:
                msg["text"] = "Debug"
                msg["ps"] = {
                    "data": str(data),
                    "error": str({"reason": error.reason, "message": error.message}),
                }
            case MessageTypeEnum.TEST:
                msg["text"] = "Test"
                msg["ps"] = {
                    "data": str(data),
                    "error": str({"reason": error.reason, "message": error.message}),
                }
            case _:
                raise NotImplementedError
        return Message.load(msg)
