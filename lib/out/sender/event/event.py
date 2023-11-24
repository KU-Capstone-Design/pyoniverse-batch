import json
import logging
import os
from datetime import datetime
from typing import Literal, NoReturn

import boto3
from boto3_type_annotations.events import Client


class EventSender:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.__source = os.getenv("EVENT_SOURCE")
        self.__detail_type = os.getenv("EVENT_DETAIL_TYPE")

        if not self.__source or not self.__detail_type:
            self.logger.error(
                f"source: {self.__source}, detail type: {self.__detail_type} shouldn't be none"
            )
            raise RuntimeError(
                f"source: {self.__source}, detail type: {self.__detail_type} shouldn't be none"
            )

    def send(self, event_type: Literal["finished"], date: datetime) -> NoReturn:
        if event_type != "finished":
            self.logger.error(f"{event_type} should be in ['finished']")
            raise RuntimeError(f"{event_type} should be in ['finished']")
        client: Client = boto3.client("events")
        response = client.put_events(
            Entries=[
                {
                    "Source": self.__source,
                    "DetailType": self.__detail_type,
                    "Detail": json.dumps(
                        {"status": "finished", "date": date.strftime("%Y-%m-%d")},
                        ensure_ascii=False,
                    ),
                    "EventBusName": os.getenv("EVENT_BUS_NAME"),
                },
            ],
        )
        if response["FailedEntryCount"] > 0:
            self.logger.error(f"Fail to send message: {response['FailedEntryCount']}")
            raise RuntimeError(f"Fail to send message: {response['FailedEntryCount']}")
