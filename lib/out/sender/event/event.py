import json
import os
from datetime import datetime
from typing import Literal

import boto3
from boto3_type_annotations.events import Client


class EventSender:
    def send(self, event_type: Literal["finished"], date: datetime):
        if event_type != "finished":
            raise RuntimeError(f"{event_type} should be in ['finished']")
        client: Client = boto3.client("events")
        response = client.put_events(
            Entries=[
                {
                    "Source": os.getenv("EVENT_SOURCE"),
                    "DetailType": os.getenv("EVENT_DETAIL_TYPE"),
                    "Detail": json.dumps(
                        {"status": "finished", "date": date.strftime("%Y-%m-%d")},
                        ensure_ascii=False,
                    ),
                    "EventBusName": os.getenv("EVENT_BUS_NAME"),
                },
            ],
        )
        return response["FailedEntryCount"] == 0
