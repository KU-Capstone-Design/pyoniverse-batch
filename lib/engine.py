import logging
import os
from datetime import datetime
from typing import Any, Dict, Literal, Mapping, NoReturn, Sequence

from lib.domain.factory import ProcessorFactory
from lib.interface.processor_ifs import ProcessorIfs
from lib.out.eraser.s3_eraser import S3Eraser
from lib.out.sender.db.db import DBSender
from lib.out.sender.event.event import EventSender
from lib.out.sender.s3.s3 import S3Sender
from lib.out.sender.slack.slack import SlackSender


class Engine:
    """
    Main Module
    """

    def __init__(self, stage: Literal["dev", "test", "prod"], date: str):
        if stage not in {"dev", "test", "prod"}:
            raise AttributeError(f"{stage} not in [dev, test, prod]")
        self.__stage = stage
        try:
            self.__date = datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"{date} should be like 2022-11-11")

        # Logger
        self.logger = logging.getLogger(__name__)

    def run(self) -> NoReturn:
        """
        0. tmp 파일 지우기
        1. Processor 돌리기
        2. S3 Upload
        3. Event 보내기
        4. Slack 보내기
        5. 종료
        """
        tmp_bucket = os.getenv("S3_BUCKET")
        tmp_key = os.getenv("S3_KEY")
        self.logger.info(f"Erase {tmp_bucket}/{tmp_key}")
        s3_eraser = S3Eraser(bucket=tmp_bucket, key=tmp_key)
        s3_eraser.erase()

        s3_sender = S3Sender()
        slack_sender = SlackSender()
        db_sender = DBSender()
        event_sender = EventSender()

        processors: Dict[Literal["events", "products"], ProcessorIfs] = {
            "events": ProcessorFactory.get_instance(_type="events"),
            "products": ProcessorFactory.get_instance(_type="products"),
        }

        results: Dict[
            Literal["events", "products"],
            Mapping[Literal["data", "updated"], Sequence[Mapping[str, Any]]],
        ] = {}
        self.logger.info(f"Start processors: {list(processors.keys())}")
        for _type, processor in processors.items():
            data = processor.run(date=self.__date)
            results[_type] = data

        self.logger.info(f"Send results to s3://{tmp_bucket}/{tmp_key}")
        s3_results: Dict[Literal["events", "products"], Sequence[str]] = {}
        for _type, data in results.items():
            s3_results[_type] = s3_sender.send(_type, data)

        if self.__stage != "test":
            self.logger.info("Send db update messages")
            for _type, data in s3_results.items():
                db_sender.send(
                    date=self.__date,
                    rel_name=_type,
                    db_name=os.getenv("MONGO_SERVICE_DB"),
                    data=data,
                )
        else:
            self.logger.info("Don't send db update messages when TEST mode")

        if self.__stage != "test":
            self.logger.info("Broadcast the finished event")
            res = event_sender.send(event_type="finished", date=self.__date)
            if not res:
                raise RuntimeError("Failed to send event")
        else:
            self.logger.info("Don't send db update messages when TEST mode")
