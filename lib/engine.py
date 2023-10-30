import logging
import os
from datetime import datetime
from typing import Any, Dict, Literal, Mapping, Sequence

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
        self.logger = logging.getLogger("batch.engine")
        self.__configure_logger()

    def run(self) -> bool:
        """
        0. tmp 파일 지우기
        1. Processor 돌리기
        2. S3 Upload
        3. Event 보내기
        4. Slack 보내기
        5. 종료
        """
        s3_eraser = S3Eraser()
        s3_eraser.erase()

        s3_sender = S3Sender()
        slack_sender = SlackSender()
        db_sender = DBSender()
        event_sender = EventSender()

        processors: Dict[Literal["events", "brands", "products"], ProcessorIfs] = {}
        self.logger.info("Processing start")
        for _type in ["events", "products"]:
            processors[_type] = ProcessorFactory.get_instance(_type=_type)

        results: Dict[
            Literal["events", "brands", "products"],
            Mapping[Literal["data", "updated"], Sequence[Mapping[str, Any]]],
        ] = {}
        for _type, processor in processors.items():
            try:
                data = processor.run(date=self.__date)
                results[_type] = data
            except Exception as e:
                # TODO : Send to slack
                self.logger.error(f"{_type} processor: {e}")
                return False

        self.logger.info("Send to s3 tmp folder")
        s3_results: Dict[Literal["events", "brands", "products"], Sequence[str]] = {}
        for _type, data in results.items():
            try:
                s3_results[_type] = s3_sender.send(_type, data)
            except Exception as e:
                # TODO : Send to slack
                self.logger.error(f"{_type} s3 sender: {e}")
                return False

        self.logger.info("Send to db messages")
        for _type, data in s3_results.items():
            try:
                db_sender.send(
                    date=self.__date,
                    rel_name=_type,
                    db_name=os.getenv("MONGO_SERVICE_DB"),
                    data=data,
                )
            except Exception as e:
                self.logger.error(f"{_type} db sender: {e}")
                return False

        # Finish Event Send
        self.logger.info("Send finish event")
        try:
            res = event_sender.send(event_type="finished", date=self.__date)
            if not res:
                raise RuntimeError("Failed to send event")
        except Exception as e:
            self.logger.error(f"{_type} event sender: {e}")
            return False
        self.logger.info("Done")
        return True

    def __configure_logger(self):
        match self.__stage:
            case "prod":
                self.logger.setLevel(logging.INFO)
            case _:
                self.logger.setLevel(logging.DEBUG)
