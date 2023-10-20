import logging
from datetime import datetime
from typing import Any, Dict, Literal, Mapping, Sequence

from lib.domain.factory import ProcessorFactory
from lib.interface.processor_ifs import ProcessorIfs
from lib.out.eraser.s3_eraser import S3Eraser
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

        processors: Dict[Literal["events", "brands", "products"], ProcessorIfs] = {}
        for _type in ["events", "brands", "products"]:
            processors[_type] = ProcessorFactory.get_instance(_type=_type)

        results: Dict[
            Literal["events", "brands", "products"],
            Mapping[Literal["data", "updated"], Sequence[Mapping[str, Any]]],
        ] = {}
        for _type, processor in processors.items():
            try:
                data = processor.run()
                results[_type] = data
            except Exception as e:
                # TODO : Send to slack
                self.logger.error(f"{_type} processor: {e}")
                return False

        # Erase all tmp files
        for _type, data in results.items():
            try:
                s3_sender.send(_type, data)
            except Exception as e:
                # TODO : Send to slack
                self.logger.error(f"{_type} sender: {e}")
        # TODO : Send Event
        return True

    def __configure_logger(self):
        match self.__stage:
            case "prod":
                self.logger.setLevel(logging.INFO)
            case _:
                self.logger.setLevel(logging.DEBUG)
