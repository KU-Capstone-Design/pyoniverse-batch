from datetime import datetime
from typing import Iterator

from lib.downloader.s3_downloader import S3Downloader


class Downloader:
    def __init__(self, *args, **kwargs):
        self.__downloaders = {"s3": S3Downloader()}

    def download(self, db_name: str, rel_name: str, date: datetime) -> Iterator[dict]:
        return self.__downloaders["s3"].download(
            db_name=db_name, rel_name=rel_name, date=date
        )
