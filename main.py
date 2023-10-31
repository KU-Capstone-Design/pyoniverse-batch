import logging
import traceback
from argparse import ArgumentParser

from lib.dependency_injector.injector import MainInjector
from lib.engine import Engine


parser = ArgumentParser(
    prog="python main.py --help",
    description="Spark 를 이용한 데이터 정제 및 저장",
    epilog="created by @yeongro",
)
parser.add_argument("--stage", choices=["dev", "test", "prod"])
parser.add_argument("--date", help="실행된 날짜 정보. yyyy-MM-dd 형식 ex) 2023-11-11")
if __name__ == "__main__":
    args = parser.parse_args()
    main_injector = MainInjector()
    main_injector.inject()
    logger = logging.getLogger(__name__)
    try:
        engine = Engine(stage=args.stage, date=args.date)
        res = engine.run()
        logger.info("Normal exit")
        exit(0)
    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"Abnormal exit: {tb}")
        exit(1)  # 비정상 종료
