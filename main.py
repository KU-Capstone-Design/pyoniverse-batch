import os
from argparse import ArgumentParser

import dotenv


dotenv.load_dotenv()

if __name__ == "__main__":
    from lib.processor.event.event_processor import EventProcessor
    from lib.processor.product.product_processor import ProductProcessor

    parser = ArgumentParser(
        prog="python main.py --help",
        description="Spark 를 이용한 데이터 정제 및 저장",
        epilog="created by @yeongro",
    )
    parser.add_argument(
        "--test",
        action="store_true",
    )
    parser.add_argument("--date", help="실행된 날짜 정보. yyyy-MM-dd 형식 ex) 2023-11-11")

    args = parser.parse_args()

    if args.test:
        os.environ["STAGE"] = "test"

    ProductProcessor.run(stage=os.getenv("STAGE"))
    EventProcessor.run(stage=os.getenv("STAGE"))
