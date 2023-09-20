import dotenv

from lib.product.product_processor import ProductProcessor


dotenv.load_dotenv()
import os
from argparse import ArgumentParser


if __name__ == "__main__":
    parser = ArgumentParser(
            prog="python main.py --help",
            description="Spark 를 이용한 데이터 정제 및 저장",
            epilog="created by @yeongro"
    )
    parser.add_argument("--test", action="store_true", )

    args = parser.parse_args()

    if args.test:
        os.environ["STAGE"] = "test"

    # TODO : Spark 전환
    ProductProcessor.run(stage=os.getenv("STAGE"))
