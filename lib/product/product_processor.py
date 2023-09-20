import json
import logging
import os
import re
from collections import Counter
from typing import List, Type

import boto3
import pandas as pd
from boto3_type_annotations.s3 import Client
from pandas import DataFrame, Series

from lib.common.util import convert_brand, convert_category
from lib.db import Repository, RepositoryFactory
from lib.product.schema.crawled_product_schema import CrawledProductSchema


class ProductProcessor:
    logger = logging.getLogger("ProductProcessor")

    def __init__(self, *args, **kwargs):
        raise NotImplementedError("This class is not meant to be instantiated")

    @classmethod
    def run(cls, *args, **kwargs):
        cls.logger.info("Start")
        repository = RepositoryFactory.get_repository("MongoRepository")
        data = cls.__start(repository, *args, **kwargs)
        cls.logger.info(f"Process {len(data)} items")
        data = cls.__process(data, *args, **kwargs)

        cls.logger.info(f"End {len(data)} items")
        cls.__end(data, repository, *args, **kwargs)

    @classmethod
    def __start(cls, repository: Type[Repository], *args, **kwargs):
        data = repository.find_all("products")
        cls.logger.info(f"Data: {len(data)}")

        errors = CrawledProductSchema().validate(data=data, many=True)
        for error, reason in errors.items():
            cls.logger.info(f"Error: {error} - {reason}")
            cls.logger.info(f"path: {data[error]['image']['thumb']}")
            # del data[error]
        cls.logger.info(f"Error: {len(errors)}")
        res = []
        for idx, d in enumerate(data):
            if idx in errors:
                continue
            res.append(d)
        return res

    @classmethod
    def __process(cls, data, *args, **kwargs):
        # 1. 데이터 정제
        # 2. 이름 기준으로 병합
        # 3. 상용 데이터로 변환
        df = DataFrame(data)
        df.drop(
            columns=["_id", "created_at", "updated_at"], inplace=True, errors="ignore"
        )

        # 1. 데이터 정제
        df = cls.__normalize(df)

        # 2. 이름 기준으로 병합
        df = cls.__merge(df)

        # 3. 병합 후 데이터 정제
        df = cls.__post_merge(df)

        # 3. 상용 데이터로 변환
        df = cls.__convert(df)

        return df

    @classmethod
    def __normalize(cls, df: DataFrame, **kwargs):
        # 1. 이름 정제
        df["name"] = df["name"].map(cls.__normalize_name)
        return df

    @classmethod
    def __normalize_name(cls, x: str):
        x = x.strip().lower()
        x = re.sub(r"\[|\{", "(", x)
        x = re.sub(r"\]|\}", ")", x)

        p = re.compile(r"(\d+(\.\d)?\d*)\s*(ml|l|kg|g|mm|p|t|입)")
        if t := p.search(x):
            digit = float(t.group(1))
            metric = t.group(3)
            x = p.sub("", x)
            x = re.sub(r"(\(\s*\)|\s*\}|\s*\])", "", x)
            x = f"{x}({digit}{metric})"
        x = re.sub(r"\s+", " ", x).strip()
        return x

    @classmethod
    def __merge(cls, df: DataFrame, **kwargs):
        # Name 기준으로 병합 - nan 값은 무시
        df = df.groupby("name").agg(
            {
                "crawled_info": list,
                "description": list,
                "events": "sum",
                "image": list,
                "price": list,
                "category": list,
                "tags": "sum",
                "discounted_price": list,
            }
        )
        for column in df.columns:
            if column == "name":
                continue
            df[column] = df[column].map(
                lambda x: list(
                    filter(
                        lambda y: isinstance(y, list)
                        or isinstance(y, dict)
                        or pd.notna(y),
                        x,
                    )
                )
            )
        df.reset_index(inplace=True)

        return df

    @classmethod
    def __post_merge(cls, df: DataFrame, **kwargs):
        # 1. image
        df["image"] = df["image"].map(cls.__get_largest_image)
        # 2. description
        df["description"] = df["description"].map(
            lambda x: x[0] if len(x) > 0 else None
        )
        # 3. category
        df["category"] = df.apply(cls.__get_category, axis=1)
        # 4. brand 별 정제
        df["brands"] = df.apply(cls.__collect_by_brand, axis=1)
        # 5. recommendation
        # TODO : Recommendation 처리
        df["recommendation"] = [[{"products": [], "events": []}]] * len(df)
        return df

    @classmethod
    def __get_largest_image(cls, images: List[dict]):
        max_img = None
        max_size = None

        s3client: Client = boto3.client("s3")
        images = DataFrame(images)
        images.dropna(subset=["thumb"], inplace=True)
        max_img = images["thumb"].iloc[0] if len(images) > 0 else None

        # for img in images:
        # TODO : S3 aioboto 로 처리하기
        # if img["thumb"]:
        #     meta = s3client.head_object(Bucket="pyoniverse-image",
        #                                 Key=img["thumb"].split("s3://pyoniverse-image/")[-1])
        #     height = meta["ResponseMetadata"]["HTTPHeaders"]["x-amz-meta-height"]
        #     width = meta["ResponseMetadata"]["HTTPHeaders"]["x-amz-meta-width"]
        #     size = int(height) * int(width)
        #     if max_size is None or size > max_size:
        #         max_size = size
        #         max_img = img["thumb"]
        # for other in img["others"]:
        #     meta = s3client.head_object(Bucket="pyoniverse-image",
        #                                 Key=other.split("s3://pyoniverse-image/")[-1])
        #     height = meta["ResponseMetadata"]["HTTPHeaders"]["x-amz-meta-height"]
        #     width = meta["ResponseMetadata"]["HTTPHeaders"]["x-amz-meta-width"]
        #     size = int(height) * int(width)
        #     if max_size is None or size > max_size:
        #         max_size = size
        #         max_img = other
        return max_img

    @classmethod
    def __get_category(cls, row: Series):
        """
        1. tag 로 카테고리 찾기
        2. name 로 카테고리 찾기
        3. 카테고리가 없다면 -> Food(Default)
        4. 카테고리가 있다면
            1. household 카테고리가 하나라도 걸리면 houshold
            2. food 와 다른 카테고리가 겹치면 해당 카테고리
            3. food 이외의 다른 카테고리 두 개 이상 겹치면 로그 띄우고, 첫번째 카테고리로 매칭
        """
        category_tag_map = {
            "Drink": [
                "냉장커피",
                "커피음료",
                "가공유",
                "음료",
                "에너지음료",
                "원컵류",
                "커피차류",
                "분말커피류",
                "커피차",
                "차류",
                "디카페인",
                "원컵",
            ],
            "Alcohol": [],
            "Snack": [
                "냉장디저트",
                "디저트",
                "스낵",
                "과자류",
                "토이캔디",
                "상온디저트",
                "기능성캔디",
                "비스켓",
                "쿠키",
                "캔디",
                "간식",
            ],
            "Ice Cream": [
                "아이스크림",
            ],
            "Noodle": [
                "조리면",
                "용기면",
                "봉지면",
                "면류",
                "냉장면",
                "건면",
            ],
            "Lunch Box": [
                "도시락",
                "덮밥류",
            ],
            "Salad": [
                "샐러드",
            ],
            "Kimbab": [
                "삼각김밥",
                "주먹밥",
            ],
            "Sandwich": [
                "샌드위치",
                "햄버거",
            ],
            "Bread": [
                "빵",
                "즉석빵",
                "베이커리",
            ],
            "Food": [
                "간편식사",
                "건강기능식품",
                "의약건강기능",
                "즉석식",
                "가공식사",
                "냉장분식류",
                "마른안주류",
                "안주류",
                "간편식",
                "냉장안주",
                "과일",
                "식재료",
                "육가공류",
                "냉동간편식",
                "햄",
                "소시지",
                "수프류",
                "스프류",
                "가공란",
                "마른안주",
                "수산안주",
                "국",
                "탕",
                "찌개",
                "농산안주",
                "죽",
                "수산안주류",
                "냉동만두",
                "조미소스",
                "조미료류",
                "냉동즉석식",
                "즉석밥류",
                "핫바",
                "찌개류",
                "분말커피",
                "햄소시지",
                "안주",
                "축산안주",
                "가공식품",
                "반찬류",
                "김치류",
                "대용식",
                "치즈",
                "육포",
                "즉석밥",
                "국밥류",
                "야채",
                "밑반찬",
                "소스류",
                "두부",
                "덮밥",
                "생란",
                "달걀",
                "계란",
                "분식류",
                "국밥",
                "김치",
                "김",
                "통조림",
                "과일통조림",
                "국탕찌개",
                "즉석요리류",
                "식사대용",
                "식용유",
                "얼음",
                "HEYROO",
                "육가공",
                "편육",
                "즉석요리",
                "냉동밥",
                "유가공품류",
                "반숙란",
                "채소",
                "유부",
                "핫바류",
                "조미료",
                "가루류",
                "냉장분식",
                "농산식재료",
                "냉동식품",
                "기타대용식",
                "가공식",
                "치즈 및 유가공품류",
                "사료",
                "반려동물용품",
                "맛살",
                "오뎅",
                "양곡",
                "맛남의광장",
                "과일. 식재료",
                "어묵",
                "유제품",
            ],
            "Household Goods": [
                "문구류",
                "생활잡화",
                "샴푸린스",
                "신변잡화",
                "패션의류",
                "의류용품",
                "목욕세면",
                "여행용세트",
            ],
        }
        category_name_map = {
            "Drink": [
                "콤푸차",
                "음료",
                "드링크",
                "워터",
                "ml",
                "l" "아메리카노",
                "라떼",
                "우유",
            ],
            "Alcohol": [
                "술",
                "맥주",
                "라거",
                "비어",
                "소주",
            ],
            "Snack": [
                "쿠키",
                "약과",
                "칩",
                "과자",
                "스낵",
                "젤리",
                "스틱",
                "초코콘",
                "딸기별",
                "오감자",
                "푸딩",
                "초코렛타",
                "나쵸",
                "꾸이깡",
                "프레첼",
                "팝콘",
                "초코볼",
                "누네띠네",
                "자일리톨",
                "새콤달콤",
            ],
            "Ice Cream": [
                "수박바",
                "폴라포",
                "파르페",
                "쿨샷스포츠",
                "빵빠레",
                "스크류바",
                "옥동자",
                "와일드바디",
                "왕수박바",
                "죠스바",
                "돼지바",
            ],
            "Noodle": [
                "라면",
                "스파게티",
                "파스타",
                "소바",
                "막국수",
                "국수",
                "짬뽕",
                "당면",
            ],
            "Lunch Box": [
                "도시락",
                "비빔밥",
                "볶음밥",
                "덮밥",
            ],
            "Salad": [
                "샐러드",
                "셀러드",
            ],
            "Kimbab": [
                "삼각",
                "삼각김밥",
                "김밥",
                "주먹밥",
            ],
            "Sandwich": [
                "버거",
                "샌드",
                "샌드위치",
                "더블빅불고기",
                "머핀",
            ],
            "Bread": [
                "베이글",
                "티라미수",
                "케익",
                "바닐라슈",
                "모찌롤",
                "휘낭시에",
                "타르트",
                "빵",
                "까눌레",
                "도넛",
                "파이",
            ],
            "Household Goods": [
                "바디워시",
                "핑크솔트",
                "스타킹",
                "테이프",
                "복사지",
                "노트",
                "밴드",
                "멀티탭",
                "슬리퍼",
                "우의",
                "종이컵",
                "스타킹",
                "장갑",
                "이력서",
                "수세미",
                "호일",
                "샤워볼",
                "접착제",
                "컷터칼",
                "면도기",
                "비누",
                "컵",
                "휴지",
                "양말",
                "팬티",
                "셔츠",
                "제트스트림",
                "네일",
                "메디폼",
                "이어폰",
                "케이블",
                "화장솜",
                "팬츠",
                "풋커버",
                "유성매직",
                "네임펜",
                "이쑤시개",
                "크린",
                "지퍼",
                "젓가락",
                "행주",
                "삭스",
                "칼",
                "가위",
                "돗자리",
                "매트",
                "티슈",
                "봉투",
                "잘풀리는집",
                "생리대",
                "좋은느낌",
                "컨디셔너",
                "삼푸",
                "가글",
                "페브리즈",
                "다우니",
                "피죤",
                "치약",
                "칫솔",
                "순면",
                "표백",
                "여행",
                "바디워시",
            ],
        }

        categories = []

        for tag in row["tags"]:
            for category, tags in category_tag_map.items():
                if tag in tags:
                    categories.append(category)

        for category, names in category_name_map.items():
            for _pattern in names:
                if re.search(_pattern, row["name"]):
                    categories.append(category)

        categories = [convert_category(category) for category in categories]
        categories += row["category"]

        if categories:
            categories = Counter(categories)
            category = categories.most_common(1)[0][0]
        else:
            # Default: Food
            category = convert_category("Food")
        return category

    @classmethod
    def __collect_by_brand(cls, row: Series):
        # TODO : 1. 브랜드 별 가격, 할인 가격, 이벤트 찾기
        brands = []
        for idx, crawled_info in enumerate(row["crawled_info"]):
            # TODO : Spider 수정 - Crawled info
            if "gs25" in crawled_info["spider"]:
                brand = "GS25"
            elif "cu" in crawled_info["spider"]:
                brand = "CU"
            elif "seven" in crawled_info["spider"]:
                brand = "Seven Eleven"
            elif "cspace" in crawled_info["spider"]:
                brand = "Cspace"
            elif "emart24" in crawled_info["spider"]:
                brand = "Emart24"
            else:
                cls.logger.error(f"Unknown brand: {crawled_info['spider']}")
                continue

            brand_id = convert_brand(brand)
            price = row["price"][idx]
            # TODO : Spider 수정 - discounted price 를 Price 안에 넣어야 한다
            discounted_price = None
            brand_events = [e["id"] for e in row["events"] if e["brand"] == brand_id]
            brands.append(
                {
                    "id": brand_id,
                    "price": {
                        "value": price["value"],
                        "currency": price["currency"],
                        "discounted_value": discounted_price,
                    },
                    "events": brand_events,
                }
            )
        return brands

    @classmethod
    def __convert(cls, df: DataFrame, **kwargs):
        # TODO : Status 처리
        # 1. status
        df["status"] = 1

        # 2. crawled_infos
        df.rename(columns={"crawled_info": "crawled_infos"}, inplace=True)

        df.drop(
            columns=["tags", "discounted_price", "price", "events"],
            inplace=True,
            errors="ignore",
        )
        return df

    @classmethod
    def __end(cls, data: DataFrame, repository: Type[Repository], *args, **kwargs):
        """
        저장
        """
        filters = data["crawled_infos"].map(
            lambda x: {
                "$or": [
                    {"crawled_infos.spider": y["spider"], "crawled_infos.id": y["id"]}
                    for y in x
                ]
            }
        )
        data = data.to_dict("records")
        filters = filters.to_list()
        repository.save_all("products", data, filters)

    @classmethod
    def test_start(cls, repository: Type[Repository], *args, **kwargs):
        if os.getenv("STAGE") != "test":
            raise RuntimeError("This method is only for test")
        return cls.__start(repository, *args, **kwargs)

    @classmethod
    def test_process(cls, data, *args, **kwargs):
        if os.getenv("STAGE") != "test":
            raise RuntimeError("This method is only for test")
        return cls.__process(data, *args, **kwargs)

    @classmethod
    def test_normalize(cls, df: DataFrame, **kwargs):
        if os.getenv("STAGE") != "test":
            raise RuntimeError("This method is only for test")
        return cls.__normalize(df, **kwargs)

    @classmethod
    def test_merge(cls, df: DataFrame, **kwargs):
        if os.getenv("STAGE") != "test":
            raise RuntimeError("This method is only for test")
        return cls.__merge(df, **kwargs)

    @classmethod
    def test_post_merge(cls, df: DataFrame, **kwargs):
        if os.getenv("STAGE") != "test":
            raise RuntimeError("This method is only for test")
        return cls.__post_merge(df, **kwargs)

    @classmethod
    def test_get_largest_image(cls, images: List[dict]):
        if os.getenv("STAGE") != "test":
            raise RuntimeError("This method is only for test")
        return cls.__get_largest_image(images)

    @classmethod
    def test_get_category(cls, row):
        if os.getenv("STAGE") != "test":
            raise RuntimeError("This method is only for test")
        return cls.__get_category(row)

    @classmethod
    def test_collect_by_brand(cls, row):
        if os.getenv("STAGE") != "test":
            raise RuntimeError("This method is only for test")
        return cls.__collect_by_brand(row)

    @classmethod
    def test_convert(cls, df: DataFrame, **kwargs):
        if os.getenv("STAGE") != "test":
            raise RuntimeError("This method is only for test")
        return cls.__convert(df, **kwargs)

    @classmethod
    def test_end(cls, data, repository: Type[Repository], *args, **kwargs):
        if os.getenv("STAGE") != "test":
            raise RuntimeError("This method is only for test")
        return cls.__end(data, repository, *args, **kwargs)
