import os
import re
from collections import Counter
from datetime import datetime
from functools import reduce
from itertools import chain
from typing import Any, List, Mapping, Optional, Sequence
from urllib.parse import urlparse

import numpy as np
import pandas as pd
from pandas import DataFrame, Series

from lib.converter.category_converter import CategoryConverter
from lib.db.factory import RepositoryFactory
from lib.domain.product.model.crawled_product_schema import CrawledProductSchema
from lib.domain.product.model.service_product_schema import ServiceProductSchema
from lib.downloader.downloader import Downloader
from lib.interface.processor_ifs import ProcessorIfs
from lib.interface.repository_ifs import RepositoryIfs


class ProductProcessor(ProcessorIfs):
    def __init__(self, *args, **kwargs):
        super().__init__("products")
        self.__repository: RepositoryIfs = RepositoryFactory.get_instance(
            _type="mongo", db_name=os.getenv("MONGO_CRAWLING_DB")
        )

    def _preprocess(self, date: datetime, *args, **kwargs) -> DataFrame:
        data = self.__repository.find(
            rel_name=self._name,
            project={"_id": False, "created_at": False, "updated_at": False},
        )
        errors = CrawledProductSchema().validate(data, many=True)
        if errors:
            self.logger.error(
                f"Crawling Product Schema Validation Error: {len(errors)}"
            )
            raise RuntimeError(
                f"Crawling Product Schema Validation Error: {len(errors)}"
            )
        self.logger.info(f"Initial data: {len(data)}")
        return DataFrame(data)

    def _process(self, data: DataFrame, date: datetime, *args, **kwargs) -> DataFrame:
        # 1. 데이터 필터
        self.logger.info("Process: filter data")
        data = self.__filter(data)

        # 2. 카테고리 찾기
        self.logger.info("Process: find categories")
        data = self.__fill(data)

        # 1. 데이터 정제
        self.logger.info("Process: normalize data")
        data = self.__normalize(data)

        # 2. 이름 기준으로 병합
        self.logger.info("Process: merge data")
        data = self.__merge(data)

        # 3. 병합 후 데이터 정제
        self.logger.info("Process: postmerge data")
        data = self.__post_merge(data)

        # 4. hostory 추가
        self.logger.info("Process append histories")
        data = self.__append_histories(data, date)

        return data

    def __filter(self, data: DataFrame, *args, **kwargs) -> DataFrame:
        # SALE - discounted_price 정제(discounted value 가 없으면 sale event 제거)
        data["events"] = data[["price", "events"]].apply(
            lambda x: list(
                filter(
                    lambda y: y["id"] != 7 or pd.notna(x["price"]["discounted_value"]),
                    x["events"],
                )
            ),
            axis=1,
        )
        return data

    def __fill(self, data: DataFrame, *args, **kwargs) -> DataFrame:
        data["category"] = data[["category", "tags", "name"]].apply(
            self.__fill_category, axis=1
        )
        return data

    def __fill_category(self, row: Series) -> Optional[int]:
        """
        1. tag 로 카테고리 찾기
        2. name 로 카테고리 찾기
        3. 카테고리가 없다면 -> Food(Default)
        4. 카테고리가 있다면
            1. household 카테고리가 하나라도 걸리면 houshold
            2. food 와 다른 카테고리가 겹치면 해당 카테고리
            3. food 이외의 다른 카테고리 두 개 이상 겹치면 로그 띄우고, 첫번째 카테고리로 매칭
        """
        converter = CategoryConverter()
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
                "l",
                "아메리카노",
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

        categories = list(map(converter.convert_to_id, categories))
        categories.append(row["category"])
        if categories := Counter(filter(pd.notna, categories)):
            category = categories.most_common(1)[0][0]
            return category
        else:
            return None

    def __normalize(self, data: DataFrame, **kwargs):
        # 이름 정제
        data["name"] = data["name"].map(self.__normalize_name)
        return data

    def __normalize_name(self, x: str):
        x = x.strip().lower()
        x = re.sub(r"\[|\{", "(", x)
        x = re.sub(r"\]|\}", ")", x)

        p = re.compile(r"(\d+(\.\d)?\d*)\s*(ml|l|kg|g|mm|p|t|입)")
        if t := p.search(x):
            digit = float(t.group(1))
            metric = t.group(3)
            x = p.sub("", x)
            x = re.sub(r"(\(\s*\)|\s*\}|\s*\])", "", x)
            # remove unclosed & empty parenthesis
            x = re.sub(r"\(\)|^\)", "", x)
            x = f"{x}({digit}{metric})"
        x = re.sub(r"\s+", " ", x).strip()

        # dm -> 덴마크로 변환
        x = x.replace("dm)", "덴마크)")
        return x

    def __merge(self, data: DataFrame, **kwargs):
        # Name 기준으로 병합 - nan 값은 무시
        data = data.groupby("name").agg(
            {
                "crawled_info": list,
                "description": list,
                "events": "sum",
                "image": list,
                "price": list,
                "category": list,
            }
        )
        for column in data.columns:
            if column == "name":
                continue
            data[column] = data[column].map(
                lambda x: list(
                    filter(
                        lambda y: isinstance(y, list)
                        or isinstance(y, dict)
                        or pd.notna(y),
                        x,
                    )
                )
            )
        data.reset_index(inplace=True)
        return data

    def __post_merge(self, data: DataFrame, **kwargs):
        # 1. image
        data["image"] = data["image"].map(self.__get_largest_image)
        # 2. description
        data["description"] = data["description"].map(
            lambda x: x[0] if len(x) > 0 else None
        )
        # 3. category
        data["category"] = data["category"].map(
            lambda x: Counter(filter(pd.notna, x)).most_common(1)[0][0]
            if Counter(filter(pd.notna, x))
            else None,
        )
        # 4. brand 별 정제
        data["brands"] = data.apply(self.__collect_by_brand, axis=1)
        # 5. recommendation
        # TODO : Recommendation 처리
        data["recommendation"] = [{"products": [], "events": []}] * len(data)

        # 기본 가격 = 최대 가격
        data["price"] = data["brands"].map(
            lambda x: max([y["price"]["value"] for y in x])
        )
        data["best"] = data[["price", "brands"]].apply(self.__get_best, axis=1)
        return data

    def __get_largest_image(self, images: List[dict]):
        thumb_images = [x["thumb"] for x in images if x["thumb"] is not None]
        thumb_sizes = list(
            map(
                lambda x: x["size"]["thumb"]["width"] * x["size"]["thumb"]["height"],
                filter(lambda y: "thumb" in y["size"], images),
            )
        )
        other_images = reduce(lambda acc, cur: acc + cur["others"], images, [])
        other_sizes = reduce(
            lambda acc, cur: acc
            + list(map(lambda x: x["width"] * x["height"], cur["size"]["others"])),
            filter(lambda y: "others" in y["size"], images),
            [],
        )
        images = thumb_images + other_images
        sizes = thumb_sizes + other_sizes
        tuples = list(zip(images, sizes))
        if tuples:
            max_img = max(tuples, key=lambda x: x[1])[0]
            max_img = os.getenv("IMAGE_DOMAIN") + urlparse(max_img).path[4:]
            return max_img
        else:
            return None

    def __collect_by_brand(self, row: Series):
        brands = {}
        for idx, crawled_info in enumerate(row["crawled_info"]):
            brand_id = crawled_info["brand"]
            if brand_id not in brands:
                price = row["price"][idx]
                brand_events = [
                    e["id"] for e in row["events"] if e["brand"] == brand_id
                ]
                brands[brand_id] = {
                    "id": brand_id,
                    "price": price,
                    "events": brand_events,
                }
            else:
                price = row["price"][idx]
                brands[brand_id]["price"] = {
                    "value": brands[brand_id]["price"]["value"] or price["value"],
                    "currency": brands[brand_id]["price"]["currency"]
                    or price["currency"],
                    "discounted_value": brands[brand_id]["price"]["discounted_value"]
                    or price["discounted_value"],
                }
        return list(brands.values())

    def __get_best(self, row: Series):
        default_price = row["price"]
        best_price, best_brand, best_events = None, None, []
        for brand in row["brands"]:
            for event in brand["events"]:
                match event:
                    case 1:  # 1+1
                        t_price = brand["price"]["value"] / 2
                        if best_price is None or t_price < best_price:
                            best_price = t_price
                            best_brand = brand["id"]
                            best_events = brand["events"]
                    case 2:  # 2+1
                        t_price = brand["price"]["value"] * 2 / 3
                        if best_price is None or t_price < best_price:
                            best_price = t_price
                            best_brand = brand["id"]
                            best_events = brand["events"]
                    case 7:  # DISCOUNT
                        t_price = brand["price"]["discounted_value"]
                        if best_price is None or t_price < best_price:
                            best_price = t_price
                            best_brand = brand["id"]
                            best_events = brand["events"]
                    case 8:  # 3+1
                        t_price = brand["price"]["value"] * 3 / 4
                        if best_price is None or t_price < best_price:
                            best_price = t_price
                            best_brand = brand["id"]
                            best_events = brand["events"]
                    case _:
                        continue
        if best_price is None:
            best_price = default_price
            best_brand = row["brands"][0]["id"]
            best_events = row["brands"][0]["events"]
        return {
            "price": round(best_price, 2),
            "brand": best_brand,
            "events": best_events,
        }

    def __append_histories(self, data: DataFrame, date: datetime) -> DataFrame:
        downloader = Downloader()
        previous_data = downloader.download(
            db_name=os.getenv("MONGO_SERVICE_DB"), rel_name=self._name, date=date
        )
        try:
            check = next(previous_data)
            previous_data = chain([check], previous_data)
            if "histories" in check:
                previous_df = DataFrame(
                    previous_data,
                    columns=["crawled_infos", "name", "brands", "histories"],
                )
            else:
                previous_df = DataFrame(
                    previous_data, columns=["crawled_infos", "name", "brands"]
                )
                previous_df["histories"] = [[]] * len(previous_df)
        except StopIteration as e:
            self.logger.error("Previous Data doesn't exist")
            data["histories"] = [[]] * len(data)
            return data
        else:
            previous_df.rename(
                columns={
                    "crawled_infos": "previous_crawled_infos",
                    "brands": "previous_brands",
                },
                inplace=True,
            )
            # merge
            # 1. dm -> 덴마크로 이름 변환
            previous_df["name"] = previous_df["name"].map(
                lambda x: x.replace("dm)", "덴마크)")
            )
            data = data.merge(previous_df, on="name", how="left", validate="one_to_one")
            data["previous_crawled_infos"] = data["previous_crawled_infos"].map(
                lambda x: x if isinstance(x, list) else []
            )
            data["tmp_crawled_info"] = data[
                ["crawled_info", "previous_crawled_infos"]
            ].apply(
                lambda x: {
                    (c["spider"], c["id"], c["url"]) for c in chain.from_iterable(x)
                },
                axis=1,
            )
            # Check (spider, id) is unique
            actual_length = data["tmp_crawled_info"].map(lambda x: len(x)).sum()
            expected_length = (
                data["tmp_crawled_info"]
                .map(lambda x: len([(c[0], c[1]) for c in x]))
                .sum()
            )
            assert actual_length == expected_length

            data["crawled_info"] = data["tmp_crawled_info"].map(
                lambda x: [{"spider": y[0], "id": y[1], "url": y[2]} for y in x]
            )
            data.drop(columns=["tmp_crawled_info"], inplace=True)
            # history 추가
            data["histories"] = data["histories"].map(
                lambda x: x if isinstance(x, list) else []
            )
            data["histories"] = data[["histories", "previous_brands"]].apply(
                lambda x: x["histories"]
                + [{"date": date.strftime("%Y-%m-%d"), "brands": x["previous_brands"]}]
                if isinstance(x["previous_brands"], list)
                else x["histories"],
                axis=1,
            )
            # Deduplicate histories
            data["histories"] = data["histories"].map(
                lambda x: {y["date"]: y["brands"] for y in x}
            )
            data["histories"] = data["histories"].map(
                lambda x: [{"date": k, "brands": v} for k, v in x.items()]
            )
            data.drop(
                columns=["previous_brands", "previous_crawled_infos"], inplace=True
            )
            return data

    def _postprocess(
        self, data: DataFrame, date: datetime, *args, **kwargs
    ) -> Sequence[Mapping[str, Any]]:
        # 3. 상용 데이터로 변환
        data.rename(columns={"crawled_info": "crawled_infos"}, inplace=True)
        data.drop(
            columns=["tags", "discounted_price", "events"],
            inplace=True,
            errors="ignore",
        )
        data = data[data["image"].notna()].copy()
        data.replace(np.nan, None, inplace=True)
        data["status"] = 2  # 현재 업데이트 되는 데이터의 status = 2
        data["status"] = data["category"].map(
            lambda x: 2 if pd.notna(x) else -1
        )  # category = null이면 -1
        data["status"] = data["brands"].map(
            lambda x: 2
            if len(reduce(lambda acc, cur: acc + cur["events"], x, [])) > 0
            else -1
        )  # event가 아무것도 없으면 -1
        data = data.to_dict("records")
        errors = ServiceProductSchema().validate(data, many=True)
        if errors:
            self.logger.error(f"Service Product Schema Validation Error: {len(errors)}")
            raise RuntimeError(
                f"Service Product Schema Validation Error: {len(errors)}"
            )
        return data
