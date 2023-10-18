import os
from typing import Any, Mapping, Sequence

from pandas import DataFrame

from lib.converter.event_converter import EventConverter
from lib.db.factory import RepositoryFactory
from lib.domain.brand.model.constant_brand_schema import ConstantBrandSchema
from lib.domain.brand.model.service_event_schema import ServiceBrandEventSchema
from lib.domain.brand.model.service_product_schema import ServiceProductSchema
from lib.error.processor import SchemaValidationError
from lib.interface.processor_ifs import ProcessorIfs
from lib.interface.repository_ifs import RepositoryIfs


class BrandProcessor(ProcessorIfs):
    """
    service DB 데이터를 이용해 페이지 미리 만들기
    service DB를 이용하는 배치와 crawling DB를 이용하는 배치는 분리되어야 한다.
    $.products와 $.events만 배치를 통해 업데이트하고, 나머지는 Manually 갱신한다.
    """

    def __init__(self, *args, **kwargs):
        super().__init__("brand")
        self.__repository: RepositoryIfs = RepositoryFactory.get_instance(
            _type="mongo", db_name=os.getenv("MONGO_SERVICE_DB")
        )

        self.__constant_repository: RepositoryIfs = RepositoryFactory.get_instance(
            _type="mongo", db_name=os.getenv("MONGO_CONSTANT_DB")
        )

    def _preprocess(self, *args, **kwargs) -> DataFrame:
        events = self.__repository.find(
            rel_name="events",
            project={
                "_id": False,
                "brand": True,
                "image": "$image.thumb",
                "id": True,
                "start_at": True,
                "end_at": True,
                "name": True,
            },
        )
        self.logger.info(f"Event: {len(events)}")
        errors = ServiceBrandEventSchema().validate(events, many=True)
        # TODO : Send error to slack
        if errors:
            raise SchemaValidationError(errors)
        products = self.__repository.find(
            rel_name="products",
            project={
                "_id": False,
                "image": True,
                "id": True,
                "brands": True,
                "name": True,
                "price": True,
                "good_count": True,
            },
        )
        self.logger.info(f"Products: {len(products)}")
        errors = ServiceProductSchema().validate(products, many=True)
        # TODO : Send error to slack
        if errors:
            raise SchemaValidationError(errors)
        brands = self.__constant_repository.find(
            rel_name="brands", project={"_id": False, "id": "$_id", "name": True}
        )
        self.logger.info(f"Brands: {len(brands)}")
        errors = ConstantBrandSchema().validate(brands, many=True)
        if errors:
            raise SchemaValidationError(errors)

        # Join
        brands = DataFrame(brands)
        products = DataFrame(products)
        events = DataFrame(events)

        t1 = brands.set_index("id")
        t2 = events.set_index("brand")

        t = t1.join(t2.add_prefix("event_"), how="inner")

        t3 = products.explode("brands").rename(columns={"brands": "brand"})
        t3.set_index(t3["brand"].map(lambda x: x["id"]), inplace=True)

        t = t.join(t3.add_prefix("product_"), how="inner")
        return t

    def _process(self, data: DataFrame, *args, **kwargs) -> DataFrame:
        event_converter = EventConverter()
        data["events"] = data[
            [
                "event_name",
                "event_id",
                "event_start_at",
                "event_end_at",
                "event_image",
                "name",
            ]
        ].apply(
            lambda x: {
                "brand": x["name"],
                "image": x["event_image"],
                "name": x["event_name"],
                "id": x["event_id"],
                "image_alt": f"{x['event_name']} thumbnail",
                "start_at": x["event_start_at"],
                "end_at": x["event_end_at"],
            },
            axis=1,
        )
        data["products"] = data[
            [
                "product_brand",
                "product_image",
                "product_name",
                "product_id",
                "product_price",
                "product_good_count",
            ]
        ].apply(
            lambda x: {
                "id": x["product_id"],
                "image": x["product_image"],
                "image_alt": f"{x['product_name']} thumbnail",
                "name": x["product_name"],
                "good_count": x["product_good_count"],
                "price": x["product_price"],
                "events": list(
                    map(event_converter.convert_to_name, x["product_brand"]["events"])
                ),
                "event_price": x["product_brand"]["price"]["value"]
                if x["product_brand"]["price"]["value"] != x["product_price"]
                else None,
            },
            axis=1,
        )
        return data

    def _postprocess(
        self, data: DataFrame, *args, **kwargs
    ) -> Sequence[Mapping[str, Any]]:
        pass
