from typing import Literal

from lib.domain.brand.processor import BrandProcessor
from lib.domain.event.processor import EventProcessor
from lib.domain.product.processor import ProductProcessor
from lib.interface.processor_ifs import ProcessorIfs


class ProcessorFactory:
    @classmethod
    def get_instance(cls, _type: Literal["event", "product", "brand"]) -> ProcessorIfs:
        match _type:
            case "event":
                return EventProcessor()
            case "brand":
                return BrandProcessor()
            case "product":
                return ProductProcessor()
