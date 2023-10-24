from typing import Literal

from lib.domain.event.processor import EventProcessor
from lib.domain.product.processor import ProductProcessor
from lib.interface.processor_ifs import ProcessorIfs


class ProcessorFactory:
    @classmethod
    def get_instance(
        cls, _type: Literal["events", "products", "brands"]
    ) -> ProcessorIfs:
        match _type:
            case "events":
                return EventProcessor()
            case "products":
                return ProductProcessor()
