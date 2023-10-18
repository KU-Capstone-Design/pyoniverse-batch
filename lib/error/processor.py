from typing import Any, Mapping

from lib.error import BaseError


class SchemaValidationError(BaseError):
    def __init__(self, reason: Mapping[str, Any] = None):
        super().__init__("Some Data cannot be validated by schema", reason)
