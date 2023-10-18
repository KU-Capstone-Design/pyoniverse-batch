from typing import Any, Mapping


class BaseError(RuntimeError):
    def __init__(self, message: str, reason: Mapping[str, Any] = None, *args):
        super().__init__(*args)
        self.message = message
        self.reason = reason

    def __str__(self):
        return f"{type(self).__name__}: {self.message}"
