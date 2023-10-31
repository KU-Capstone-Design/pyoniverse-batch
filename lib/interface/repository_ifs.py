import logging
from abc import ABCMeta, abstractmethod
from typing import Any, List, Mapping, Optional


class RepositoryIfs(metaclass=ABCMeta):
    def __init__(self, client: Any, db_name: str, *args, **kwargs):
        self.logger = logging.getLogger(__name__)
        self._client = client
        self._db_name = db_name

    @abstractmethod
    def find_one(
        self,
        rel_name: str,
        filter: Mapping[str, Any] = None,
        project: Mapping[str, bool] = None,
        hint: Mapping[str, int] = None,
        *args,
        **kwargs,
    ) -> Optional[Mapping[str, Any]]:
        pass

    @abstractmethod
    def find(
        self,
        rel_name: str,
        filter: Mapping[str, Any] = None,
        project: Mapping[str, bool] = None,
        hint: Mapping[str, int] = None,
        order_by: Mapping[str, int] = None,
        limit: int = None,
        *args,
        **kwargs,
    ) -> List[Mapping[str, Any]]:
        pass

    @abstractmethod
    def distinct(
        self,
        rel_name: str,
        attr_name: str,
        filter: Mapping[str, Any] = None,
        *args,
        **kwargs,
    ) -> List[Any]:
        pass
