import os
from urllib.parse import urlparse

from pandas import DataFrame

from lib.db.factory import RepositoryFactory
from lib.domain.event.model.crawled_event_schema import CrawledBrandEventSchema
from lib.error.processor import SchemaValidationError
from lib.interface.processor_ifs import ProcessorIfs
from lib.interface.repository_ifs import RepositoryIfs


class EventProcessor(ProcessorIfs):
    def __init__(self, *args, **kwargs):
        super().__init__("event")
        self.__repository: RepositoryIfs = RepositoryFactory.get_instance(
            _type="mongo", db_name=os.getenv("MONGO_CRAWLING_DB")
        )

    def _preprocess(self, *args, **kwargs) -> DataFrame:
        data = self.__repository.find(
            rel_name="events",
            project={"_id": False, "created_at": False, "updated_at": False},
        )
        self.logger.info(f"Data: {len(data)}")
        errors = CrawledBrandEventSchema().validate(data, many=True)
        # TODO : Send error to slack
        if errors:
            raise SchemaValidationError(errors)
        # for error, reason in errors.items():
        #     self.logger.error(f"{error}: {reason}")
        return DataFrame(data)

    def _process(self, data: DataFrame, *args, **kwargs) -> DataFrame:
        data["name"] = data["name"].map(lambda x: x.lower())
        # Replace Image Url
        data["image"] = data["image"].map(
            lambda x: {
                "thumb": os.getenv("IMAGE_URL") + urlparse(x["thumb"]).path
                if x["thumb"]
                else None,
                "others": [
                    os.getenv("IMAGE_URL") + urlparse(y).path for y in x["others"]
                ],
            }
        )
        data["brand"] = data["crawled_info"].map(lambda x: x["brand"])

        # 2. crawled_infos
        data.rename(columns={"crawled_info": "crawled_infos"}, inplace=True)
        data["crawled_infos"] = data["crawled_infos"].map(lambda x: [x])
        return data

    def _postprocess(self, data: DataFrame, *args, **kwargs) -> DataFrame:
        data = data[
            [
                "brand",
                "name",
                "image",
                "description",
                "crawled_infos",
                "start_at",
                "end_at",
            ]
        ]
        return data

    # @classmethod
    # def __end(cls, data: DataFrame, repository: Type[Repository], *args, **kwargs):
    #     filters = data["crawled_infos"].map(
    #             lambda x: {
    #                 "$or": [
    #                     {"crawled_infos.spider": y["spider"], "crawled_infos.id": y["id"]}
    #                     for y in x
    #                 ]
    #             }
    #     )
    #     data = data.to_dict("records")
    #     filters = filters.to_list()
    #     repository.save_all("events", data, filters)
