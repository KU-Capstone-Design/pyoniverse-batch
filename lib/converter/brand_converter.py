class BrandConverter:
    def __init__(self, *args, **kwargs):
        self.__name_id_map = {
            "GS25": 1,
            "CU": 2,
            "SEVEN ELEVEN": 3,
            "EMART24": 4,
            "CSPACE": 5,
        }
        self.__id_name_map = {v: k for k, v in self.__name_id_map.items()}

    def convert_to_id(self, name: str) -> int:
        if name in self.__name_id_map:
            return self.__name_id_map[name]
        else:
            raise KeyError(f"{name} NOT FOUND in brands")

    def convert_to_name(self, _id: int) -> str:
        if _id in self.__id_name_map:
            return self.__id_name_map[_id]
        else:
            raise KeyError(f"{_id} NOT FOUND in brands")
