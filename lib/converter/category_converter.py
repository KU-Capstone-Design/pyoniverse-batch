class CategoryConverter:
    def __init__(self, *args, **kwargs):
        self.__name_id_map = {
            "Drink": 1,
            "Alcohol": 2,
            "Snack": 3,
            "Ice Cream": 4,
            "Noodle": 5,
            "Lunch Box": 6,
            "Salad": 7,
            "Kimbab": 8,
            "Sandwich": 9,
            "Bread": 10,
            "Food": 11,
            "Household Goods": 12,
        }
        self.__id_name_map = {v: k for k, v in self.__name_id_map.items()}

    def convert_to_id(self, name: str) -> int:
        """
        :param name: Category 이름
        :return: Category id
        """
        if name in self.__name_id_map:
            return self.__name_id_map[name]
        else:
            raise KeyError(f"{name} Not Found in categories")

    def convert_to_name(self, _id: int) -> str:
        """
        :param _id: Category id
        :return: Category 이름
        """
        if _id in self.__id_name_map:
            return self.__id_name_map[_id].title()
        else:
            raise KeyError(f"{_id} Not Found in categories")
