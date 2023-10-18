from lib.converter.brand_converter import BrandConverter
from lib.converter.category_converter import CategoryConverter


def test_convert_category():
    # given
    converter = CategoryConverter()
    categories = {
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
    # when & then
    for k, v in categories.items():
        exp_v = converter.convert_to_id(k)
        exp_k = converter.convert_to_name(v)
        assert v == exp_v and k.title() == exp_k


def test_convert_brand():
    # given
    converter = BrandConverter()
    brands = {
        "GS25": 1,
        "CU": 2,
        "SEVEN ELEVEN": 3,
        "EMART24": 4,
        "CSPACE": 5,
    }
    # when & then
    for k, v in brands.items():
        exp_v = converter.convert_to_id(k)
        exp_k = converter.convert_to_name(v)
        assert v == exp_v and k.upper() == exp_k
