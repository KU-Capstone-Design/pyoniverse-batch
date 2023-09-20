def convert_category(category: str) -> int:
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
    return categories[category]


def convert_brand(brand: str) -> int:
    brand = brand.upper().strip()
    match brand:
        case "GS25":
            return 1
        case "CU":
            return 2
        case "SEVEN ELEVEN":
            return 3
        case "EMART24":
            return 4
        case "CSPACE":
            return 5
        case _:
            raise ValueError(f"Unknown brand: {brand!r}")
