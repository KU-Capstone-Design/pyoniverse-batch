from marshmallow import EXCLUDE, Schema, fields

from lib.model.schema import CrawledInfoSchema


class ServiceProductPriceSchema(Schema):
    value = fields.Float(required=True)
    currency = fields.Integer(required=True)
    discounted_value = fields.Float(required=True, allow_none=True)


class ProductBrandSchema(Schema):
    id = fields.Integer(required=True)
    price = fields.Nested(ServiceProductPriceSchema, required=True)
    events = fields.List(fields.Integer(), required=True)


class ProductRecommendationSchema(Schema):
    products = fields.List(fields.Integer(), required=True)
    events = fields.List(fields.Integer(), required=True)


class ServiceProductBestSchema(Schema):
    brand = fields.Integer(required=True)  # best brand
    price = fields.Float(required=True)  # best price
    events = fields.List(fields.Integer(), required=True)  # best events


class ServiceProductSchema(Schema):
    name = fields.String(required=True)
    category = fields.Integer(required=True, allow_none=True)
    description = fields.String(required=True, allow_none=True)
    brands = fields.Nested(ProductBrandSchema, many=True, required=True)
    recommendation = fields.Nested(ProductRecommendationSchema, required=True)
    image = fields.URL(required=True)
    crawled_infos = fields.Nested(CrawledInfoSchema, many=True, required=True)
    price = fields.Float(required=True)  # 기본 가격
    best = fields.Nested(ServiceProductBestSchema, required=True)

    class Meta:
        unknown = EXCLUDE
