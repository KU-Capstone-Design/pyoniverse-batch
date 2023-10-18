from marshmallow import EXCLUDE, Schema, fields

from lib.model.schema import CrawledInfoSchema, ImageSchema, PriceSchema


class ProductEventSchema(Schema):
    brand = fields.Integer(required=True)
    id = fields.Integer(required=True)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class CrawledProductSchema(Schema):
    crawled_info = fields.Nested(CrawledInfoSchema, required=True)
    name = fields.String(required=True)
    description = fields.String(required=True, allow_none=True)
    events = fields.Nested(ProductEventSchema, many=True, required=True)
    image = fields.Nested(ImageSchema, required=True)
    price = fields.Nested(PriceSchema, required=True)
    discounted_price = fields.Float(required=True, allow_none=True)
    category = fields.Integer(required=True, allow_none=True)
    tags = fields.List(fields.String(), required=True)

    class Meta:
        order = True
        unknown = EXCLUDE
