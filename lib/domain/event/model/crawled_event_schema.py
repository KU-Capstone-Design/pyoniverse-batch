from marshmallow import EXCLUDE, Schema, fields

from lib.model.schema import CrawledInfoSchema, ImageSchema


class CrawledBrandEventSchema(Schema):
    crawled_info = fields.Nested(CrawledInfoSchema, required=True)
    start_at = fields.Integer(required=True)
    end_at = fields.Integer(required=True)
    name = fields.String(required=True)
    image = fields.Nested(ImageSchema, required=True)
    description = fields.String(required=True, allow_none=True)

    class Meta:
        order = True
        unknown = EXCLUDE
