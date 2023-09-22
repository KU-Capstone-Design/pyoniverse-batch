from marshmallow import Schema, fields

from lib.common.schema import CrawledInfoSchema, ImageSchema


class CrawledBrandEventSchema(Schema):
    crawled_info = fields.Nested(CrawledInfoSchema, required=True)
    created_at = fields.Integer(required=True)
    updated_at = fields.Integer(required=True)
    written_at = fields.Integer(required=True)
    name = fields.String(required=True)
    image = fields.Nested(ImageSchema, required=True)
    description = fields.String(required=True, allow_none=True)
