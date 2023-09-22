from marshmallow import Schema, fields

from lib.common.schema import CrawledInfoSchema, ImageSchema


class ServiceBrandEventSchema(Schema):
    # id = fields.Integer(required=True)
    status = fields.Integer(required=True)
    # created_at = fields.Integer(required=True)
    # updated_at = fields.Integer(required=True)
    written_at = fields.Integer(required=True)
    name = fields.String(required=True)
    brand = fields.Integer(required=True)
    image = fields.Nested(ImageSchema, required=True)
    description = fields.String(required=True, allow_none=True)
    crawled_infos = fields.Nested(CrawledInfoSchema, required=True, many=True)
