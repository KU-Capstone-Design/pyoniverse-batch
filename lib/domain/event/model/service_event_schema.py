from marshmallow import Schema, fields

from lib.model.schema import CrawledInfoSchema, ServiceImageSchema


class ServiceBrandEventSchema(Schema):
    status = fields.Integer(required=True)
    name = fields.String(required=True)
    brand = fields.Integer(required=True)
    image = fields.Nested(ServiceImageSchema, required=True)
    description = fields.String(required=True, allow_none=True)
    crawled_infos = fields.Nested(CrawledInfoSchema, required=True, many=True)
    start_at = fields.Integer(required=True)
    end_at = fields.Integer(required=True)
