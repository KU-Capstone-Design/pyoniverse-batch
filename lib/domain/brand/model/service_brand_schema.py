from marshmallow import Schema, fields


class ServiceBrandEventSchema(Schema):
    brand = fields.String(required=True)
    image = fields.URL(required=True)
    image_alt = fields.String(required=True)
    name = fields.String(required=True)
    id = fields.Integer(required=True)
    start_at = fields.Date(required=True, format="iso")
    end_at = fields.Date(required=True, format="iso")


class ServiceBrandProductSchema(Schema):
    id = fields.Integer(required=True)
    image = fields.URL(required=True)
    image_alt = fields.String(required=True)
    name = fields.String(required=True)
    good_count = fields.Integer(required=True)
    price = fields.Float(required=True)
    events = fields.List(fields.String(), required=True)
    event_price = fields.Float(required=True, allow_none=True)


class ServiceBrandSchema(Schema):
    id = fields.Integer(required=True)
    events = fields.Nested(ServiceBrandEventSchema, required=True, many=True)
    products = fields.Nested(ServiceBrandProductSchema, required=True, many=True)
