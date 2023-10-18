from marshmallow import EXCLUDE, Schema, fields


class ServiceBrandEventSchema(Schema):
    id = fields.Integer(required=True)
    image = fields.String(required=True)
    brand = fields.Integer(required=True)
    name = fields.String(required=True)
    start_at = fields.Integer(required=True)
    end_at = fields.Integer(required=True)

    class Meta:
        unknown = EXCLUDE
