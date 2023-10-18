from marshmallow import Schema, fields


class ConstantBrandSchema(Schema):
    id = fields.Integer(required=True)
    name = fields.String(required=True)
