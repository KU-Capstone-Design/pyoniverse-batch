from marshmallow import Schema, fields


class ServiceProductPriceSchema(Schema):
    value = fields.Float(required=True)
    currency = fields.String(required=True)
    discounted_value = fields.Float(required=True, allow_none=True)


class ProductBrandSchema(Schema):
    id = fields.Integer(required=True)
    price = fields.Nested(ServiceProductPriceSchema, required=True)
    events = fields.List(fields.Integer(), required=True)


class ProductRecommendationSchema(Schema):
    products = fields.List(fields.Integer(), required=True)
    events = fields.List(fields.Integer(), required=True)


class ServiceProductSchema(Schema):
    id = fields.Integer(required=True)
    status = fields.Integer(required=True)
    created_at = fields.Integer(required=True)
    updated_at = fields.Integer(required=True)
    name = fields.String(required=True)
    category = fields.Integer(required=True)
    description = fields.String(required=True, allow_none=True)
    brands = fields.Nested(ProductBrandSchema, many=True, required=True)
    recommendation = fields.Nested(ProductRecommendationSchema, required=True)
    image = fields.URL(required=True)
