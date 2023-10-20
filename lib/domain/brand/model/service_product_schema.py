from marshmallow import EXCLUDE, Schema, fields

from lib.domain.product.model.service_product_schema import ProductBrandSchema


class ServiceProductSchema(Schema):
    id = fields.Integer(required=True)
    name = fields.String(required=True)
    image = fields.String(required=True)
    brands = fields.Nested(ProductBrandSchema, many=True, required=True)
    price = fields.Float(required=True)
    view_count = fields.Integer(required=True)
    good_count = fields.Integer(required=True)

    class Meta:
        unknown = EXCLUDE
