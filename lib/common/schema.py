from marshmallow import EXCLUDE, Schema, fields
from marshmallow.validate import Regexp


class CrawledInfoSchema(Schema):
    spider = fields.String(required=True)
    id = fields.String(required=True)
    url = fields.String(required=True)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class ImageSchema(Schema):
    thumb = fields.String(
        required=True, allow_none=True, validate=Regexp(r"^s3?://.*\.webp$")
    )
    others = fields.List(
        fields.String(
            required=True, allow_none=True, validate=Regexp(r"^s3?://.*\.webp$")
        ),
        required=True,
    )

    class Meta:
        ordered = True
        unknown = EXCLUDE


class PriceSchema(Schema):
    value = fields.Float(required=True)
    currency = fields.Integer(required=True)

    class Meta:
        ordered = True
        unknown = EXCLUDE
