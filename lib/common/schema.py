from marshmallow import EXCLUDE, Schema, fields


class CrawledInfoSchema(Schema):
    spider = fields.String(required=True)
    id = fields.String(required=True)
    url = fields.String(required=True)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class ImageSchema(Schema):
    thumb = fields.URL(
        required=True, allow_none=True, schemes=["s3"], require_tld=False
    )
    others = fields.List(
        fields.URL(required=True, schemes=["s3"], require_tld=False),
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
