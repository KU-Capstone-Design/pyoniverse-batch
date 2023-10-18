from marshmallow import EXCLUDE, Schema, fields


class CrawledInfoSchema(Schema):
    spider = fields.String(required=True)
    id = fields.String(required=True)
    url = fields.String(required=True)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class ServiceImageSchema(Schema):
    thumb = fields.URL(required=True)
    others = fields.List(fields.URL(required=True, require_tld=False), required=True)

    class Meta:
        ordered = True
        unknown = EXCLUDE


class CrawledImageSizeSchema(Schema):
    width = fields.Integer(required=True)
    height = fields.Integer(required=True)

    class Meta:
        unknown = EXCLUDE


class CrawledImageSizeMapSchema(Schema):
    thumb = fields.Nested(CrawledImageSizeSchema, required=False)
    others = fields.Nested(CrawledImageSizeSchema, required=False, many=True)

    class Meta:
        unknown = EXCLUDE


class CrawledImageSchema(Schema):
    thumb = fields.URL(
        required=True, allow_none=True, schemes=["s3"], require_tld=False
    )
    others = fields.List(
        fields.URL(required=True, schemes=["s3"], require_tld=False),
        required=True,
    )
    size = fields.Nested(CrawledImageSizeMapSchema, required=True)


class PriceSchema(Schema):
    value = fields.Float(required=True)
    currency = fields.Integer(required=True)

    class Meta:
        ordered = True
        unknown = EXCLUDE
