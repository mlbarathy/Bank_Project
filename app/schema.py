from marshmallow import Schema, fields, validates_schema, ValidationError, INCLUDE
from app.utils.validation import validate_not_empty, validate_numeric, validate_alpha

class BaseSchema(Schema):
    class Meta:
        unknown = INCLUDE

class InsertRecordSchema(BaseSchema):
    user_id      = fields.Integer(required=False, validate=validate_numeric)
    interaction_type    = fields.String(required=True, validate=validate_not_empty)
    item_id   = fields.String(required=True, validate=validate_not_empty)
    name     = fields.Integer(required=True, validate=validate_numeric)

class InsertPayloadSchema(BaseSchema):
    data = fields.List(fields.Nested(InsertRecordSchema), required=True, validate=validate_not_empty)

insert_payload_schema = InsertPayloadSchema()
