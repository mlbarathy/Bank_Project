# app/utils/validation.py

from marshmallow import ValidationError

def validate_not_empty(value):
    if not value:
        raise ValidationError("This field cannot be empty.")

def validate_currency_length(value):
    if len(value) != 3:
        raise ValidationError("Currency must be a 3-letter code (e.g., USD).")

def validate_alpha(value):
    if not value.isalpha():
        raise ValidationError("This field must contain only alphabetic characters.")

def validate_numeric(value):
    if not str(value).isdigit():
        raise ValidationError("This field must contain only numeric characters.")
