from flask_marshmallow import Schema
from marshmallow.fields import Str, Bool, Dict, Int
from marshmallow_enum import EnumField

from models.enums.sample import SampleType


class SampleSchema(Schema):
    """Equiv schema for models.model.sample.Sample"""

    class Meta:
        """marshmallow schema"""

        strict = False
        fields = [
            'id',
            'external_id',
            'participant_id',
            'active',
            'meta',
            'type',
        ]

    id = Int(nullable=True, required=False, allow_none=True)
    external_id = Str(nullable=True, required=False, allow_none=True)
    participant_id = Int(nullable=True, required=False, allow_none=True)
    active = Bool(nullable=True, required=False, allow_none=True)
    meta = Dict(nullable=True, required=False, allow_none=True)
    type = EnumField(SampleType, nullable=True, required=False, allow_none=True)
