from flask_marshmallow import Schema
from marshmallow.fields import Str, Bool, Dict, Int
from marshmallow_enum import EnumField

from models.enums.sample import SampleType


class SampleSchema(Schema):
    """Equiv schema for models.model.sample.Sample"""

    class Meta:
        """marshmallow schema"""

        fields = [
            'id',
            'external_id',
            'participant_id',
            'active',
            'meta',
            'type',
        ]

    id = Int()
    external_id = Str()
    participant_id = Int()
    active = Bool()
    meta = Dict()
    type = EnumField(SampleType)
