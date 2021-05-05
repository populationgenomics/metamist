from flask_marshmallow import Schema
from marshmallow.fields import Str, Number, Bool, Dict
from marshmallow_enum import EnumField

from models.enums.sample import SampleType


class SampleSchema(Schema):
    """Equiv schema for models.model.sample.Sample"""

    class Meta:
        """marshmallow schema"""

        fields = [
            'id_',
            'external_id',
            'participant_id',
            'active',
            'sample_meta',
            'sample_type',
        ]

    id_ = Number()
    external_id = Str()
    participant_id = Number()
    active = Bool()
    sample_meta = Dict()
    sample_type = EnumField(SampleType)
