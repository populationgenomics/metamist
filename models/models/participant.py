import json
from typing import Dict, Any

from pydantic import BaseModel


class ParticipantModel(BaseModel):
    """Update participant model"""

    id: int
    meta: Dict
    external_id: str | None = None
    reported_sex: int | None = None
    reported_gender: str | None = None
    karyotype: str | None = None

    def __init__(self, **data: Any) -> None:
        if 'meta' in data and isinstance(data['meta'], str):
            data['meta'] = json.loads(data['meta'])

        super().__init__(**data)
