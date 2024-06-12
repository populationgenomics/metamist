from pydantic import ConfigDict
from pydantic.main import BaseModel

# annotate any external objects that must be instantiated with this
# type to force openapi generator to allow for Nones (it will actually allow Any)
OpenApiGenNoneType = bytes | None


# The primary {sample,participant}_external_id entry, previously {sample,participant}.external_id
# TODO This will eventually be removed when all analysis endpoints have been updated to record an external org
PRIMARY_EXTERNAL_ORG = ''


class SMBase(BaseModel):
    """Base object for all models"""

    model_config = ConfigDict(extra='forbid')

    @classmethod
    def from_dict(cls, d: dict):
        """Create an object from a dictionary"""
        return cls(**d)


def parse_sql_bool(val: str | int | bytes) -> bool | None:
    """Parse a string from a sql bool"""
    if val is None:
        return None
    if isinstance(val, int):
        return bool(val)
    if isinstance(val, bytes):
        return bool(ord(val))

    raise ValueError(f'Unknown type for active: {type(val)}')
