from pydantic import ConfigDict
from pydantic.main import BaseModel

# annotate any external objects that must be instantiated with this
# type to force openapi generator to allow for Nones (it will actually allow Any)
OpenApiGenNoneType = bytes | None


class SMBase(BaseModel):
    """Base object for all models"""

    model_type = ConfigDict(extra='forbid')

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
