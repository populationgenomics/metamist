from pydantic.main import BaseModel

# annotate any external objects that must be instantiated with this
# type to force openapi generator to allow for Nones (it will actually allow Any)
OpenApiGenNoneType = bytes | None


class SMBase(BaseModel):
    """Base object for all models"""
