import datetime
import json
from enum import Enum

import strawberry

from models.models.user import UserInternal


@strawberry.enum
class DisplayMode(Enum):
    """Enum for display modes."""

    LIGHT = 'light'
    DARK = 'dark'
    AUTO = 'auto'


@strawberry.input
class UserSettings:
    """User settings input model. This is used for GraphQL input -> db methods"""

    display_mode: DisplayMode | None = None

    def validate_fields(self):
        """Validate that the instance only contains allowed fields."""
        allowed_keys = type(self).__annotations__.keys()
        extra_keys = set(self.__dict__.keys()) - set(allowed_keys)
        if extra_keys:
            raise ValueError(f'Unexpected fields: {extra_keys}')

    def to_dict(self) -> dict:
        """Convert UserSettings to a dictionary with camelCase keys and enum values."""

        def to_camel_case(s):
            parts = s.split('_')
            return parts[0] + ''.join(word.capitalize() for word in parts[1:])

        def serialize_value(val):
            if isinstance(val, Enum):
                return val.value
            return val

        return {
            to_camel_case(k): serialize_value(v)
            for k, v in self.__dict__.items()
            if v is not None
        }

    def to_db(self) -> str:
        """Convert UserSettings to a JSON string with camelCase keys and enum values."""
        return json.dumps(self.to_dict())


@strawberry.type
class GraphQLUser:
    """User GraphQL model"""

    id: int
    email: str
    full_name: str
    settings: strawberry.scalars.JSON
    created_at: datetime.datetime
    updated_at: datetime.datetime

    @staticmethod
    def from_internal(user: UserInternal) -> 'GraphQLUser':
        """Convert UserInternal to GraphQLUser."""
        return GraphQLUser(
            id=user.id,
            email=user.email,
            full_name=user.full_name,
            settings=strawberry.scalars.JSON(user.settings),
            created_at=user.created_at,
            updated_at=user.updated_at,
        )
