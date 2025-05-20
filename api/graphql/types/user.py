from dataclasses import dataclass
import json
import strawberry
import datetime
from enum import Enum


@strawberry.enum
class DisplayMode(Enum):
    """Enum for display modes."""

    LIGHT = 'light'
    DARK = 'dark'
    AUTO = 'auto'


@dataclass
class UserSettings:
    """User settings model. This is used for output from db -> GraphQLUser"""

    display_mode: DisplayMode | None = None

    @classmethod
    def validate_fields(cls, instance):
        """Validate that the instance only contains allowed fields."""
        allowed_keys = cls.__annotations__.keys()
        extra_keys = set(instance.__dict__.keys()) - allowed_keys
        if extra_keys:
            raise ValueError(f'Unexpected fields: {extra_keys}')


@strawberry.input
class UserSettingsInput(UserSettings):
    """User settings input model. This is used for GraphQL input -> db methods"""

    def to_dict(self) -> dict:
        """Convert UserSettingsInput to a dictionary with camelCase keys and enum values."""

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
        """Convert UserSettingsInput to a JSON string with camelCase keys and enum values."""
        return json.dumps(self.to_dict())


@strawberry.type
class GraphQLUser:
    """User GraphQL model"""

    id: int
    email: str
    full_name: str | None
    settings: UserSettings | None
    created_at: datetime.datetime
    updated_at: datetime.datetime

    @staticmethod
    def from_internal(user: dict) -> 'GraphQLUser':
        """Convert from internal User model (db results) to GraphQLUser"""
        settings = user.get('settings', {})

        return GraphQLUser(
            id=user['id'],
            email=user['email'],
            full_name=user['full_name'],
            settings=UserSettings(**settings),
            created_at=user['created_at'],
            updated_at=user['updated_at'],
        )
