from typing import Type
from inspect import isclass

from fastapi import APIRouter
from api.utils.db import get_projectless_db_connection
from db.python import enum_tables
from db.python.enum_tables.enums import EnumTable

router = APIRouter(prefix='/enums', tags=['enums'])


def create_route(enum: Type[EnumTable]):
    hyphenated_name = enum.get_enum_name().replace('_', '-')
    camel_case_name = ''.join([x.capitalize() for x in hyphenated_name.split('-')])

    @router.get(
        '/' + hyphenated_name,
        operation_id='get' + camel_case_name + 's',
    )
    async def get(connection=get_projectless_db_connection) -> list[str]:
        return await enum(connection).get()


for enum in enum_tables.__dict__.values():
    if not isclass(enum):
        continue
    create_route(enum)
