import datetime
from models.base import SMBase, parse_sql_dict


class UserInternal(SMBase):
    """
    Internal class for User records
    """

    id: int
    email: str
    full_name: str
    settings: dict
    created_at: datetime.datetime
    updated_at: datetime.datetime

    @staticmethod
    def from_db(db_dict: dict):
        """Convert from db Record"""
        return UserInternal(
            id=db_dict.pop('id'),
            email=db_dict.pop('email'),
            full_name=db_dict.pop('full_name'),
            settings=parse_sql_dict(db_dict.pop('settings', None)) or {},
            created_at=db_dict.pop('created_at'),
            updated_at=db_dict.pop('updated_at'),
        )
