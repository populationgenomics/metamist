from typing import Optional

from fastapi import Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from db.python.connect import SMConnections, Connection
from api.utils.gcp import email_from_id_token


auth = HTTPBearer()


def authenticate(
    token: Optional[HTTPAuthorizationCredentials] = Depends(auth),
) -> Optional[str]:
    """If a token is provided, return the email, else return None"""
    if token:
        return email_from_id_token(token.credentials)
    return None


def dependable_get_connection(
    project: str, author: str = Depends(authenticate)
) -> Connection:
    """FastAPI handler for getting connection"""
    return SMConnections.get_connection_for_project(project, author)


get_db_connection = Depends(dependable_get_connection)
