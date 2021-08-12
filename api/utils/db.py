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


async def dependable_get_project_connection(
    project: str, author: str = Depends(authenticate)
) -> Connection:
    """FastAPI handler for getting connection WITH project"""
    return await SMConnections.get_connection(project_name=project, author=author)


async def dependable_get_connection(author: str = Depends(authenticate)):
    """FastAPI handler for getting connection withOUT project"""
    return await SMConnections.get_connection_no_project(author)


get_author = Depends(authenticate)
get_project_db_connection = Depends(dependable_get_project_connection)
get_projectless_db_connection = Depends(dependable_get_connection)
