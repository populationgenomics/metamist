# pylint: disable=unused-import,too-many-instance-attributes
# flake8: noqa
"""
Code for connecting to Postgres database
"""

import asyncio
import json
import logging
import os
from typing import Iterable

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import DictRow, dict_row
from db.python.tables.project import ProjectPermissionsTable
from db.python.utils import (
    InternalError,
    NoProjectAccess,
    NotFoundError,
    ProjectDoesNotExist,
)
from models.models.project import Project, ProjectId, ProjectMemberRole

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


MAIN_SCHEMA = 'main'
HISTORY_SCHEMA = 'history'


class Connection:
    """Stores a DB connection config, project and author"""

    def __init__(
        self,
        postgres_pool: AsyncConnectionPool[AsyncConnection[DictRow]],
        project: Project | None,
        project_id_map: dict[ProjectId, Project],
        project_name_map: dict[str, Project],
        author: str,
        on_behalf_of: str | None,
        ar_guid: str | None,
        meta: dict[str, str] | None = None,
    ):
        self.__postgres_pool = postgres_pool
        self.__project: Project | None = project
        self.__project_id_map = project_id_map
        self.__project_name_map = project_name_map
        self.author: str = author
        self.on_behalf_of: str | None = on_behalf_of
        self.ar_guid: str | None = ar_guid
        self.meta = meta

        self._audit_log_id: int | None = None
        self._audit_log_lock = asyncio.Lock()

    @property
    def pool(self):
        return self.__postgres_pool

    @property
    def project(self):
        """Public getter for private project class variable"""
        return self.__project

    @property
    def project_id_map(self):
        """Public getter for private project_id_map class variable"""
        return self.__project_id_map

    @property
    def project_name_map(self):
        """Public getter for private project_name_map class variable"""
        return self.__project_name_map

    @property
    def project_id(self):
        """Safely get the project id from the project model attached to the connection"""
        return self.project.id if self.project is not None else None

    def all_projects(self):
        """Return all projects that the current user has access to"""
        return list(self.project_id_map.values())

    def projects_with_role(self, allowed_roles: set[ProjectMemberRole]):
        """Return all projects that the current user has access to"""
        return [p for p in self.project_id_map.values() if p.roles & allowed_roles]

    def get_and_check_access_to_projects(
        self, projects: Iterable[Project], allowed_roles: set[ProjectMemberRole]
    ):
        """
        Check if the current user has _any_ of the specified roles in _all_ of the
        specified projects. Raise an error if they do not.
        """
        # projects that the user has some access to, but not the required access
        disallowed_projects = [p for p in projects if not p.roles & allowed_roles]

        if disallowed_projects:
            raise NoProjectAccess(
                [p.name for p in disallowed_projects],
                allowed_roles=[r.name for r in allowed_roles],
                author=self.author,
            )

        return projects

    def get_and_check_access_to_projects_for_ids(
        self, project_ids: Iterable[ProjectId], allowed_roles: set[ProjectMemberRole]
    ):
        """
        Check if the current user has _any_ of the specified roles in _all_ of the
        projects based on the specified project ids. Raise an error if they do not.
        Also raise an error if any of the specified project ids doesn't exist or the
        current user has no access to it. Return the matching projects
        """
        projects = [
            self.project_id_map[id] for id in project_ids if id in self.project_id_map
        ]

        # Check if any of the provided ids aren't valid project ids, or the user has
        # no access to them at all. A NotFoundError is raised here rather than a
        # Forbidden so as to not leak the existence of the project to those with no access.
        missing_project_ids = set(project_ids) - set(p.id for p in projects)
        if missing_project_ids:
            missing_project_ids_str = ', '.join([str(p) for p in missing_project_ids])
            raise NotFoundError(
                f'Could not find projects with ids: {missing_project_ids_str}'
            )

        return self.get_and_check_access_to_projects(projects, allowed_roles)

    def check_access_to_projects_for_ids(
        self, project_ids: Iterable[ProjectId], allowed_roles: set[ProjectMemberRole]
    ):
        """
        Check if the current user has _any_ of the specified roles in _all_ of the
        projects based on the specified project ids. Raise an error if they do not.
        Also raise an error if any of the specified project ids doesn't exist or the
        current user has no access to it. Returns None
        """
        self.get_and_check_access_to_projects_for_ids(project_ids, allowed_roles)

    def get_and_check_access_to_projects_for_names(
        self, project_names: Iterable[str], allowed_roles: set[ProjectMemberRole]
    ):
        """
        Check if the current user has _any_ of the specified roles in _all_ of the
        projects based on the specified project names. Raise an error if they do not.
        Also raise an error if any of the specified project names doesn't exist or
        the current user has no access to it. Return the matching projects
        """
        projects = [
            self.project_name_map[name]
            for name in project_names
            if name in self.project_name_map
        ]

        # Check if any of the provided names aren't valid project names, or the user has
        # no access to them at all.  A NotFoundError is raised here rather than a
        # Forbidden so as to not leak the existence of the project to those with no access.
        missing_project_names = set(project_names) - set(p.name for p in projects)

        if missing_project_names:
            missing_project_names_str = ', '.join(
                [f'"{str(p)}"' for p in missing_project_names]
            )
            raise NotFoundError(
                f'Could not find projects with names: {missing_project_names_str}'
            )

        return self.get_and_check_access_to_projects(projects, allowed_roles)

    def check_access_to_projects_for_names(
        self, project_names: Iterable[str], allowed_roles: set[ProjectMemberRole]
    ):
        """
        Check if the current user has _any_ of the specified roles in _all_ of the
        projects based on the specified project names. Raise an error if they do not.
        Also raise an error if any of the specified project names doesn't exist or
        the current user has no access to it. Returns None
        """
        self.get_and_check_access_to_projects_for_names(project_names, allowed_roles)

    def check_access(self, allowed_roles: set[ProjectMemberRole]):
        """
        Check if the current user has the specified role within the project that is
        attached to the connection. If there is no project attached to the connection
        this will raise an error.
        """
        if self.project is None:
            raise InternalError(
                'Connection was expected to have a project attached, but did not'
            )
        if not allowed_roles & self.project.roles:
            raise NoProjectAccess(
                project_names=[self.project.name],
                author=self.author,
                allowed_roles=[r.name for r in allowed_roles],
            )

    async def audit_log_id(self):
        """Get audit_log ID for write operations, cached per connection"""

        async with self._audit_log_lock:
            if not self._audit_log_id:
                # make this import here, otherwise we'd have a circular import
                from db.python.tables.audit_log import (  # pylint: disable=import-outside-toplevel,R0401
                    AuditLogTable,
                )

                at = AuditLogTable(connection=self)
                self._audit_log_id = await at.create_audit_log(
                    author=self.author,
                    on_behalf_of=self.on_behalf_of,
                    ar_guid=self.ar_guid,
                    comment=None,
                    project=self.project_id,
                    meta=self.meta,
                )

        return self._audit_log_id

    def update_project(self, project_name: str):
        """Update the project attached to the connection to the specified project name"""
        if project_name not in self.project_name_map:
            raise ProjectDoesNotExist(project_name)

        self.__project = self.project_name_map[project_name]

    async def refresh_projects(self):
        """
        Re-fetch the projects for the current user and update the connection.
        This only really needs to be run after project member updates or project
        creation, and really only for tests. The API fetches projects on each request
        so subsequent requests after updates will already have up-to-date data
        """
        pt = ProjectPermissionsTable(connection=self)

        project_id_map, project_name_map = await pt.get_projects_accessible_by_user(
            user=self.author
        )
        self.__project_id_map = project_id_map
        self.__project_name_map = project_name_map

        if self.project_id:
            self.__project = self.project_id_map.get(self.project_id)


class CredentialedDatabaseConfiguration:
    """Class to hold information about a MySqlConfiguration"""

    def __init__(
        self,
        dbname: str,
        host: str | None = None,
        port: str | None = None,
        username: str | None = None,
        password: str | None = None,
    ):
        self.dbname = dbname
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    @staticmethod
    def dev_config() -> 'CredentialedDatabaseConfiguration':
        """Dev config for local database with name 'sm_dev'"""
        # consider pulling from env variables
        return CredentialedDatabaseConfiguration(
            dbname=os.environ.get('SM_DEV_DB_NAME', 'sm_dev'),
            username=os.environ.get('SM_DEV_DB_USER', 'root'),
            password=os.environ.get('SM_DEV_DB_PASSWORD', ''),
            host=os.environ.get('SM_DEV_DB_HOST', '127.0.0.1'),
            port=os.environ.get('SM_DEV_DB_PORT', '3306'),
        )

    def get_connection_string(self):
        """Prepares the connection string for mysql / mariadb"""

        _host = self.host or 'localhost'

        assert self.username
        u_p = self.username

        if self.password:
            u_p += f':{self.password}'
        if self.port:
            _host += f':{self.port}'

        url = f'postgresql://{u_p}@{_host}/{self.dbname}'

        return url


async def configure_pg_connection(connection: AsyncConnection):
    """Configure a new connection

    - set the search path to include the main and history schemas
    - set autocommit to True for more predictable behavious
        this avoids creating transactions for queries that don't need them, and requires
        us to explictly create transactions when we need them

    """
    async with connection:
        await connection.set_autocommit(True)
        await connection.execute(f'SET search_path TO {MAIN_SCHEMA}, {HISTORY_SCHEMA};')


class SMConnections:
    """Contains useful functions for connecting to the database"""

    _credentials: CredentialedDatabaseConfiguration | None = None
    _postgres_pool: AsyncConnectionPool[AsyncConnection[DictRow]] | None = None

    @staticmethod
    def _get_config():
        if SMConnections._credentials:
            return SMConnections._credentials

        config = CredentialedDatabaseConfiguration.dev_config()
        creds_from_env = os.getenv('SM_DBCREDS')
        if creds_from_env is not None:
            config = CredentialedDatabaseConfiguration(**json.loads(creds_from_env))
            logger.info(f'Using supplied SM DB CREDS: {config.host}')

        SMConnections._credentials = config

        return SMConnections._credentials

    @staticmethod
    def get_postgres_pool() -> AsyncConnectionPool[AsyncConnection[DictRow]]:
        """Get the global Postgres connection pool"""

        if SMConnections._postgres_pool:
            return SMConnections._postgres_pool

        credentials = SMConnections._get_config()

        pool: AsyncConnectionPool[AsyncConnection[DictRow]] = AsyncConnectionPool(
            conninfo=credentials.get_connection_string(),
            # pool is opened/closed by api server lifespan event
            open=False,
            min_size=1,
            max_size=10,
            check=AsyncConnectionPool.check_connection,
            configure=configure_pg_connection,
            kwargs={'row_factory': dict_row},
        )  # type: ignore (psycopg can't infer the row factory type here and thinks rows will be tuples not dicts)
        SMConnections._postgres_pool = pool
        return pool

    @staticmethod
    async def get_connection_with_project(
        *,
        author: str,
        project_name: str,
        allowed_roles: set[ProjectMemberRole],
        ar_guid: str,
        on_behalf_of: str | None = None,
        meta: dict[str, str] | None = None,
    ):
        """Get a db connection from a project and user"""
        # maybe it makes sense to perform permission checks here too
        logger.debug(f'Authenticate connection to {project_name} with {author!r}')

        # Instantiate connection with some bits missing so that we can check access
        connection = Connection(
            postgres_pool=SMConnections.get_postgres_pool(),
            author=author,
            project=None,
            project_id_map={},
            project_name_map={},
            on_behalf_of=on_behalf_of,
            ar_guid=ar_guid,
            meta=meta,
        )

        await connection.refresh_projects()
        connection.update_project(project_name)

        connection.check_access(allowed_roles)

        return connection

    @staticmethod
    async def get_connection_no_project(
        author: str, ar_guid: str, meta: dict[str, str], on_behalf_of: str | None
    ):
        """Get a db connection from a project and user"""
        # maybe it makes sense to perform permission checks here too
        logger.debug(f'Authenticate no-project connection with {author!r}')

        connection = Connection(
            postgres_pool=SMConnections.get_postgres_pool(),
            author=author,
            project=None,
            on_behalf_of=on_behalf_of,
            ar_guid=ar_guid,
            meta=meta,
            project_id_map={},
            project_name_map={},
        )

        await connection.refresh_projects()
        return connection
