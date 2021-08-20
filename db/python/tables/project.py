import os
from datetime import datetime, timedelta
from typing import Dict, List, Set, Iterable

from databases import Database
from google.cloud import secretmanager

# minutes
from models.models.project import ProjectRow

PROJECT_CACHE_LENGTH = 1
PERMISSIONS_CACHE_LENGTH = 1
IS_DEVELOPMENT = 'dev' in os.getenv('SM_ENVIRONMENT', 'development').lower()

ProjectId = int


class Forbidden(Exception):
    """Forbidden action"""


class ProjectDoesNotExist(Forbidden):
    """Custom error for ProjectDoesNotExist"""

    def __init__(self, project_name, *args: object) -> None:
        super().__init__(
            f'Project with id "{project_name}" does not exist, '
            'or you do not have the appropriate permissions',
            *args,
        )


class NoProjectAccess(Forbidden):
    """Not allowed access to a project (or not allowed project-less access)"""

    def __init__(self, project_names: Iterable[str], *args):
        project_names_str = ', '.join(project_names)
        super().__init__(
            'You do not have access to resources from the '
            f'following project(s): {project_names_str}',
            *args,
        )


class ProjectPermissionCacheObject:
    """
    Project permissions object to cache,
    with expiry and 'is_valid' value
    """

    def __init__(self, users: Set[str], expiry=None):
        self.users = users
        self.expiry = expiry or (
            datetime.utcnow() + timedelta(minutes=PERMISSIONS_CACHE_LENGTH)
        )

    def is_valid(self):
        """Is the value still valid, or has it expired"""
        return self.expiry < datetime.utcnow()


class ProjectPermissionsTable:
    """
    Capture project operations and queries
    """

    table_name = 'project'
    _cached_client = None

    _cache_expiry = None
    _cached_project_names: Dict[str, ProjectId] = None
    _cached_project_by_id: Dict[ProjectId, ProjectRow] = None

    _cached_permissions: Dict[ProjectId, ProjectPermissionCacheObject] = {}

    def __init__(self, connection: Database, allow_full_access=IS_DEVELOPMENT):

        self.connection: Database = connection
        self.allow_full_access = allow_full_access

    def _get_secret_manager_client(self):
        if not self._cached_client:
            self._cached_client = secretmanager.SecretManagerServiceClient()
        return self._cached_client

    def _read_secret(self, project_id: str, secret_name: str):
        """Reads the latest version of a GCP Secret Manager secret.
        Returns None if the secret doesn't exist."""

        secret_manager = self._get_secret_manager_client()
        secret_path = secret_manager.secret_path(project_id, secret_name)

        response = secret_manager.access_secret_version(
            request={'name': f'{secret_path}/versions/latest'}
        )

        return response.payload.data.decode('UTF-8')

    async def check_access_to_project_ids(
        self,
        user: str,
        project_ids: Iterable[ProjectId],
        readonly: bool,
        raise_exception=True,
    ) -> bool:
        """Check user has access to list of project_ids"""
        if not project_ids:
            raise Forbidden(
                "You don't have access to this resources, as the resource you requested didn't belong to a project"
            )
        if self.allow_full_access:
            return True
        missing_project_ids = []
        for project_id in set(project_ids):
            has_access = await self.check_access_to_project_id(
                user, project_id, readonly=readonly, raise_exception=False
            )
            if not has_access:
                missing_project_ids.append(project_id)

        if missing_project_ids:
            if raise_exception:
                project_map = await self.get_project_id_map()
                missing_project_names = [
                    project_map[pid].name for pid in missing_project_ids
                ]
                raise NoProjectAccess(missing_project_names)
            return False

        return True

    async def check_access_to_project_id(
        self, user: str, project_id: ProjectId, readonly: bool, raise_exception=True
    ) -> bool:
        """Check whether a user has access to project_id"""
        if self.allow_full_access:
            return True
        if not readonly:
            # validate write privileges here connection
            pass
        users = await self.get_allowed_users_for_project_id(project_id)
        has_access = user in users
        if not has_access and raise_exception:
            project_name = (await self.get_project_id_map())[project_id].name
            raise NoProjectAccess([project_name])
        return has_access

    async def get_allowed_users_for_project_id(self, project_id) -> Set[str]:
        """Get allowed users for a project_id"""
        if (
            project_id not in self._cached_permissions
            or not self._cached_permissions[project_id].is_valid()
        ):
            project_id_map = await self.get_project_id_map()
            project = project_id_map[project_id]
            response = self._read_secret(
                project.gcp_id, f'{project.dataset}-access-members-cache'
            )
            users = set(response.split(','))
            self._cached_permissions[project_id] = ProjectPermissionCacheObject(
                users=users
            )

        return self._cached_permissions[project_id].users

    async def ensure_project_id_cache_is_filled(self):
        """(CACHED) Get map of project names to project IDs"""
        if (
            not ProjectPermissionsTable._cached_project_names
            or ProjectPermissionsTable._cache_expiry < datetime.utcnow()
        ):
            project_rows = await self.get_project_rows()
            ProjectPermissionsTable._cached_project_by_id = {
                p.id: p for p in project_rows
            }
            ProjectPermissionsTable._cached_project_names = {
                p.name: p.id for p in project_rows
            }
            ProjectPermissionsTable._cache_expiry = datetime.utcnow() + timedelta(
                minutes=1
            )

    async def get_project_id_map(self) -> Dict[int, ProjectRow]:
        """Get {project_id: ProjectRow} map"""
        await self.ensure_project_id_cache_is_filled()
        return ProjectPermissionsTable._cached_project_by_id

    async def get_project_name_map(self) -> Dict[str, int]:
        """Get {project_name: project_id} map"""
        await self.ensure_project_id_cache_is_filled()
        return ProjectPermissionsTable._cached_project_names

    async def get_project_id_from_name_and_user(
        self, user: str, project_name: str, readonly: bool
    ) -> ProjectId:
        """
        Get projectId from project name and user (email address)
        Returns:
            - int: if user has access to specific project[SampleSequencing.from_db(s) for s in sequence_dicts]
            - None: if user has <no-project> access
            - False if unable to access the specified project
        """
        project_ids = await self.get_project_ids_from_names_and_user(
            user, [project_name], readonly=readonly
        )
        return project_ids[0]

    async def get_project_ids_from_names_and_user(
        self, user: str, project_names: List[str], readonly: bool
    ) -> List[ProjectId]:
        """Get project ids from project names and the user"""
        if not user:
            raise Exception('An internal error occurred during authorization')

        project_name_map = await self.get_project_name_map()
        project_ids = []
        invalid_project_names = []
        for project_name in project_names:
            project_id = project_name_map.get(project_name)
            if not project_id:
                invalid_project_names.append(project_name)
                continue

            can_use = await self.check_access_to_project_id(
                user, project_id, readonly=readonly
            )
            if not can_use:
                invalid_project_names.append(project_name)
                continue

            project_ids.append(project_id)

        if invalid_project_names:
            raise NoProjectAccess(invalid_project_names)

        if len(project_ids) != len(project_names):
            raise Exception(
                'An internal error occurred when mapping project names to IDs'
            )

        return project_ids

    async def get_project_rows(self) -> List[ProjectRow]:
        """Get {name: id} project map"""
        _query = 'SELECT id, name, gcp_id, dataset FROM project'
        rows = await self.connection.fetch_all(_query)
        return list(map(ProjectRow.from_db, rows))

    async def create_project(
        self, project_name: str, dataset_name: str, gcp_project_id: str, author: str
    ):
        """Create project row"""
        # check permissions in here
        if not self.allow_full_access:
            response = self._read_secret('sample-metadata', 'project-creator-users')
            if author not in set(response.split(',')):
                raise Forbidden(f'{author} does not have access to creating project')

        _query = """\
INSERT INTO project (name, gcp_id, dataset, author)
VALUES (:name, :gcp_id, :dataset, :author)
RETURNING ID"""
        values = {
            'name': project_name,
            'dataset': dataset_name,
            'gcp_id': gcp_project_id,
            'author': author,
        }

        project_id = await self.connection.fetch_val(_query, values)
        return project_id
