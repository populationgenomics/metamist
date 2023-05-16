# pylint: disable=global-statement
import asyncio
from typing import Dict, List, Set, Iterable, Optional, Tuple, Any

import json
from datetime import datetime, timedelta

from databases import Database
from google.cloud import secretmanager
from cpg_utils.cloud import get_cached_group_members

from api.settings import MEMBERS_CACHE_LOCATION, is_all_access
from db.python.utils import (
    ProjectId,
    Forbidden,
    NoProjectAccess,
    get_logger,
    to_db_json,
    InternalError,
)
from models.models.project import Project

# minutes
PROJECT_CACHE_LENGTH = 1
PERMISSIONS_CACHE_LENGTH = 1


logger = get_logger()


class ProjectPermissionCacheObject:
    """
    Project permissions object to cache,
    with expiry and 'is_valid' value
    """

    def __init__(self, users: Set[str], expiry=None):
        self.users = set(users or [])
        self.expiry = expiry or (
            datetime.utcnow() + timedelta(minutes=PERMISSIONS_CACHE_LENGTH)
        )

    def is_valid(self):
        """Is the value still valid, or has it expired"""
        return datetime.utcnow() < self.expiry


class ProjectPermissionsTable:
    """
    Capture project operations and queries
    """

    table_name = 'project'
    _cached_client = None

    _cache_expiry = None
    _cached_project_names: Dict[str, ProjectId] = {}
    _cached_project_by_id: Dict[ProjectId, Project] = {}

    _cached_permissions: Dict[Tuple[ProjectId, bool], ProjectPermissionCacheObject] = {}

    def __init__(self, connection: Database, allow_full_access=None):
        if not isinstance(connection, Database):
            raise ValueError(
                f'Invalid type connection, expected Database, got {type(connection)}, did you forget to call connection.connection?'
            )
        self.connection: Database = connection
        self.allow_full_access = (
            allow_full_access if allow_full_access is not None else is_all_access()
        )

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
        spids = list(set(project_ids))
        # do this all at once to save time
        promises = [
            self.check_access_to_project_id(
                user, project_id, readonly=readonly, raise_exception=False
            )
            for project_id in spids
        ]
        has_access_map = await asyncio.gather(*promises)

        missing_project_ids = [
            project_id
            for project_id, has_access in zip(spids, has_access_map)
            if not has_access
        ]

        if missing_project_ids:
            if raise_exception:
                project_map = await self.get_project_id_map()
                missing_project_names = [
                    project_map[pid].name for pid in missing_project_ids
                ]
                raise NoProjectAccess(
                    missing_project_names, readonly=readonly, author=user
                )
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
        try:
            users = await self.get_allowed_users_for_project_id(
                project_id, readonly=readonly
            )
        except Exception as e:  # pylint: disable=broad-except
            if raise_exception:
                raise e

            return False

        has_access = users is None or user in users
        if not has_access and raise_exception:
            project_name = (await self.get_project_id_map())[project_id].name
            raise NoProjectAccess([project_name], readonly=readonly, author=user)
        return has_access

    async def get_allowed_users_for_project_id(
        self, project_id, readonly: bool
    ) -> Optional[Set[str]]:
        """Get allowed users for a project_id"""
        cache_key = (project_id, readonly)
        if (
            cache_key not in self._cached_permissions
            or not self._cached_permissions[cache_key].is_valid()
        ):
            project_id_map = await self.get_project_id_map()
            project = project_id_map[project_id]
            group_name = (
                project.read_group_name if readonly else project.write_group_name
            )
            if group_name is None:
                project_name = (await self.get_project_id_map())[project_id].name
                read_or_write = 'read' if readonly else 'write'
                raise InternalError(
                    f'An internal error occurred when validating access to {project_name}, '
                    f'there must be a value in the DB for "{read_or_write}_secret_name" to lookup'
                )

            try:
                assert (
                    MEMBERS_CACHE_LOCATION is not None
                ), 'Requires "SM_MEMBERS_CACHE_LOCATION" to be set'
                users = get_cached_group_members(
                    group_name, members_cache_location=MEMBERS_CACHE_LOCATION
                )

            except Exception as e:
                raise type(e)(
                    f'An error occurred when determining access to this project: {e}'
                ) from e

            self._cached_permissions[cache_key] = ProjectPermissionCacheObject(
                users=users
            )

        return self._cached_permissions[cache_key].users

    async def ensure_project_id_cache_is_filled(self):
        """(CACHED) Get map of project names to project IDs"""
        if (
            not ProjectPermissionsTable._cached_project_names
            or ProjectPermissionsTable._cache_expiry < datetime.utcnow()
        ):
            project_rows = await self.get_project_rows(check_permissions=False)
            ProjectPermissionsTable._cached_project_by_id = {
                p.id: p for p in project_rows
            }
            ProjectPermissionsTable._cached_project_names = {
                p.name: p.id for p in project_rows
            }
            ProjectPermissionsTable._cache_expiry = datetime.utcnow() + timedelta(
                minutes=1
            )

    async def get_project_id_map(self) -> Dict[int, Project]:
        """Get {project_id: ProjectRow} map"""
        await self.ensure_project_id_cache_is_filled()
        return ProjectPermissionsTable._cached_project_by_id

    async def get_project_name_map(self) -> Dict[str, int]:
        """Get {project_name: project_id} map"""
        await self.ensure_project_id_cache_is_filled()
        return ProjectPermissionsTable._cached_project_names

    async def get_project_id_map_for_names(self, project_names, author, readonly: bool, check_access=True) -> dict[str, ProjectId]:
        m = await self.get_project_name_map()
        project_name_map = {name: m[name] for name in project_names}
        if check_access:
            await self.check_access_to_project_ids(user=author, project_ids=project_name_map.values(), readonly=readonly)

        return project_name_map

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
            raise InternalError('An internal error occurred during authorization')

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
            raise NoProjectAccess(invalid_project_names, readonly=readonly, author=user)

        if len(project_ids) != len(project_names):
            raise InternalError(
                'An internal error occurred when mapping project names to IDs'
            )

        return project_ids

    async def check_project_creator_permissions(self, author):
        """Check author has project_creator permissions"""
        # check permissions in here
        if self.allow_full_access:
            return True

        response = self._read_secret('sample-metadata', 'project-creator-users')
        if author not in set(response.split(',')):
            raise Forbidden(f'{author} does not have access to creating project')

        return True

    async def get_project_rows(
        self, author: Optional[str] = None, check_permissions=True
    ) -> List[Project]:
        """Get {name: id} project map"""
        if check_permissions:
            await self.check_project_creator_permissions(author)

        _query = 'SELECT id, name, meta, dataset, read_group_name, write_group_name FROM project'
        rows = await self.connection.fetch_all(_query)
        return list(map(Project.from_db, rows))

    async def get_project_by_id(self, project_id: ProjectId) -> Project:
        """Get Project from id, NO auth checks are performed"""
        if not project_id:
            raise ValueError('Project ID is required for get_project_by_id')
        _query = """
        SELECT id, name, meta, dataset, read_group_name, write_group_name
        FROM project
        WHERE id = :project_id
        """
        row = await self.connection.fetch_one(_query, {'project_id': project_id})
        if not row:
            raise ValueError('No project found')
        return Project.from_db(row)

    async def get_projects_accessible_by_user(
        self, author: str, readonly=True
    ) -> dict[int, str]:
        """
        Get projects that are accessible by the specified user
        """
        assert author

        _query = 'SELECT id, name FROM project'
        project_id_map = {p[0]: p[1] for p in await self.connection.fetch_all(_query)}

        promises = [
            self.check_access_to_project_id(
                author, pid, readonly=readonly, raise_exception=False
            )
            for pid in project_id_map.keys()
        ]
        has_access_to_project = await asyncio.gather(*promises)
        relevant_project_map = {
            pid: name
            for (pid, name), has_access in zip(
                project_id_map.items(), has_access_to_project
            )
            if has_access
        }

        return relevant_project_map

    async def get_projects_by_ids(self, project_ids: list[ProjectId]) -> list[Project]:
        """
        Get projects by IDs, NO authorization is performed here
        """
        _query = """
        SELECT id, name, meta, dataset, read_group_name, write_group_name
        FROM project
        WHERE id IN :project_ids
        """
        rows = await self.connection.fetch_all(_query, {'project_ids': project_ids})
        projects = list(map(Project.from_db, rows))
        if len(project_ids) != len(rows):
            missing_projects = set(project_ids) - set(p.id for p in projects)
            raise ValueError(f'Some projects were not found: {missing_projects}')

        return projects

    async def create_project(
        self,
        project_name: str,
        dataset_name: str,
        author: str,
        read_group_name: str,
        write_group_name: str,
        create_test_project: bool,
        check_permissions=True,
    ):
        """Create project row"""
        if check_permissions:
            await self.check_project_creator_permissions(author)

        _query = """\
INSERT INTO project (name, dataset, author, read_group_name, write_group_name)
VALUES (:name, :dataset, :author, :read_group_name, :write_group_name)
RETURNING ID"""
        values = {
            'name': project_name,
            'dataset': dataset_name,
            'author': author,
            'read_group_name': read_group_name,
            'write_group_name': write_group_name,
        }

        project_id = await self.connection.fetch_val(_query, values)

        if create_test_project:
            values = {
                'name': project_name + '-test',
                'dataset': dataset_name,
                'author': author,
                'read_group_name': read_group_name.replace('-main-', '-test-'),
                'write_group_name': write_group_name.replace('-main-', '-test-'),
            }

            project_id = await self.connection.fetch_val(_query, values)

        return project_id

    async def update_project(self, project_name: str, update: dict, author: str):
        """Update a sample-metadata project"""
        await self.check_project_creator_permissions(author)

        meta = update.get('meta')

        fields: Dict[str, Any] = {'author': author, 'name': project_name}

        setters = ['author = :author']

        if meta is not None and len(meta) > 0:
            fields['meta'] = to_db_json(meta)
            setters.append('meta = JSON_MERGE_PATCH(COALESCE(meta, "{}"), :meta)')

        fields_str = ', '.join(setters)

        _query = f'UPDATE project SET {fields_str} WHERE name = :name'

        return await self.connection.execute(_query, fields)

    async def get_seqr_projects(self) -> list[dict[str, Any]]:
        """
        Get all projects with meta.is_seqr = true
        """
        _query = """\
        SELECT id, name, dataset, meta FROM project
        WHERE json_extract(meta, '$.is_seqr') = true
        """

        projects = []
        for r in await self.connection.fetch_all(_query):
            r = dict(r)
            r['meta'] = json.loads(r['meta'] or '{}')
            projects.append(r)

        return projects

    async def delete_project_data(
        self, project_id: int, delete_project: bool, author: str
    ) -> bool:
        """
        Delete data in metamist project, requires project_creator_permissions
        Can optionally delete the project also.
        """
        await self.check_project_creator_permissions(author)

        async with self.connection.transaction():
            _query = """
DELETE FROM participant_phenotypes where participant_id IN (
    SELECT id FROM participant WHERE project = :project
);
DELETE FROM family_participant WHERE family_id IN (
    SELECT id FROM family where project = :project
);
DELETE FROM family WHERE project = :project;
DELETE FROM sequencing_group_external_id WHERE project = :project;
DELETE FROM assay_external_id WHERE project = :project;
DELETE FROM sequencing_group_assay WHERE sequencing_group_id IN (
    SELECT sg.id FROM sequencing_group sg
    INNER JOIN sample ON sample.id = sg.sample_id
    WHERE sample.project = :project
);
DELETE FROM analysis_sequencing_group WHERE sequencing_group_id in (
    SELECT sg.id FROM sequencing_group sg
    INNER JOIN sample ON sample.id = sg.sample_id
    WHERE sample.project = :project
);
DELETE FROM assay WHERE sample_id in (SELECT id FROM sample WHERE project = :project);
DELETE FROM sequencing_group WHERE sample_id IN (
    SELECT id FROM sample WHERE project = :project
);
DELETE FROM sample WHERE project = :project;
DELETE FROM participant WHERE project = :project;
DELETE FROM analysis WHERE project = :project;
            """
            if delete_project:
                _query += 'DELETE FROM project WHERE id = :project;\n'

            await self.connection.execute(_query, {'project': project_id})

        return True
