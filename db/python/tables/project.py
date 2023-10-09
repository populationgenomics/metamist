# pylint: disable=global-statement
from typing import Any, Dict, Iterable, List

from async_lru import alru_cache
from databases import Database

from api.settings import is_all_access
from db.python.utils import (
    Forbidden,
    InternalError,
    NoProjectAccess,
    NotFoundError,
    ProjectId,
    get_logger,
    to_db_json,
)
from models.models.project import Project

logger = get_logger()

GROUP_NAME_PROJECT_CREATORS = 'project-creators'
GROUP_NAME_MEMBERS_ADMIN = 'members-admin'


class ProjectPermissionsTable:
    """
    Capture project operations and queries
    """

    table_name = 'project'

    @staticmethod
    def get_project_group_name(project_name: str, readonly: bool) -> str:
        """
        Get group name for a project, for readonly / write
        """
        if readonly:
            return f'{project_name}-read'
        return f'{project_name}-write'

    def __init__(self, connection: Database, allow_full_access=None):
        if not isinstance(connection, Database):
            raise ValueError(
                f'Invalid type connection, expected Database, got {type(connection)}, '
                'did you forget to call connection.connection?'
            )
        self.connection: Database = connection
        self.gtable = GroupTable(connection, allow_full_access=allow_full_access)

    # region UNPROTECTED_GETS

    @alru_cache()
    async def _get_project_rows_internal(self):
        """
        Internally cached get_project_rows
        """
        _query = """
        SELECT id, name, meta, dataset, read_group_id, write_group_id
        FROM project
        """
        rows = await self.connection.fetch_all(_query)
        return list(map(Project.from_db, rows))

    async def _get_project_id_map(self):
        """
        Internally cached get_project_id_map
        """
        return {p.id: p for p in await self._get_project_rows_internal()}

    async def _get_project_name_map(self) -> Dict[str, int]:
        """Get {project_name: project_id} map"""
        return {p.name: p.id for p in await self._get_project_rows_internal()}

    async def _get_project_by_id(self, project_id: ProjectId) -> Project:
        """Get project by id"""
        pmap = await self._get_project_id_map()
        if project_id not in pmap:
            raise NotFoundError(f'Could not find project {project_id}')
        return pmap[project_id]

    async def _get_project_by_name(self, project_name: str) -> Project:
        """Get project by name"""
        pmap = await self._get_project_name_map()
        if project_name not in pmap:
            raise NotFoundError(f'Could not find project {project_name}')
        return await self._get_project_by_id(pmap[project_name])

    async def _get_projects_by_ids(
        self, project_ids: Iterable[ProjectId]
    ) -> List[Project]:
        """Get projects by ids"""
        pids = set(project_ids)
        pmap = await self._get_project_id_map()
        missing_pids = pids - set(pmap.keys())
        if missing_pids:
            raise NotFoundError(f'Could not find projects {missing_pids}')
        return [pmap[pid] for pid in pids]

    # endregion UNPROTECTED_GETS

    # region AUTH

    async def get_all_projects(self, author: str):
        """Get all projects"""
        await self.check_project_creator_permissions(author)
        return await self._get_project_rows_internal()

    async def get_projects_accessible_by_user(
        self, author: str, readonly=True
    ) -> list[Project]:
        """
        Get projects that are accessible by the specified user
        """
        assert author

        group_name = 'read_group_id' if readonly else 'write_group_id'
        _query = f"""
            SELECT p.id
            FROM project p
            INNER JOIN group_member gm ON gm.group_id = p.{group_name}
            WHERE gm.member = :author
        """
        relevant_project_ids = await self.connection.fetch_all(
            _query, {'author': author}
        )
        projects = await self._get_projects_by_ids(relevant_project_ids)

        return projects

    async def get_and_check_access_to_project_for_id(
        self, user: str, project_id: ProjectId, readonly: bool
    ) -> Project:
        """Get project by id"""
        project = await self._get_project_by_id(project_id)
        has_access = await self.gtable.check_if_member_in_group(
            group_id=project.read_group_id if readonly else project.write_group_id,
            member=user,
        )
        if not has_access:
            raise NoProjectAccess([project.name], readonly=readonly, author=user)

        return project

    async def get_and_check_access_to_project_for_name(
        self, user: str, project_name: str, readonly: bool
    ) -> Project:
        """Get project by name + perform access checks"""
        project = await self._get_project_by_name(project_name)
        has_access = await self.gtable.check_if_member_in_group(
            group_id=project.read_group_id if readonly else project.write_group_id,
            member=user,
        )
        if not has_access:
            raise NoProjectAccess([project.name], readonly=readonly, author=user)

        return project

    async def get_and_check_access_to_projects_for_names(
        self, user: str, project_names: list[str], readonly: bool
    ):
        """Get projects by names + perform access checks"""
        project_name_map = await self._get_project_name_map()

        # check missing_projects
        missing_project_names = set(project_names) - set(project_name_map.keys())
        if missing_project_names:
            raise NotFoundError(
                f'Could not find projects {", ".join(missing_project_names)}'
            )

        projects = await self.get_and_check_access_to_projects_for_ids(
            user=user,
            project_ids=[project_name_map[name] for name in project_names],
            readonly=readonly,
        )

        return projects

    async def get_and_check_access_to_projects_for_ids(
        self, user: str, project_ids: list[ProjectId], readonly: bool
    ) -> list[Project]:
        """Get project by id"""
        if not project_ids:
            raise Forbidden(
                "You don't have access to this resources, as the resource you "
                "requested didn't belong to a project"
            )

        projects = await self._get_projects_by_ids(project_ids)
        # do this all at once to save time
        if readonly:
            group_id_project_map = {p.read_group_id: p for p in projects}
        else:
            group_id_project_map = {p.write_group_id: p for p in projects}

        group_ids = set(gid for gid in group_id_project_map.keys() if gid)
        # awkward double negative to find missing projects
        missing_group_ids = await self.gtable.check_which_groups_member_is_missing(
            group_ids=group_ids, member=user
        )

        if missing_group_ids:
            # so we can directly return the project names they're missing
            missing_project_names = [
                group_id_project_map[gid].name or str(gid) for gid in missing_group_ids
            ]
            raise NoProjectAccess(missing_project_names, readonly=readonly, author=user)

        return projects

    async def check_access_to_project_id(
        self, user: str, project_id: ProjectId, readonly: bool, raise_exception=True
    ) -> bool:
        """Check whether a user has access to project_id"""
        project = await self._get_project_by_id(project_id)
        has_access = await self.gtable.check_if_member_in_group(
            group_id=project.read_group_id if readonly else project.write_group_id,
            member=user,
        )
        if not has_access and raise_exception:
            raise NoProjectAccess([project.name], readonly=readonly, author=user)

        return has_access

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
                "You don't have access to this resources, as the resource you "
                "requested didn't belong to a project"
            )

        projects = await self._get_projects_by_ids(project_ids)
        # do this all at once to save time
        if readonly:
            group_id_project_map = {p.read_group_id: p for p in projects}
        else:
            group_id_project_map = {p.write_group_id: p for p in projects}

        group_ids = set(gid for gid in group_id_project_map.keys() if gid)
        # awkward double negative to find missing projects
        missing_group_ids = await self.gtable.check_which_groups_member_is_missing(
            group_ids=group_ids, member=user
        )

        if missing_group_ids:
            # so we can directly return the project names they're missing
            missing_project_names = [
                group_id_project_map[gid].name or str(gid) for gid in missing_group_ids
            ]
            if raise_exception:
                raise NoProjectAccess(
                    missing_project_names, readonly=readonly, author=user
                )
            return False

        return True

    async def check_project_creator_permissions(self, author):
        """Check author has project_creator permissions"""
        # check permissions in here
        is_in_group = await self.gtable.check_if_member_in_group_name(
            group_name=GROUP_NAME_PROJECT_CREATORS, member=author
        )

        if not is_in_group:
            raise Forbidden(f'{author} does not have access to creating project')

        return True

    # endregion AUTH

    async def get_project_ids_from_names_and_user(
        self, user: str, project_names: List[str], readonly: bool
    ) -> List[ProjectId]:
        """Get project ids from project names and the user"""
        if not user:
            raise InternalError('An internal error occurred during authorization')

        project_name_map = await self._get_project_name_map()
        ordered_project_ids = [project_name_map[name] for name in project_names]
        await self.check_access_to_project_ids(
            user, ordered_project_ids, readonly=readonly, raise_exception=True
        )

        return ordered_project_ids

    # region CREATE / UPDATE

    async def create_project(
        self,
        project_name: str,
        dataset_name: str,
        author: str,
        check_permissions=True,
    ):
        """Create project row"""
        if check_permissions:
            await self.check_project_creator_permissions(author)

        async with self.connection.transaction():
            read_group_id = await self.gtable.create_group(
                self.get_project_group_name(project_name, readonly=True)
            )
            write_group_id = await self.gtable.create_group(
                self.get_project_group_name(project_name, readonly=False)
            )

            _query = """\
    INSERT INTO project (name, dataset, author, read_group_id, write_group_id)
    VALUES (:name, :dataset, :author, :read_group_id, :write_group_id)
    RETURNING ID"""
            values = {
                'name': project_name,
                'dataset': dataset_name,
                'author': author,
                'read_group_id': read_group_id,
                'write_group_id': write_group_id,
            }

            project_id = await self.connection.fetch_val(_query, values)

        # pylint: disable=no-member
        self._get_project_rows_internal.cache_invalidate()

        return project_id

    async def update_project(self, project_name: str, update: dict, author: str):
        """Update a metamist project"""
        await self.check_project_creator_permissions(author)

        meta = update.get('meta')

        fields: Dict[str, Any] = {'author': author, 'name': project_name}

        setters = ['author = :author']

        if meta is not None and len(meta) > 0:
            fields['meta'] = to_db_json(meta)
            setters.append('meta = JSON_MERGE_PATCH(COALESCE(meta, "{}"), :meta)')

        fields_str = ', '.join(setters)

        _query = f'UPDATE project SET {fields_str} WHERE name = :name'

        await self.connection.execute(_query, fields)

        # pylint: disable=no-member
        self._get_project_rows_internal.cache_invalidate()

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
            values: dict = {'project': project_id}
            if delete_project:
                group_ids = await self.connection.fetch_one(
                    """
                    SELECT read_group_id, write_group_id
                    FROM project WHERE id = :project'
                    """
                )
                _query += 'DELETE FROM project WHERE id = :project;\n'
                _query += 'DELETE FROM `group` WHERE id IN :group_ids\n'
                values['group_ids'] = [
                    group_ids['read_group_id'],
                    group_ids['write_group_id'],
                ]

            await self.connection.execute(_query, {'project': project_id})

        if delete_project:
            # pylint: disable=no-member
            self._get_project_rows_internal.cache_invalidate()

        return True

    async def set_group_members(self, group_name: str, members: list[str], author: str):
        """
        Set group members for a group (by name)
        """

        has_permission = await self.gtable.check_if_member_in_group_name(
            GROUP_NAME_MEMBERS_ADMIN, author
        )
        if not has_permission:
            raise Forbidden(
                f'User {author} does not have permission to add members to group {group_name}'
            )
        group_id = await self.gtable.get_group_name_from_id(group_name)
        await self.gtable.set_group_members(group_id, members, author=author)

    # endregion CREATE / UPDATE

    async def get_seqr_projects(self) -> list[dict[str, Any]]:
        """
        Get all projects with meta.is_seqr = true
        """

        all_projects = await self._get_project_rows_internal()
        seqr_projects = [p for p in all_projects if p.meta.get('is_seqr')]
        return seqr_projects

    # gruo


class GroupTable:
    """
    Capture Analysis table operations and queries
    """

    table_name = 'group'

    def __init__(self, connection: Database, allow_full_access: bool = None):
        if not isinstance(connection, Database):
            raise ValueError(
                f'Invalid type connection, expected Database, got {type(connection)}, '
                'did you forget to call connection.connection?'
            )
        self.connection: Database = connection
        self.allow_full_access = (
            allow_full_access if allow_full_access is not None else is_all_access()
        )

    async def get_group_members(self, group_id: int) -> set[str]:
        """Get project IDs for sampleIds (mostly for checking auth)"""
        _query = """
            SELECT member
            FROM `group`
            WHERE id = :group_id
        """
        rows = await self.connection.fetch_all(_query, {'group_id': group_id})
        members = set(r['member'] for r in rows)
        return members

    async def get_group_name_from_id(self, name: str) -> int:
        """Get group name to group id"""
        _query = """
            SELECT id, name
            FROM `group`
            WHERE name = :name
        """
        row = await self.connection.fetch_one(_query, {'name': name})
        if not row:
            raise NotFoundError(f'Could not find group {name}')
        return row['id']

    async def get_group_name_to_ids(self, names: list[str]) -> dict[str, int]:
        """Get group name to group id"""
        _query = """
            SELECT id, name
            FROM `group`
            WHERE name IN :names
        """
        rows = await self.connection.fetch_all(_query, {'names': names})
        return {r['name']: r['id'] for r in rows}

    async def check_if_member_in_group(self, group_id: int, member: str) -> bool:
        """Check if a member is in a group"""
        if self.allow_full_access:
            return True

        _query = """
            SELECT EXISTS (
                SELECT 1
                FROM group_member gm
                WHERE gm.group_id = :group_id
                AND gm.member = :member
            )
        """
        value = await self.connection.fetch_val(
            _query, {'group_id': group_id, 'member': member}
        )
        return bool(ord(value))

    async def check_if_member_in_group_name(self, group_name: str, member: str) -> bool:
        """Check if a member is in a group"""
        if self.allow_full_access:
            return True

        _query = """
            SELECT COUNT(*) > 1
            FROM group_member gm
            INNER JOIN `group` g ON g.id = gm.group_id
            WHERE g.name = :group_name
            AND gm.member = :member
        """
        value = await self.connection.fetch_val(
            _query, {'group_name': group_name, 'member': member}
        )
        # return len(value) > 0
        return bool(ord(value))

    async def check_which_groups_member_is_missing(
        self, group_ids: set[int], member: str
    ) -> set[int]:
        """Check if a member is in a group"""
        if self.allow_full_access:
            return set()

        _query = """
            SELECT g.group_id
            FROM group_member gm
            WHERE gm.member = member AND gm.group_id IN :group_ids
        """
        membered_group_ids = await self.connection.fetch_all(
            _query, {'group_ids': group_ids, 'member': member}
        )
        return set(group_ids) - set(membered_group_ids)

    async def create_group(self, name: str) -> int:
        """Create a new group"""
        _query = """
            INSERT INTO `group` (name)
            VALUES (:name)
            RETURNING id
        """
        return await self.connection.fetch_val(_query, {'name': name})

    async def set_group_members(self, group_id: int, members: list[str], author: str):
        """
        Set group members for a group (by id)
        """
        async with self.connection.transaction():
            await self.connection.execute(
                """
                DELETE FROM group_member
                WHERE group_id = :group_id
                """,
                {'group_id': group_id},
            )
            await self.connection.execute_many(
                """
                INSERT INTO group_member (group_id, member, author)
                VALUES (:group_id, :member, :author)
                """,
                [
                    {'group_id': group_id, 'member': member, 'author': author}
                    for member in members
                ],
            )
