# pylint: disable=global-statement
from typing import TYPE_CHECKING, Any

from databases import Database
from typing_extensions import TypedDict

from api.settings import is_all_access
from db.python.utils import Forbidden, NotFoundError, get_logger, to_db_json
from models.models.project import Project, ProjectMemberRole

# Avoid circular import for type definition
if TYPE_CHECKING:
    from db.python.connect import Connection
else:
    Connection = object

logger = get_logger()

GROUP_NAME_PROJECT_CREATORS = 'project-creators'
GROUP_NAME_MEMBERS_ADMIN = 'members-admin'


class ProjectMemberWithRole(TypedDict):
    """Dict passed to the update project member endpoint to specify roles for members"""

    member: str
    role: str


class ProjectPermissionsTable:
    """
    Capture project operations and queries
    """

    table_name = 'project'

    def __init__(
        self,
        connection: Connection | None,
        allow_full_access: bool | None = None,
        database_connection: Database | None = None,
    ):
        self._connection = connection
        if not database_connection:
            if not connection:
                raise ValueError(
                    'Must call project permissions table with either a direct '
                    'database_connection or a fully formed connection'
                )
            self.connection = connection.connection
        else:
            self.connection = database_connection

        self.gtable = GroupTable(self.connection, allow_full_access=allow_full_access)

    async def audit_log_id(self):
        """
        Generate (or return) a audit_log_id by inserting a row into the database
        """
        if not self._connection:
            raise ValueError(
                'Cannot call audit_log_id without a fully formed connection'
            )
        return await self._connection.audit_log_id()

    # region AUTH
    async def get_projects_accessible_by_user(
        self, user: str, return_all_projects: bool = False
    ) -> tuple[dict[int, Project], dict[str, Project]]:
        """
        Get projects that are accessible by the specified user
        """
        parameters: dict[str, str] = {
            'user': user,
        }

        # In most cases we want to exclude projects that the user doesn't explicitly
        # have access to. If the user is in the project creators group it may be
        # necessary to return all projects whether the user has explict access to them
        # or not.
        where_cond = 'WHERE pm.member = :user' if return_all_projects is False else ''

        _query = f"""
            SELECT
                p.id,
                p.name,
                p.meta,
                p.dataset,
                GROUP_CONCAT(pm.role) as roles
            FROM project p
            LEFT JOIN project_member pm
            ON p.id = pm.project_id
            AND pm.member = :user
            {where_cond}
            GROUP BY p.id
        """

        user_projects = await self.connection.fetch_all(_query, parameters)

        project_id_map: dict[int, Project] = {}
        project_name_map: dict[str, Project] = {}

        for row in user_projects:
            project = Project.from_db(dict(row))
            project_id_map[row['id']] = project
            project_name_map[row['name']] = project

        return project_id_map, project_name_map

    async def check_project_creator_permissions(self, author: str):
        """Check author has project_creator permissions"""
        # check permissions in here
        is_in_group = await self.gtable.check_if_member_in_group_name(
            group_name=GROUP_NAME_PROJECT_CREATORS, member=author
        )

        if not is_in_group:
            raise Forbidden(f'{author} does not have access to creating project')

        return True

    async def check_member_admin_permissions(self, author: str):
        """Check author has member_admin permissions"""
        # check permissions in here
        is_in_group = await self.gtable.check_if_member_in_group_name(
            GROUP_NAME_MEMBERS_ADMIN, author
        )
        if not is_in_group:
            raise Forbidden(
                f'User {author} does not have permission to edit project members'
            )

        return True

    # endregion AUTH

    # region CREATE / UPDATE

    async def create_project(
        self,
        project_name: str,
        dataset_name: str,
        author: str,
        check_permissions: bool = True,
    ):
        """Create project row"""
        if check_permissions:
            await self.check_project_creator_permissions(author)

        async with self.connection.transaction():
            _query = """\
    INSERT INTO project (name, dataset, audit_log_id, read_group_id, write_group_id)
    VALUES (:name, :dataset, :audit_log_id, :read_group_id, :write_group_id)
    RETURNING ID"""
            values = {
                'name': project_name,
                'dataset': dataset_name,
                'audit_log_id': await self.audit_log_id(),
            }

            project_id = await self.connection.fetch_val(_query, values)

        return project_id

    async def update_project(self, project_name: str, update: dict, author: str):
        """Update a metamist project"""
        await self.check_project_creator_permissions(author)

        meta = update.get('meta')

        fields: dict[str, Any] = {
            'audit_log_id': await self.audit_log_id(),
            'name': project_name,
        }

        setters = ['audit_log_id = :audit_log_id']

        if meta is not None and len(meta) > 0:
            fields['meta'] = to_db_json(meta)
            setters.append('meta = JSON_MERGE_PATCH(COALESCE(meta, "{}"), :meta)')

        fields_str = ', '.join(setters)

        _query = f'UPDATE project SET {fields_str} WHERE name = :name'

        await self.connection.execute(_query, fields)

    async def delete_project_data(self, project_id: int, delete_project: bool) -> bool:
        """
        Delete data in metamist project, requires project_creator_permissions
        Can optionally delete the project also.
        """
        if delete_project:
            # stop allowing delete project with analysis-runner entries
            raise ValueError('2024-03-08: delete_project is no longer allowed')

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
DELETE FROM analysis_sample WHERE sample_id in (
    SELECT s.id FROM sample s
    WHERE s.project = :project
);
DELETE FROM analysis_sequencing_group WHERE analysis_id in (
    SELECT id FROM analysis WHERE project = :project
);
DELETE FROM analysis_sample WHERE analysis_id in (
    SELECT id FROM analysis WHERE project = :project
);
DELETE FROM assay WHERE sample_id in (SELECT id FROM sample WHERE project = :project);
DELETE FROM sequencing_group WHERE sample_id IN (
    SELECT id FROM sample WHERE project = :project
);
DELETE FROM sample WHERE project = :project;
DELETE FROM participant WHERE project = :project;
DELETE FROM analysis WHERE project = :project;
            """

            await self.connection.execute(_query, {'project': project_id})

        return True

    async def set_project_members(
        self, project: Project, members: list[ProjectMemberRole]
    ):
        """
        Set group members for a group (by name)
        """
        print('@TODO')
        # group_id = await self.gtable.get_group_name_from_id(group_name)
        # await self.gtable.set_group_members(
        #     group_id, members, audit_log_id=await self.audit_log_id()
        # )

    # endregion CREATE / UPDATE


class GroupTable:
    """
    Capture Group table operations and queries
    """

    table_name = 'group'

    def __init__(self, connection: Database, allow_full_access: bool | None = None):
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
            SELECT COUNT(*) > 0
            FROM group_member gm
            WHERE gm.group_id = :group_id
            AND gm.member = :member
        """
        value = await self.connection.fetch_val(
            _query, {'group_id': group_id, 'member': member}
        )
        if value not in (0, 1):
            raise ValueError(
                f'Unexpected value {value!r} when determining access to group with ID '
                f'{group_id} for {member}'
            )
        return bool(value)

    async def check_if_member_in_group_name(self, group_name: str, member: str) -> bool:
        """Check if a member is in a group"""
        if self.allow_full_access:
            return True

        _query = """
            SELECT COUNT(*) > 0
            FROM group_member gm
            INNER JOIN `group` g ON g.id = gm.group_id
            WHERE g.name = :group_name
            AND gm.member = :member
        """
        value = await self.connection.fetch_val(
            _query, {'group_name': group_name, 'member': member}
        )
        if value not in (0, 1):
            raise ValueError(
                f'Unexpected value {value!r} when determining access to {group_name} '
                f'for {member}'
            )

        return bool(value)

    async def check_which_groups_member_has(
        self, group_ids: set[int], member: str
    ) -> set[int]:
        """
        Check which groups a member has
        """
        if self.allow_full_access:
            return group_ids

        _query = """
            SELECT gm.group_id as gid
            FROM group_member gm
            WHERE gm.member = :member AND gm.group_id IN :group_ids
        """
        results = await self.connection.fetch_all(
            _query, {'group_ids': group_ids, 'member': member}
        )
        return set(r['gid'] for r in results)

    async def create_group(self, name: str, audit_log_id: int) -> int:
        """Create a new group"""
        _query = """
            INSERT INTO `group` (name, audit_log_id)
            VALUES (:name, :audit_log_id)
            RETURNING id
        """
        return await self.connection.fetch_val(
            _query, {'name': name, 'audit_log_id': audit_log_id}
        )

    async def set_group_members(
        self, group_id: int, members: list[str], audit_log_id: int
    ):
        """
        Set group members for a group (by id)
        """
        await self.connection.execute(
            """
            DELETE FROM group_member
            WHERE group_id = :group_id
            """,
            {'group_id': group_id},
        )
        await self.connection.execute_many(
            """
            INSERT INTO group_member (group_id, member, audit_log_id)
            VALUES (:group_id, :member, :audit_log_id)
            """,
            [
                {
                    'group_id': group_id,
                    'member': member,
                    'audit_log_id': audit_log_id,
                }
                for member in members
            ],
        )
