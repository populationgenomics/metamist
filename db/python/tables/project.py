# pylint: disable=global-statement
from typing import TYPE_CHECKING, Any, Tuple

from databases import Database

from db.python.utils import Forbidden, get_logger, to_db_json
from models.models.project import (
    Project,
    ProjectMemberUpdate,
    project_member_role_names,
)

# Avoid circular import for type definition
if TYPE_CHECKING:
    from db.python.connect import Connection
else:
    Connection = object

logger = get_logger()

GROUP_NAME_PROJECT_CREATORS = 'project-creators'
GROUP_NAME_MEMBERS_ADMIN = 'members-admin'


class ProjectPermissionsTable:
    """
    Capture project operations and queries
    """

    table_name = 'project'

    def __init__(
        self,
        connection: Connection | None,
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
        self, user: str
    ) -> tuple[dict[int, Project], dict[str, Project]]:
        """
        Get projects that are accessible by the specified user
        """
        parameters: dict[str, str] = {
            'user': user,
            'project_creators_group_name': GROUP_NAME_PROJECT_CREATORS,
            'members_admin_group_name': GROUP_NAME_MEMBERS_ADMIN,
        }

        _query = """
            -- Check what admin groups the user belongs to, if they belong
            -- to project-creators then a project_admin role will be added to
            -- all projects, if they belong to members-admin then a `project_member_admin`
            -- role will be appended to all projects.
            WITH admin_roles AS (
                SELECT
                    CASE (g.name)
                        WHEN :project_creators_group_name THEN 'project_admin'
                        WHEN :members_admin_group_name THEN 'project_member_admin'
                    END
                as role
                FROM `group` g
                JOIN group_member gm
                ON gm.group_id = g.id
                WHERE gm.member = :user
                AND g.name in (:project_creators_group_name, :members_admin_group_name)
            ),
            -- Combine together the project roles and the admin roles
            project_roles AS (
                SELECT pm.project_id, pm.member, pm.role
                FROM project_member pm
                WHERE pm.member = :user
                UNION ALL
                SELECT p.id as project_id, :user as member, ar.role
                FROM project p
                JOIN admin_roles ar ON TRUE
            )
            SELECT
                p.id,
                p.name,
                p.meta,
                p.dataset,
                GROUP_CONCAT(pr.role) as roles
            FROM project p
            JOIN project_roles pr
            ON p.id = pr.project_id
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

    async def get_seqr_project_ids(self) -> list[int]:
        """
        Get all projects with meta.is_seqr = true
        """
        rows = await self.connection.fetch_all(
            "SELECT id FROM project WHERE JSON_VALUE(meta, '$.is_seqr') = 1"
        )
        return [r['id'] for r in rows]

    async def check_if_member_in_group_by_name(self, group_name: str, member: str):
        """Check if a user exists in the group"""

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

    async def check_project_creator_permissions(self, author: str):
        """Check author has project_creator permissions"""
        is_in_group = await self.check_if_member_in_group_by_name(
            group_name=GROUP_NAME_PROJECT_CREATORS, member=author
        )
        if not is_in_group:
            raise Forbidden(f'{author} does not have access to create a project')

        return True

    async def check_member_admin_permissions(self, author: str):
        """Check author has member_admin permissions"""
        is_in_group = await self.check_if_member_in_group_by_name(
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
    ):
        """Create project row"""
        await self.check_project_creator_permissions(author)

        async with self.connection.transaction():
            _query = """\
    INSERT INTO project (name, dataset, audit_log_id)
    VALUES (:name, :dataset, :audit_log_id)
    RETURNING ID"""
            values = {
                'name': project_name,
                'dataset': dataset_name,
                'audit_log_id': await self.audit_log_id(),
            }

            project_id = await self.connection.fetch_val(_query, values)

        if self._connection:
            await self._connection.refresh_projects()

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
DELETE FROM family_external_id WHERE project = :project;
DELETE FROM family WHERE project = :project;
DELETE FROM sequencing_group_external_id WHERE project = :project;
DELETE FROM sample_external_id WHERE project = :project;
DELETE FROM participant_external_id WHERE project = :project;
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
        self, project: Project, members: list[ProjectMemberUpdate]
    ):
        """
        Set group members for a group (by name)
        """

        async with self.connection.transaction():
            # Get existing rows so that we can keep the existing audit log ids
            existing_rows = await self.connection.fetch_all(
                """
                SELECT project_id, member, role, audit_log_id
                FROM project_member
                WHERE project_id = :project_id
            """,
                {'project_id': project.id},
            )

            audit_log_id_map: dict[Tuple[str, str], int | None] = {
                (r['member'], r['role']): r['audit_log_id'] for r in existing_rows
            }

            # delete existing rows for project
            await self.connection.execute(
                """
                DELETE FROM project_member
                WHERE project_id = :project_id
            """,
                {'project_id': project.id},
            )

            new_audit_log_id = await self.audit_log_id()

            db_members: list[dict[str, str]] = []

            for m in members:
                db_members.extend([{'member': m.member, 'role': r} for r in m.roles])

            await self.connection.execute_many(
                """
                    INSERT INTO project_member
                        (project_id, member, role, audit_log_id)
                    VALUES (:project_id, :member, :role, :audit_log_id);
                """,
                [
                    {
                        'project_id': project.id,
                        'member': m['member'],
                        'role': m['role'],
                        'audit_log_id': audit_log_id_map.get(
                            (m['member'], m['role']), new_audit_log_id
                        ),
                    }
                    for m in db_members
                    if m['role'] in project_member_role_names
                ],
            )

        if self._connection:
            await self._connection.refresh_projects()

    # endregion CREATE / UPDATE
