from typing import TYPE_CHECKING, Any

from psycopg import sql
from psycopg.rows import class_row
from psycopg.types.enum import EnumInfo, register_enum

from db.python.utils import Forbidden, get_logger
from models.models.project import (
    Project,
    ProjectMemberRole,
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

    def __init__(self, connection: Connection):
        self.connection = connection

    async def audit_log_id(self):
        """
        Generate (or return) a audit_log_id by inserting a row into the database
        """
        return await self.connection.audit_log_id()

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
                        WHEN %(project_creators_group_name)s THEN 'project_admin'
                        WHEN %(members_admin_group_name)s THEN 'project_member_admin'
                    END
                as role
                FROM "group" g
                JOIN group_member gm
                ON gm.group_id = g.id
                WHERE gm.member = %(user)s
                AND g.name in (%(project_creators_group_name)s, %(members_admin_group_name)s)
            ),
            -- Combine together the project roles and the admin roles
            project_roles AS (
                SELECT pm.project_id, pm.member, pm.role
                FROM project_member pm
                WHERE pm.member = %(user)s
                UNION ALL
                SELECT p.id as project_id, %(user)s as member, ar.role::project_member_role
                FROM project p
                JOIN admin_roles ar ON TRUE
            )
            SELECT
                p.id,
                p.name,
                coalesce(p.meta, '{}') as meta,
                p.dataset,
                array_agg(pr.role) as roles
            FROM project p
            JOIN project_roles pr
            ON p.id = pr.project_id
            GROUP BY p.id
        """

        project_id_map: dict[int, Project] = {}
        project_name_map: dict[str, Project] = {}

        conn = self.connection.pg_connection
        async with conn.cursor(row_factory=class_row(Project)) as acur:
            info = await EnumInfo.fetch(conn, 'project_member_role')
            if info is None:
                raise ValueError(
                    "Enum type 'project_member_role' not found in database"
                )
            register_enum(info, acur, ProjectMemberRole)

            await acur.execute(_query, parameters)

            projects = await acur.fetchall()

        for project in projects:
            project_id_map[project.id] = project
            project_name_map[project.name] = project
        return project_id_map, project_name_map

    async def get_seqr_project_ids(self) -> list[int]:
        """
        Get all projects with meta.is_seqr = true
        """
        _query = "SELECT id FROM project WHERE (meta->>'is_seqr')::boolean"

        conn = self.connection.pg_connection

        cur = await conn.execute(_query)
        rows = await cur.fetchall()
        return [r['id'] for r in rows]

    async def check_if_member_in_group_by_name(self, group_name: str, member: str):
        """Check if a user exists in the group"""

        _query = """
            SELECT gm.member, g.name
            FROM "group" g
            JOIN group_member gm ON g.id = gm.group_id
            WHERE g.name = %(group_name)s
            AND gm.member = %(member)s
            LIMIT 1
        """

        conn = self.connection.pg_connection
        cur = conn.execute(
            _query, {'group_name': group_name, 'member': member}
        )
        row = await cur.fetchone()

        return row is not None

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

        _query = """
            INSERT INTO project (name, dataset, audit_log_id)
            VALUES (%(name)s, %(dataset)s, %(audit_log_id)s)
            RETURNING id
        """
        values: dict[str, Any] = {
            'name': project_name,
            'dataset': dataset_name,
            'audit_log_id': await self.audit_log_id(),
        }

        conn = self.connection.pg_connection

        cur = await conn.execute(_query, values)
        row = await cur.fetchone()
        assert row
        project_id = row['id']

        await self.connection.refresh_projects()

        return project_id

    async def update_project(
        self, project_name: str, update: dict[str, Any], author: str
    ):
        """Update a metamist project"""
        await self.check_project_creator_permissions(author)

        meta = update.get('meta')

        fields: dict[str, Any] = {
            'audit_log_id': await self.audit_log_id(),
            'name': project_name,
        }

        setters = [sql.SQL('audit_log_id = %(audit_log_id)s')]

        if meta is not None and len(meta) > 0:
            fields['meta'] = meta
            setters.append(
                sql.SQL(
                    "meta = json_merge_patch(COALESCE(meta, '{}'::jsonb),  %(meta)s)"
                )
            )
        fields_str = sql.SQL(',').join(setters)

        _query = sql.SQL(
            'UPDATE project SET {fields_str} WHERE name = %(name)s'
        ).format(fields_str=fields_str)

        conn = self.connection.pg_connection
        await conn.execute(_query, fields)

    async def delete_project_data(self, project: Project) -> bool:
        """
        Delete data in metamist project, requires project_creator_permissions
        """
        if not project.is_test_project:
            raise ValueError('2025-12-04: refusing to delete non-test project')

        delete_queries: list[sql.SQL] = [
            sql.SQL("""
            DELETE FROM comment WHERE id IN (
                SELECT ac.comment_id FROM assay_comment ac
                INNER JOIN assay a ON ac.assay_id = a.id
                INNER JOIN sample s ON a.sample_id = s.id
                WHERE s.project = %(project)s

                UNION
                SELECT fc.comment_id FROM family_comment fc
                INNER JOIN family f ON fc.family_id = f.id
                WHERE f.project = %(project)s

                UNION
                SELECT pc.comment_id FROM participant_comment pc
                INNER JOIN participant p ON pc.participant_id = p.id
                WHERE p.project = %(project)s

                UNION
                SELECT comment_id FROM project_comment
                WHERE project_id = %(project)s

                UNION
                SELECT sc.comment_id FROM sample_comment sc
                INNER JOIN sample s ON sc.sample_id = s.id
                WHERE s.project = %(project)s

                UNION
                SELECT sgc.comment_id FROM sequencing_group_comment sgc
                INNER JOIN sequencing_group sg ON sgc.sequencing_group_id = sg.id
                INNER JOIN sample s ON sg.sample_id = s.id
                WHERE s.project = %(project)s
            )
            """),
            sql.SQL('DELETE FROM project_member WHERE project_id = %(project)s'),
            sql.SQL("""
            DELETE FROM participant_phenotypes WHERE participant_id IN (
                SELECT id FROM participant WHERE project = %(project)s
            )
            """),
            sql.SQL("""
            DELETE FROM family_participant WHERE family_id IN (
                SELECT id FROM family WHERE project = %(project)s
            )
            """),
            sql.SQL('DELETE FROM family_external_id WHERE project = %(project)s'),
            sql.SQL('DELETE FROM family WHERE project = %(project)s'),
            sql.SQL(
                'DELETE FROM sequencing_group_external_id WHERE project = %(project)s'
            ),
            sql.SQL('DELETE FROM sample_external_id WHERE project = %(project)s'),
            sql.SQL('DELETE FROM participant_external_id WHERE project = %(project)s'),
            sql.SQL('DELETE FROM assay_external_id WHERE project = %(project)s'),
            sql.SQL("""
            DELETE FROM sequencing_group_assay WHERE sequencing_group_id IN (
                SELECT sg.id FROM sequencing_group sg
                INNER JOIN sample ON sample.id = sg.sample_id
                WHERE sample.project = %(project)s
            )
            """),
            sql.SQL("""
            DELETE FROM analysis_sequencing_group WHERE sequencing_group_id IN (
                SELECT sg.id FROM sequencing_group sg
                INNER JOIN sample ON sample.id = sg.sample_id
                WHERE sample.project = %(project)s
            )
            """),
            sql.SQL("""
            DELETE FROM output_file WHERE id IN (
                SELECT file_id FROM analysis_outputs ao
                INNER JOIN analysis a ON ao.analysis_id = a.id
                WHERE a.project = %(project)s
            )
            """),
            sql.SQL("""
            DELETE FROM analysis_sequencing_group WHERE analysis_id IN (
                SELECT id FROM analysis WHERE project = %(project)s
            )
            """),
            sql.SQL("""
            DELETE FROM analysis_cohort WHERE cohort_id IN (
                SELECT id FROM cohort WHERE project = %(project)s
            )
            """),
            sql.SQL("""
            DELETE FROM cohort_sequencing_group WHERE cohort_id IN (
                SELECT id FROM cohort WHERE project = %(project)s
            )
            """),
            sql.SQL('DELETE FROM cohort_template WHERE project = %(project)s'),
            sql.SQL('DELETE FROM cohort WHERE project = %(project)s'),
            sql.SQL("""
            DELETE FROM assay WHERE sample_id IN (
                SELECT id FROM sample WHERE project = %(project)s
            )
            """),
            sql.SQL("""
            DELETE FROM sequencing_group WHERE sample_id IN (
                SELECT id FROM sample WHERE project = %(project)s
            )
            """),
            sql.SQL('DELETE FROM sample WHERE project = %(project)s'),
            sql.SQL('DELETE FROM participant WHERE project = %(project)s'),
            sql.SQL('DELETE FROM analysis WHERE project = %(project)s'),
        ]

        conn = self.connection.pg_connection
        async with conn.transaction():
            for query in delete_queries:
                await conn.execute(query, {'project': project.id})

        return True

    async def set_project_members(
        self, project: Project, members: list[ProjectMemberUpdate]
    ):
        """
        Set group members for a group (by name)
        """

        conn = self.connection.pg_connection
        async with (
            conn.transaction(),
            conn.cursor() as cur,
        ):
            # Get existing rows so that we can keep the existing audit log ids
            await cur.execute(
                """
                SELECT project_id, member, role, audit_log_id
                FROM project_member
                WHERE project_id = %(project_id)s
                """,
                {'project_id': project.id},
            )
            existing_rows = await cur.fetchall()

            audit_log_id_map: dict[tuple[str, str], int | None] = {
                (r['member'], r['role']): r['audit_log_id'] for r in existing_rows
            }

            # delete existing rows for project
            await cur.execute(
                """
                DELETE FROM project_member
                WHERE project_id = %(project_id)s
                """,
                {'project_id': project.id},
            )

            new_audit_log_id = await self.audit_log_id()

            db_members: list[dict[str, str]] = []

            for m in members:
                db_members.extend([{'member': m.member, 'role': r} for r in m.roles])

            await cur.executemany(
                """
                INSERT INTO project_member
                    (project_id, member, role, audit_log_id)
                VALUES (%(project_id)s, %(member)s, %(role)s, %(audit_log_id)s)
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

        await self.connection.refresh_projects()

    # endregion CREATE / UPDATE
