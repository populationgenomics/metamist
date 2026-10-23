from string.templatelib import Template
from typing import TYPE_CHECKING, Any

from psycopg import sql
from psycopg.rows import class_row
from psycopg.types.json import Jsonb

from db.python.utils import Forbidden, get_logger
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
        _query = t"""
            -- Check what admin groups the user belongs to, if they belong
            -- to project-creators then a project_admin role will be added to
            -- all projects, if they belong to members-admin then a `project_member_admin`
            -- role will be appended to all projects.
            WITH admin_roles AS (
                SELECT
                    CASE (g.name)
                        WHEN {GROUP_NAME_PROJECT_CREATORS} THEN 'project_admin'
                        WHEN {GROUP_NAME_MEMBERS_ADMIN} THEN 'project_member_admin'
                    END
                as role
                FROM "group" g
                JOIN group_member gm
                ON gm.group_id = g.id
                WHERE gm.member = {user}
                AND g.name in ({GROUP_NAME_PROJECT_CREATORS}, {GROUP_NAME_MEMBERS_ADMIN})
            ),
            -- Combine together the project roles and the admin roles
            project_roles AS (
                SELECT pm.project_id, pm.member, pm.role
                FROM project_member pm
                WHERE pm.member = {user}
                UNION ALL
                SELECT p.id as project_id, {user} as member, ar.role::project_member_role
                FROM project p
                JOIN admin_roles ar ON TRUE
            )
            SELECT
                p.id,
                p.name,
                coalesce(p.meta, '{{}}') as meta,
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
            await acur.execute(_query)
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

        _query = t"""
            SELECT gm.member, g.name
            FROM "group" g
            JOIN group_member gm ON g.id = gm.group_id
            WHERE LOWER(g.name) = {group_name.lower()}
            AND LOWER(gm.member) = {member.lower()}
            LIMIT 1
        """

        conn = self.connection.pg_connection
        cur = await conn.execute(_query)
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
        audit_log_id = await self.audit_log_id()

        _query = t"""
            INSERT INTO project (name, dataset, audit_log_id)
            VALUES ({project_name}, {dataset_name}, {audit_log_id})
            RETURNING id
        """

        project_id = (await self.connection.execute_must_fetch_one(_query))['id']

        await self.connection.refresh_projects()

        return project_id

    async def update_project(
        self, project_name: str, update: dict[str, Any], author: str
    ):
        """Update a metamist project"""
        await self.check_project_creator_permissions(author)

        meta = update.get('meta')
        audit_log_id = await self.audit_log_id()

        setters = [t'audit_log_id = {audit_log_id}']

        if meta is not None and len(meta) > 0:
            setters.append(
                t"meta = json_merge_patch(COALESCE(meta, '{{}}'::jsonb),  {Jsonb(meta)})"
            )
        fields_str = sql.SQL(',').join(setters)

        _query = t'UPDATE project SET {fields_str:q} WHERE LOWER(name) = {project_name.lower()}'

        conn = self.connection.pg_connection
        await conn.execute(_query)

    async def delete_project_data(self, project: Project) -> bool:
        """
        Delete data in metamist project, requires project_creator_permissions
        """
        if not project.is_test_project:
            raise ValueError('2025-12-04: refusing to delete non-test project')

        project_id = project.id

        delete_queries: list[Template] = [
            t"""
            DELETE FROM comment WHERE id IN (
                SELECT ac.comment_id FROM assay_comment ac
                INNER JOIN assay a ON ac.assay_id = a.id
                INNER JOIN sample s ON a.sample_id = s.id
                WHERE s.project = {project_id}

                UNION
                SELECT fc.comment_id FROM family_comment fc
                INNER JOIN family f ON fc.family_id = f.id
                WHERE f.project = {project_id}

                UNION
                SELECT pc.comment_id FROM participant_comment pc
                INNER JOIN participant p ON pc.participant_id = p.id
                WHERE p.project = {project_id}

                UNION
                SELECT comment_id FROM project_comment
                WHERE project_id = {project_id}

                UNION
                SELECT sc.comment_id FROM sample_comment sc
                INNER JOIN sample s ON sc.sample_id = s.id
                WHERE s.project = {project_id}

                UNION
                SELECT sgc.comment_id FROM sequencing_group_comment sgc
                INNER JOIN sequencing_group sg ON sgc.sequencing_group_id = sg.id
                INNER JOIN sample s ON sg.sample_id = s.id
                WHERE s.project = {project_id}
            )
            """,
            t'DELETE FROM project_member WHERE project_id = {project_id}',
            # Deletion from `output_file` cascades to `analysis_outputs`
            t"""
            DELETE FROM output_file WHERE id IN (
                SELECT file_id FROM analysis_outputs ao
                INNER JOIN analysis a ON ao.analysis_id = a.id
                WHERE a.project = {project_id}
            )
            """,
            # Analysis join tables: clear rows linking to this project's analyses OR
            # to this project's entities (analyses can be cross-project).
            t"""
            DELETE FROM analysis_sequencing_group WHERE analysis_id IN (
                SELECT id FROM analysis WHERE project = {project_id}
            ) OR sequencing_group_id IN (
                SELECT sg.id FROM sequencing_group sg
                INNER JOIN sample ON sample.id = sg.sample_id
                WHERE sample.project = {project_id}
            )
            """,
            t"""
            DELETE FROM analysis_cohort WHERE analysis_id IN (
                SELECT id FROM analysis WHERE project = {project_id}
            ) OR cohort_id IN (
                SELECT id FROM cohort WHERE project = {project_id}
            )
            """,
            # Cohorts: delete from join table first, then cohort (references
            # cohort_template), then the templates themselves.
            t"""
            DELETE FROM cohort_sequencing_group WHERE cohort_id IN (
                SELECT id FROM cohort WHERE project = {project_id}
            ) OR sequencing_group_id IN (
                SELECT sg.id FROM sequencing_group sg
                INNER JOIN sample ON sample.id = sg.sample_id
                WHERE sample.project = {project_id}
            )
            """,
            t'DELETE FROM cohort WHERE project = {project_id}',
            t'DELETE FROM cohort_template WHERE project = {project_id}',
            # Assay <-> sequencing_group link, delete from here before either side
            # is removed.
            t"""
            DELETE FROM sequencing_group_assay WHERE sequencing_group_id IN (
                SELECT sg.id FROM sequencing_group sg
                INNER JOIN sample ON sample.id = sg.sample_id
                WHERE sample.project = {project_id}
            )
            """,
            # Clear all external id tables
            t'DELETE FROM sequencing_group_external_id WHERE project = {project_id}',
            t'DELETE FROM assay_external_id WHERE project = {project_id}',
            t'DELETE FROM sample_external_id WHERE project = {project_id}',
            t'DELETE FROM participant_external_id WHERE project = {project_id}',
            t'DELETE FROM family_external_id WHERE project = {project_id}',
            # Delete from family + participant link tables before families/participants
            # are removed.
            t"""
            DELETE FROM family_participant WHERE family_id IN (
                SELECT id FROM family WHERE project = {project_id}
            )
            """,
            t"""
            DELETE FROM participant_phenotypes WHERE participant_id IN (
                SELECT id FROM participant WHERE project = {project_id}
            )
            """,
            # Entity tables, delete from dependent tables before parents.
            t"""
            DELETE FROM sequencing_group WHERE sample_id IN (
                SELECT id FROM sample WHERE project = {project_id}
            )
            """,
            t"""
            DELETE FROM assay WHERE sample_id IN (
                SELECT id FROM sample WHERE project = {project_id}
            )
            """,
            t'DELETE FROM family WHERE project = {project_id}',
            t'DELETE FROM sample WHERE project = {project_id}',
            t'DELETE FROM participant WHERE project = {project_id}',
            t'DELETE FROM analysis WHERE project = {project_id}',
        ]

        conn = self.connection.pg_connection
        async with conn.transaction():
            for query in delete_queries:
                await conn.execute(query)

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
                t"""
                SELECT project_id, member, role, audit_log_id
                FROM project_member
                WHERE project_id = {project.id}
                """
            )
            existing_rows = await cur.fetchall()

            audit_log_id_map: dict[tuple[str, str], int | None] = {
                (r['member'], r['role']): r['audit_log_id'] for r in existing_rows
            }

            # delete existing rows for project
            await cur.execute(
                t"""
                DELETE FROM project_member
                WHERE project_id = {project.id}
                """
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
