import uuid
from test.testbase import DbIsolatedTest, run_as_sync

from db.python.tables.project import (
    GROUP_NAME_MEMBERS_ADMIN,
    GROUP_NAME_PROJECT_CREATORS,
    ProjectPermissionsTable,
)
from db.python.utils import Forbidden
from models.models.project import (
    FullWriteAccessRoles,
    ProjectMemberRole,
    ProjectMemberUpdate,
    ReadAccessRoles,
)


class TestGroupAccess(DbIsolatedTest):
    """
    Test project access permissions + newish internal group implementation
    """

    @run_as_sync
    async def setUp(self):
        """Setup tests"""
        super().setUp()

        # specifically required to test permissions
        self.pttable = ProjectPermissionsTable(self.connection)

    async def _add_group_member_direct(self, group_name: str):
        """
        Helper method to directly add members to group with name
        """
        members_admin_group = await self.connection.connection.fetch_val(
            'SELECT id FROM `group` WHERE name = :name',
            {'name': group_name},
        )
        await self.connection.connection.execute(
            """
            INSERT INTO group_member (group_id, member, audit_log_id)
            VALUES (:group_id, :member, :audit_log_id);
            """,
            {
                'group_id': members_admin_group,
                'member': self.author,
                'audit_log_id': await self.audit_log_id(),
            },
        )

    @run_as_sync
    async def test_project_creators_failed(self):
        """
        Test that a user without permission cannot create a project
        """
        with self.assertRaises(Forbidden):
            await self.pttable.create_project(
                'another-test-project', 'another-test-project', self.author
            )

    @run_as_sync
    async def test_project_create_succeed(self):
        """
        Test that a user with permission can create a project,
        and that read/write groups are created
        """
        await self._add_group_member_direct(GROUP_NAME_PROJECT_CREATORS)
        g = str(uuid.uuid4())

        project_id = await self.pttable.create_project(g, g, self.author)

        # pylint: disable=protected-access
        project_id_map, _ = await self.pttable.get_projects_accessible_by_user(
            user=self.author
        )

        project = project_id_map.get(project_id)
        assert project
        self.assertEqual(project.name, g)


class TestProjectAccess(DbIsolatedTest):
    """Test project access methods directly"""

    @run_as_sync
    async def setUp(self):
        """Setup tests"""
        super().setUp()

        # specifically required to test permissions
        self.pttable = ProjectPermissionsTable(self.connection)

    async def _add_group_member_direct(
        self,
        group_name: str,
    ):
        """
        Helper method to directly add members to group with name
        """
        members_admin_group = await self.connection.connection.fetch_val(
            'SELECT id FROM `group` WHERE name = :name',
            {'name': group_name},
        )
        await self.connection.connection.execute(
            """
            INSERT INTO group_member (group_id, member, audit_log_id)
            VALUES (:group_id, :member, :audit_log_id);
            """,
            {
                'group_id': members_admin_group,
                'member': self.author,
                'audit_log_id': await self.audit_log_id(),
            },
        )

    @run_as_sync
    async def test_no_project_access(self):
        """
        Test that a user without permission cannot access a project
        """
        await self._add_group_member_direct(GROUP_NAME_PROJECT_CREATORS)
        g = str(uuid.uuid4())

        project_id = await self.pttable.create_project(g, g, self.author)
        with self.assertRaises(Forbidden):
            self.connection.check_access_to_projects_for_ids(
                project_ids=[project_id], allowed_roles=ReadAccessRoles
            )

        with self.assertRaises(Forbidden):
            self.connection.get_and_check_access_to_projects_for_names(
                project_names=[g], allowed_roles=ReadAccessRoles
            )

    @run_as_sync
    async def test_project_access_success(self):
        """
        Test that a user with permission CAN access a project
        """
        await self._add_group_member_direct(GROUP_NAME_PROJECT_CREATORS)
        await self._add_group_member_direct(GROUP_NAME_MEMBERS_ADMIN)

        g = str(uuid.uuid4())

        pid = await self.pttable.create_project(g, g, self.author)

        project_id_map, _ = await self.pttable.get_projects_accessible_by_user(
            user=self.author
        )
        project = project_id_map.get(pid)
        assert project
        await self.pttable.set_project_members(
            project=project,
            members=[ProjectMemberUpdate(member=self.author, roles=['reader'])],
        )

        project_for_id = self.connection.get_and_check_access_to_projects_for_ids(
            project_ids=[pid], allowed_roles=ReadAccessRoles
        )
        user_project_for_id = next(p for p in project_for_id)
        self.assertEqual(pid, user_project_for_id.id)

        project_for_name = self.connection.get_and_check_access_to_projects_for_names(
            project_names=[g], allowed_roles=ReadAccessRoles
        )
        user_project_for_name = next(p for p in project_for_name)
        self.assertEqual(g, user_project_for_name.name)

    @run_as_sync
    async def test_project_access_insufficient(self):
        """
        Test that a user with access to a project will be disallowed if their access is
        not sufficient
        """
        await self._add_group_member_direct(GROUP_NAME_PROJECT_CREATORS)
        await self._add_group_member_direct(GROUP_NAME_MEMBERS_ADMIN)

        g = str(uuid.uuid4())

        pid = await self.pttable.create_project(g, g, self.author)

        project_id_map, _ = await self.pttable.get_projects_accessible_by_user(
            user=self.author
        )
        project = project_id_map.get(pid)
        assert project
        # Give the user read access to the project
        await self.pttable.set_project_members(
            project=project,
            members=[ProjectMemberUpdate(member=self.author, roles=['reader'])],
        )

        # But require Write access

        with self.assertRaises(Forbidden):
            self.connection.check_access_to_projects_for_ids(
                project_ids=[project.id], allowed_roles=FullWriteAccessRoles
            )

        with self.assertRaises(Forbidden):
            self.connection.get_and_check_access_to_projects_for_names(
                project_names=[g], allowed_roles=FullWriteAccessRoles
            )

    @run_as_sync
    async def test_get_my_projects(self):
        """
        Test that a user with permission only has MY projects
        """
        await self._add_group_member_direct(GROUP_NAME_PROJECT_CREATORS)
        await self._add_group_member_direct(GROUP_NAME_MEMBERS_ADMIN)

        g = str(uuid.uuid4())

        pid = await self.pttable.create_project(g, g, self.author)

        project_id_map, _ = await self.pttable.get_projects_accessible_by_user(
            user=self.author
        )
        project = project_id_map.get(pid)
        assert project
        # Give the user read access to the project
        await self.pttable.set_project_members(
            project=project,
            members=[ProjectMemberUpdate(member=self.author, roles=['contributor'])],
        )

        project_id_map, project_name_map = (
            await self.pttable.get_projects_accessible_by_user(user=self.author)
        )

        # Get projects with at least a read access role
        my_projects = self.connection.projects_with_role(
            {ProjectMemberRole.contributor}
        )
        print(my_projects)

        self.assertEqual(len(project_id_map), len(project_name_map))
        self.assertEqual(len(my_projects), 1)
        self.assertEqual(pid, my_projects[0].id)
