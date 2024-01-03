import uuid
from test.testbase import DbIsolatedTest, run_as_sync

from db.python.tables.project import (
    GROUP_NAME_MEMBERS_ADMIN,
    GROUP_NAME_PROJECT_CREATORS,
    Forbidden,
    ProjectPermissionsTable,
)
from db.python.utils import NotFoundError


class TestGroupAccess(DbIsolatedTest):
    """
    Test project access permissions + newish internal group implementation
    """

    @run_as_sync
    async def setUp(self):
        """Setup tests"""
        super().setUp()

        # specifically required to test permissions
        self.pttable = ProjectPermissionsTable(self.connection, False)

    async def _add_group_member_direct(self, group_name):
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
    async def test_group_set_members_failed_no_permission(self):
        """
        Test that a user without permission cannot set members
        """
        with self.assertRaises(Forbidden):
            await self.pttable.set_group_members(
                'another-test-project', ['user1'], self.author
            )

    @run_as_sync
    async def test_group_set_members_failed_not_exists(self):
        """
        Test that a user with permission, cannot set members
        for a group that doesn't exist
        """
        await self._add_group_member_direct(GROUP_NAME_MEMBERS_ADMIN)
        with self.assertRaises(NotFoundError):
            await self.pttable.set_group_members(
                'another-test-project', ['user1'], self.author
            )

    @run_as_sync
    async def test_group_set_members_succeeded(self):
        """
        Test that a user with permission, can set members for a group that exists
        """
        await self._add_group_member_direct(GROUP_NAME_MEMBERS_ADMIN)

        g = str(uuid.uuid4())
        await self.pttable.gtable.create_group(g, await self.audit_log_id())

        self.assertFalse(
            await self.pttable.gtable.check_if_member_in_group_name(g, 'user1')
        )
        self.assertFalse(
            await self.pttable.gtable.check_if_member_in_group_name(g, 'user2')
        )

        await self.pttable.set_group_members(
            group_name=g, members=['user1', 'user2'], author=self.author
        )

        self.assertTrue(
            await self.pttable.gtable.check_if_member_in_group_name(g, 'user1')
        )
        self.assertTrue(
            await self.pttable.gtable.check_if_member_in_group_name(g, 'user2')
        )

    @run_as_sync
    async def test_check_which_groups_member_is_missing(self):
        """Test the check_which_groups_member_has function"""
        await self._add_group_member_direct(GROUP_NAME_MEMBERS_ADMIN)

        group = str(uuid.uuid4())
        gid = await self.pttable.gtable.create_group(group, await self.audit_log_id())
        present_gids = await self.pttable.gtable.check_which_groups_member_has(
            {gid}, self.author
        )
        missing_gids = {gid} - present_gids
        self.assertEqual(1, len(missing_gids))
        self.assertEqual(gid, missing_gids.pop())

    @run_as_sync
    async def test_check_which_groups_member_is_missing_none(self):
        """Test the check_which_groups_member_has function"""
        await self._add_group_member_direct(GROUP_NAME_MEMBERS_ADMIN)

        group = str(uuid.uuid4())
        gid = await self.pttable.gtable.create_group(group, await self.audit_log_id())
        await self.pttable.gtable.set_group_members(
            gid, [self.author], audit_log_id=await self.audit_log_id()
        )
        present_gids = await self.pttable.gtable.check_which_groups_member_has(
            group_ids={gid}, member=self.author
        )
        missing_gids = {gid} - present_gids

        self.assertEqual(0, len(missing_gids))

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
        project = await self.pttable._get_project_by_id(project_id)

        # test that the group names make sense
        self.assertIsNotNone(project.read_group_id)
        self.assertIsNotNone(project.write_group_id)


class TestProjectAccess(DbIsolatedTest):
    """Test project access methods directly"""

    @run_as_sync
    async def setUp(self):
        """Setup tests"""
        super().setUp()

        # specifically required to test permissions
        self.pttable = ProjectPermissionsTable(self.connection, False)

    async def _add_group_member_direct(self, group_name):
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
            await self.pttable.get_and_check_access_to_project_for_id(
                user=self.author, project_id=project_id, readonly=True
            )

        with self.assertRaises(Forbidden):
            await self.pttable.get_and_check_access_to_project_for_name(
                user=self.author, project_name=g, readonly=True
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
        await self.pttable.set_group_members(
            group_name=self.pttable.get_project_group_name(g, readonly=True),
            members=[self.author],
            author=self.author,
        )

        project_for_id = await self.pttable.get_and_check_access_to_project_for_id(
            user=self.author, project_id=pid, readonly=True
        )
        self.assertEqual(pid, project_for_id.id)

        project_for_name = await self.pttable.get_and_check_access_to_project_for_name(
            user=self.author, project_name=g, readonly=True
        )
        self.assertEqual(pid, project_for_name.id)

    @run_as_sync
    async def test_get_my_projects(self):
        """
        Test that a user with permission only has MY projects
        """
        await self._add_group_member_direct(GROUP_NAME_PROJECT_CREATORS)
        await self._add_group_member_direct(GROUP_NAME_MEMBERS_ADMIN)

        g = str(uuid.uuid4())

        pid = await self.pttable.create_project(g, g, self.author)

        await self.pttable.set_group_members(
            group_name=self.pttable.get_project_group_name(g, readonly=True),
            members=[self.author],
            author=self.author,
        )

        projects = await self.pttable.get_projects_accessible_by_user(
            author=self.author
        )

        self.assertEqual(1, len(projects))
        self.assertEqual(pid, projects[0].id)
