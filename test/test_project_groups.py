import uuid
from test.testbase import DbIsolatedTest, run_as_sync

from db.python.tables.project import (
    GROUP_NAME_MEMBERS_ADMIN,
    GROUP_NAME_PROJECT_CREATORS,
    Forbidden,
    ProjectPermissionsTable,
)
from db.python.utils import NotFoundError


class TestProjectAccess(DbIsolatedTest):
    """
    Test project access permissions + newish internal group implementation
    """

    @run_as_sync
    async def setUp(self):
        """Setup tests"""
        super().setUp()

        # specifically required to test permissions
        self.pttable = ProjectPermissionsTable(self.connection.connection, False)

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
            INSERT INTO group_member (group_id, member, author)
            VALUES (:group_id, :member, :author);
            """,
            {
                'group_id': members_admin_group,
                'member': self.author,
                'author': self.author,
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
        await self.pttable.gtable.create_group(g)

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
        project_id = await self.pttable.create_project(
            'another-test-project', 'another-test-project', self.author
        )

        # pylint: disable=protected-access
        project = await self.pttable._get_project_by_id(project_id)

        # test that the group names make sense
        self.assertIsNotNone(project.read_group_id)
        self.assertIsNotNone(project.write_group_id)
