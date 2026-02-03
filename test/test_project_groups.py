import uuid

import pytest

from db.python.connect import Connection
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
from test.conftest import TEST_USER


class TestGroupAccess:
    """
    Test project access permissions + newish internal group implementation
    """

    @pytest.mark.asyncio
    async def test_project_creators_failed(
        self,
        connection: Connection,
    ) -> None:
        """
        Test that a user without permission cannot create a project
        """
        pttable = ProjectPermissionsTable(connection)

        with pytest.raises(Forbidden):
            await pttable.create_project(
                'another-test-project', 'another-test-project', TEST_USER
            )

    @pytest.mark.asyncio
    @pytest.mark.admin_groups([GROUP_NAME_PROJECT_CREATORS])
    async def test_project_create_succeed(
        self,
        connection: Connection,
    ) -> None:
        """
        Test that a user with permission can create a project,
        and that read/write groups are created
        """
        pttable = ProjectPermissionsTable(connection)
        g = str(uuid.uuid4())

        project_id = await pttable.create_project(g, g, TEST_USER)

        project_id_map, _ = await pttable.get_projects_accessible_by_user(
            user=TEST_USER
        )

        project = project_id_map.get(project_id)
        assert project is not None
        assert project.name == g


class TestProjectAccess:
    """Test project access methods directly"""

    @pytest.mark.asyncio
    @pytest.mark.admin_groups([GROUP_NAME_PROJECT_CREATORS])
    async def test_no_project_access(
        self,
        connection: Connection,
    ) -> None:
        """
        Test that a user without permission cannot access a project
        """
        pttable = ProjectPermissionsTable(connection)
        g = str(uuid.uuid4())

        project_id = await pttable.create_project(g, g, TEST_USER)

        # Need to refresh projects to see the new project
        await connection.refresh_projects()

        with pytest.raises(Forbidden):
            connection.check_access_to_projects_for_ids(
                project_ids=[project_id], allowed_roles=ReadAccessRoles
            )

        with pytest.raises(Forbidden):
            connection.get_and_check_access_to_projects_for_names(
                project_names=[g], allowed_roles=ReadAccessRoles
            )

    @pytest.mark.asyncio
    @pytest.mark.admin_groups([GROUP_NAME_PROJECT_CREATORS, GROUP_NAME_MEMBERS_ADMIN])
    async def test_project_access_success(
        self,
        connection: Connection,
    ) -> None:
        """
        Test that a user with permission CAN access a project
        """
        pttable = ProjectPermissionsTable(connection)
        g = str(uuid.uuid4())

        pid = await pttable.create_project(g, g, TEST_USER)

        project_id_map, _ = await pttable.get_projects_accessible_by_user(
            user=TEST_USER
        )
        project = project_id_map.get(pid)
        assert project is not None

        await pttable.set_project_members(
            project=project,
            members=[ProjectMemberUpdate(member=TEST_USER, roles=['reader'])],
        )

        # Need to refresh projects to see updated permissions
        await connection.refresh_projects()

        project_for_id = connection.get_and_check_access_to_projects_for_ids(
            project_ids=[pid], allowed_roles=ReadAccessRoles
        )
        user_project_for_id = next(p for p in project_for_id)
        assert pid == user_project_for_id.id

        project_for_name = connection.get_and_check_access_to_projects_for_names(
            project_names=[g], allowed_roles=ReadAccessRoles
        )
        user_project_for_name = next(p for p in project_for_name)
        assert g == user_project_for_name.name

    @pytest.mark.asyncio
    @pytest.mark.admin_groups([GROUP_NAME_PROJECT_CREATORS, GROUP_NAME_MEMBERS_ADMIN])
    async def test_project_access_insufficient(
        self,
        connection: Connection,
    ) -> None:
        """
        Test that a user with access to a project will be disallowed if their access is
        not sufficient
        """
        pttable = ProjectPermissionsTable(connection)
        g = str(uuid.uuid4())

        pid = await pttable.create_project(g, g, TEST_USER)

        project_id_map, _ = await pttable.get_projects_accessible_by_user(
            user=TEST_USER
        )
        project = project_id_map.get(pid)
        assert project is not None

        # Give the user read access to the project
        await pttable.set_project_members(
            project=project,
            members=[ProjectMemberUpdate(member=TEST_USER, roles=['reader'])],
        )

        # Need to refresh projects to see updated permissions
        await connection.refresh_projects()

        # But require Write access
        with pytest.raises(Forbidden):
            connection.check_access_to_projects_for_ids(
                project_ids=[project.id], allowed_roles=FullWriteAccessRoles
            )

        with pytest.raises(Forbidden):
            connection.get_and_check_access_to_projects_for_names(
                project_names=[g], allowed_roles=FullWriteAccessRoles
            )

    @pytest.mark.asyncio
    @pytest.mark.admin_groups([GROUP_NAME_PROJECT_CREATORS, GROUP_NAME_MEMBERS_ADMIN])
    async def test_get_my_projects(
        self,
        connection: Connection,
    ) -> None:
        """
        Test that a user with permission only has MY projects
        """

        pttable = ProjectPermissionsTable(connection)
        g = str(uuid.uuid4())

        pid = await pttable.create_project(g, g, TEST_USER)

        project_id_map, _ = await pttable.get_projects_accessible_by_user(
            user=TEST_USER
        )
        project = project_id_map.get(pid)
        assert project is not None

        # Give the user contributor access to the project
        await pttable.set_project_members(
            project=project,
            members=[ProjectMemberUpdate(member=TEST_USER, roles=['contributor'])],
        )

        # Need to refresh projects to see updated permissions
        await connection.refresh_projects()

        (
            project_id_map,
            project_name_map,
        ) = await pttable.get_projects_accessible_by_user(user=TEST_USER)

        # Get projects with at least contributor access role
        my_projects = connection.projects_with_role({ProjectMemberRole.contributor})

        assert len(project_id_map) == len(project_name_map)
        assert len(my_projects) == 1
        assert pid == my_projects[0].id

    @pytest.mark.asyncio
    @pytest.mark.admin_groups([GROUP_NAME_PROJECT_CREATORS])
    async def test_delete_project_data(
        self,
        connection: Connection,
    ) -> None:
        """
        Test deleting all project data
        """
        pttable = ProjectPermissionsTable(connection)

        main_pid = await pttable.create_project('foo', 'foo_data', TEST_USER)
        test_pid = await pttable.create_project('a-test', 'a_data', TEST_USER)

        pid_map, _ = await pttable.get_projects_accessible_by_user(TEST_USER)

        with pytest.raises(ValueError):  # deleting non-test project not supported
            await pttable.delete_project_data(pid_map[main_pid])

        assert await pttable.delete_project_data(pid_map[test_pid]) is True
