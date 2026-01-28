import pytest

from db.python.connect import Connection
from db.python.tables.project import ProjectPermissionsTable
from models.models.project import ProjectMemberRole


class TestConnectionNoProject:
    """Tests for ProjectPermissionsTable."""

    @pytest.mark.asyncio
    async def test_get_projects_accessible_by_user_empty(
        self,
        connection: Connection,
    ) -> None:
        """Test that a user with no project access gets empty results."""
        table = ProjectPermissionsTable(connection)

        project_id_map, project_name_map = await table.get_projects_accessible_by_user(
            user='unknown-user@example.com'
        )

        assert project_id_map == {}
        assert project_name_map == {}

    @pytest.mark.asyncio
    async def test_get_projects_accessible_by_user_with_access(
        self, connection: Connection
    ) -> None:
        """Test that a user with project access gets their projects."""
        from test.conftest import TEST_USER

        table = ProjectPermissionsTable(connection)

        _, project_name_map = await table.get_projects_accessible_by_user(
            user=TEST_USER
        )

        # User should have access to test-project
        assert 'test-project' in project_name_map
        project = project_name_map['test-project']
        assert project.name == 'test-project'
        assert project.dataset == 'test-dataset'

        # User should have reader role (direct) and project_admin (from project-creators group)
        assert ProjectMemberRole.reader in project.roles


class TestConnectionWithProject:
    """Tests demonstrating connection_with_project fixture usage."""

    @pytest.mark.asyncio
    async def test_connection_has_project(
        self,
        connection_with_project: Connection,
    ) -> None:
        """Test that connection_with_project has the test project attached."""
        assert connection_with_project.project is not None
        assert connection_with_project.project.name == 'test-project'
        assert connection_with_project.project_id is not None

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['reader'])
    async def test_connection_can_check_access(
        self,
        connection_with_project: Connection,
    ) -> None:
        """Test that connection can check access to the attached project."""
        from models.models.project import ReadAccessRoles

        # This should not raise - user has reader role
        connection_with_project.check_access(ReadAccessRoles)

    @pytest.mark.asyncio
    async def test_connection_all_projects(
        self,
        connection_with_project: Connection,
    ) -> None:
        """Test that connection.all_projects() returns accessible projects."""
        projects = connection_with_project.all_projects()

        assert len(projects) == 1
        assert projects[0].name == 'test-project'
