import pytest
from httpx import AsyncClient

from db.python.tables.project import GROUP_NAME_PROJECT_CREATORS
from test.conftest import GraphQLQueryFunction


class TestProjectListing:
    """Tests for project listing functionality."""

    @pytest.mark.asyncio
    @pytest.mark.project_roles([])
    async def test_my_projects_returns_empty_when_no_projects(
        self,
        graphql_query: GraphQLQueryFunction,
    ) -> None:
        """Test that myProjects returns empty list when user has no projects."""
        query = """
            query {
                myProjects {
                    id
                    name
                    dataset
                }
            }
        """

        data = await graphql_query(query)

        assert 'errors' not in data
        assert data['data']['myProjects'] == []

    @pytest.mark.asyncio
    async def test_my_projects_returns_accessible_projects(
        self,
        app_client: AsyncClient,
    ) -> None:
        """Test that myProjects returns projects the user has access to."""
        query = """
            query {
                myProjects {
                    id
                    name
                    dataset
                    meta
                    roles
                }
            }
        """

        response = await app_client.post(
            '/graphql',
            json={'query': query},
        )

        assert response.status_code == 200  # noqa: PLR2004
        data = response.json()
        assert 'errors' not in data

        projects = data['data']['myProjects']
        assert len(projects) == 1

        project = projects[0]
        assert project['name'] == 'test-project'
        assert project['dataset'] == 'test-dataset'
        assert 'reader' in project['roles']

    @pytest.mark.asyncio
    @pytest.mark.admin_groups([GROUP_NAME_PROJECT_CREATORS])
    async def test_creating_projects_concurrently_works(
        self,
        app_client: AsyncClient,
    ) -> None:
        """Test that myProjects returns projects the user has access to."""
        query = """
            mutation CreateProject {
                project {
                    p1: createProject(name: "project-1", dataset: "project-1", createTestProject: false) {
                        name
                        dataset
                    }
                    p2: createProject(name: "project-2", dataset: "project-2", createTestProject: false) {
                        name
                        dataset
                    }
                }
            }
        """

        response = await app_client.post(
            '/graphql',
            json={'query': query},
        )

        assert response.status_code == 200  # noqa: PLR2004
        data = response.json()
        assert 'errors' not in data

        project1 = data['data']['project']['p1']
        assert project1['name'] == 'project-1'

        project2 = data['data']['project']['p2']
        assert project2['name'] == 'project-2'
