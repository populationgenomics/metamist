import pytest
from httpx import AsyncClient

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

        assert response.status_code == 200
        data = response.json()
        assert 'errors' not in data

        projects = data['data']['myProjects']
        assert len(projects) == 1

        project = projects[0]
        assert project['name'] == 'test-project'
        assert project['dataset'] == 'test-dataset'
        assert 'reader' in project['roles']
