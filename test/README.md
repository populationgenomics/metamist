# Metamist tests

## Overview

The test setup uses:

- **pytest** as the test framework
- **testcontainers** for spinning up a custom PostgreSQL container with the `temporal_tables` extension
- **PostgreSQL template databases** for fast test isolation


## Running Tests

```bash
# Run all tests
uv run pytest test/ -v

# Run a specific test file
uv run pytest test/test_project_graphql.py -v

# Run with coverage
uv run coverage run -m pytest test/
uv run coverage report -m # to view coverage in terminal
uv run coverage html # to generate a html report

```

## Test Fixtures

To make writing tests easy, there are several fixtures available

### `connection`

Provides a `Connection` object for direct database layer/table testing without any project attached. Use this for testing code that doesn't require project context.

```python
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
```

### `connection_with_project`

Provides a `Connection` object with the test project attached. Automatically seeds the database with test data. Use this for testing layer/table methods that require project context.

```python
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
```



### `app_client`

Instantiates the FastAPI app and provides it to the test. This can be used to test http routes.

```python
class TestProjectListing:
    """Tests for project listing functionality."""

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
```


### `graphql_query`

If you do just want to test a graphql query, there's a fixture that simplifies this process.

```python
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
```

## Seed data project roles and admin groups

You can configure which roles and admin groups the test user is a member of by using pytest marks.

```python
class TestProjectRoles:

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['reader', 'writer']) # Test user will have reader and writer roles
    async def test_reader_writer(
        self,
        graphql_query: GraphQLQueryFunction,
    ) -> None:
        ...


    @pytest.mark.asyncio
    @pytest.mark.admin_groups(['members-admin']) # Test user will be in the members-admin group
    async def test_members_admin(
        self,
        graphql_query: GraphQLQueryFunction,
    ) -> None:
        ...


```
