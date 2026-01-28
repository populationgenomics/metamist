"""
Pytest configuration and fixtures for metamist tests.

Uses testcontainers with a custom PostgreSQL image that includes temporal_tables extension.
Uses PostgreSQL template database pattern for fast test database creation.
Runs dbmate migrations inside the container for schema setup.
"""

import uuid
from collections.abc import AsyncGenerator, Generator
from pathlib import Path
from typing import Any, Awaitable, Protocol

import psycopg
import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from psycopg import AsyncConnection, sql
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool
from testcontainers.core.container import DockerContainer
from testcontainers.core.image import DockerImage
from testcontainers.core.waiting_utils import wait_for_logs

from api import settings
from api.server import app
from db.python.connect import (
    Connection,
    SMConnections,
    configure_pg_connection,
)
from db.python.tables.project import (
    GROUP_NAME_MEMBERS_ADMIN,
    GROUP_NAME_PROJECT_CREATORS,
)
from models.models.project import ProjectMemberRole

# Path to the db directory containing Dockerfile and migrations
DB_DIR = Path(__file__).parent.parent / 'db'

# Default test user for authentication
TEST_USER = 'testuser@example.com'


class PostgresContainer(DockerContainer):
    """Custom PostgreSQL container with temporal_tables extension and dbmate."""

    POSTGRES_USER = 'test_user'
    POSTGRES_PASSWORD = 'test_password'
    POSTGRES_DB = 'template_metamist'  # This will be our template database
    POSTGRES_PORT = 5432

    def __init__(self, image: str = 'metamist-postgres-test:latest'):
        super().__init__(image)
        self.with_exposed_ports(self.POSTGRES_PORT)
        self.with_env('POSTGRES_USER', self.POSTGRES_USER)
        self.with_env('POSTGRES_PASSWORD', self.POSTGRES_PASSWORD)
        self.with_env('POSTGRES_DB', self.POSTGRES_DB)
        # Use /dev/shm which is an in-memory tmpfs mount, using this for the postgres
        # data dir will speed up the data copying done by tests
        self.with_env('PGDATA', '/dev/shm/pgdata')
        # Increase the shared memory size for PostgreSQL, default is 64MB
        self.with_kwargs(shm_size='512m')

        # Mount the migrations directory so that dbmate migrations can run
        migrations_path = str(DB_DIR / 'migrations')
        self.with_volume_mapping(migrations_path, '/db/migrations', mode='ro')

        # Tweak postgres settings to improve performance for testing. These would
        # be very bad settings for prod, but we don't need durability in tests.
        self.with_command(
            'postgres '
            '-c wal_level=minimal '  # Reduces WAL logging overhead
            '-c max_wal_senders=0 '  # Required for wal_level=minimal
            '-c fsync=off '  # Skip fsync calls (data durability not needed in tests)
            '-c synchronous_commit=off '  # Don't wait for WAL writes
            '-c full_page_writes=off '  # Skip full page writes after checkpoint
            '-c checkpoint_timeout=1d '  # Avoid checkpoints during test run
            '-c max_wal_size=1GB'  # Allow more WAL before checkpoint
        )

    def get_connection_url(self, database: str | None = None) -> str:
        """Get the connection URL for the container. This is the connection used by tests"""
        host = self.get_container_host_ip()
        port = self.get_exposed_port(self.POSTGRES_PORT)
        db = database or self.POSTGRES_DB
        return f'postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{host}:{port}/{db}'

    def get_internal_database_url(self, database: str | None = None) -> str:
        """Get the database URL for use inside the container, this is used for migrations"""
        db = database or self.POSTGRES_DB
        return f'postgres://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@localhost:5432/{db}'

    def start(self) -> 'PostgresContainer':
        """Start the container and wait for PostgreSQL to be ready."""
        super().start()
        # Wait for logs first (container startup)
        wait_for_logs(
            self, 'database system is ready to accept connections', timeout=30
        )
        return self

    def run_migrations(self) -> None:
        """Run dbmate migrations inside the container."""
        database_url = self.get_internal_database_url()
        exit_code, output = self.exec(
            f'dbmate --url "{database_url}?sslmode=disable&search_path=main" --migrations-dir /db/migrations --no-dump-schema migrate'
        )
        if exit_code != 0:
            raise RuntimeError(
                f'dbmate migration failed with exit code {exit_code}: {output.decode()}'
            )


def _mark_database_as_template(postgres_container: PostgresContainer) -> None:
    """Fixture to ensure the template database is set up before tests run."""
    database_url = postgres_container.get_connection_url('postgres')
    template_db = PostgresContainer.POSTGRES_DB
    # Connect to default postgres database, as we can't be connected to the
    # template database when we mark it as a template
    with psycopg.connect(database_url, autocommit=True) as conn:
        conn.execute(
            sql.SQL('ALTER DATABASE {template_db} WITH is_template = true').format(
                template_db=sql.Identifier(template_db),
            )
        )


def _drop_database(connection_url: str, db_name: str) -> None:
    """Drop a database."""
    postgres_url = connection_url.rsplit('/', 1)[0] + '/postgres'
    with psycopg.connect(postgres_url, autocommit=True) as conn:
        conn.execute(
            sql.SQL('DROP DATABASE IF EXISTS {db_name}').format(
                db_name=sql.Identifier(db_name),
            )
        )


@pytest.fixture(scope='session')
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """
    Pytest fixture to provide a postgres container to tests. This will run db migrations
    so that all the necessary tables exist in the database that will be used as a template
    """
    # Build the image using testcontainers' DockerImage
    # clean_up=False keeps the image for faster subsequent test runs
    with DockerImage(
        path=str(DB_DIR),
        tag='metamist-postgres-test:latest',
        clean_up=False,
    ) as image:
        # Start the container with the built image
        container = PostgresContainer(image=str(image))
        container.start()

        try:
            # Run dbmate migrations inside the container to apply schema
            container.run_migrations()
            _mark_database_as_template(container)
            yield container
        finally:
            container.stop()


@pytest.fixture
def test_db_url(postgres_container: PostgresContainer) -> Generator[str, None, None]:
    """
    Fixture that creates a fresh database for each test.

    This uses postges's template database functionality to make the creation of
    each database very fast.
    """
    # random database name
    db_name = f'test_{uuid.uuid4().hex[:12]}'

    # Create database from template
    base_url = postgres_container.get_connection_url('postgres')

    with psycopg.connect(base_url, autocommit=True) as conn:
        conn.execute(
            sql.SQL('CREATE DATABASE {db_name} TEMPLATE {template_db}').format(
                db_name=sql.Identifier(db_name),
                template_db=sql.Identifier(PostgresContainer.POSTGRES_DB),
            ),
        )

    yield postgres_container.get_connection_url(db_name)

    # Once tests are done, then drop the database
    _drop_database(base_url, db_name)


@pytest.fixture
async def db_pool(
    test_db_url: str,
) -> AsyncGenerator[AsyncConnectionPool[AsyncConnection[DictRow]], None]:
    """
    Async fixture that provides a connection pool for the test database.
    """
    pool: AsyncConnectionPool[AsyncConnection[DictRow]] = AsyncConnectionPool(
        conninfo=test_db_url,
        open=False,
        min_size=1,
        max_size=5,
        configure=configure_pg_connection,
        kwargs={'row_factory': dict_row},
    )  # type: ignore # psycopg doesn't know that row factory will change the row type

    await pool.open()

    yield pool

    await pool.close()


@pytest.fixture
def configured_app(
    db_pool: AsyncConnectionPool[AsyncConnection[DictRow]],
    monkeypatch: pytest.MonkeyPatch,
) -> FastAPI:
    """
    Set up the metamist FastAPI app, and patch necessary settings for testing.
    """
    # Patch the SMConnections class to use our test pool
    monkeypatch.setattr(SMConnections, '_postgres_pool', db_pool)

    # Patch settings module directly (env vars are read at import time)
    monkeypatch.setattr(settings, 'SM_ENVIRONMENT', 'test')
    monkeypatch.setattr(settings, '_DEFAULT_USER', TEST_USER)

    return app


@pytest.fixture
async def seeded_db(
    request: pytest.FixtureRequest,
    db_pool: AsyncConnectionPool,
) -> None:
    """
    Seed the test database with basic test data.

    Roles and admin groups can be configured via the project_roles and admin_groups markers:

        @pytest.mark.project_roles(['reader', 'writer'])
        @pytest.mark.admin_groups(['project-creators'])
        async def test_something(connection_with_project):
            ...
    """
    # Get roles from marker, or default to reader
    project_roles_marker = request.node.get_closest_marker('project_roles')
    roles: list[str] = (
        project_roles_marker.args[0] if project_roles_marker else ['reader']
    )

    # validate roles
    for role in roles:
        if role not in ProjectMemberRole.__members__:
            raise ValueError(f'Invalid project role in marker: {role}')

    # Admin groups from marker
    admin_groups_marker = request.node.get_closest_marker('admin_groups')
    admin_groups: list[str] = admin_groups_marker.args[0] if admin_groups_marker else []

    # validate admin groups
    valid_admin_groups = {GROUP_NAME_PROJECT_CREATORS, GROUP_NAME_MEMBERS_ADMIN}
    for group in admin_groups:
        if group not in valid_admin_groups:
            raise ValueError(f'Invalid admin group in marker: {group}')

    async with db_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # Create groups for project creators and members admin
            await cur.execute("""
                INSERT INTO "group" (name)
                VALUES ('project-creators'), ('members-admin')
                RETURNING id
            """)

            # Add test user to project-creators group
            await cur.execute(
                """
                INSERT INTO group_member(group_id, member)
                SELECT id, %(user)s
                FROM "group" WHERE name = ANY(%(group_names)s)
                """,
                {'user': TEST_USER, 'group_names': admin_groups},
            )

            # Create a test project and get its ID
            await cur.execute("""
                INSERT INTO project (name, dataset, meta)
                VALUES ('test-project', 'test-dataset', '{}')
                RETURNING id
            """)
            row = await cur.fetchone()
            assert row is not None
            project_id: int = row['id']

            # Add test user as project member with configured roles
            for role in roles:
                await cur.execute(
                    """
                    INSERT INTO project_member (project_id, member, role)
                    VALUES (%s, %s, %s)
                    """,
                    (project_id, TEST_USER, role),
                )

        await conn.commit()


@pytest.fixture
async def app_client(
    configured_app: FastAPI,
    seeded_db: None,  # Fixture dependency - ensures database is seeded first
) -> AsyncGenerator[AsyncClient, None]:
    """
    Httpx client for making requests to the metamist app.
    """
    transport = ASGITransport(app=configured_app)
    async with AsyncClient(transport=transport, base_url='http://test') as client:
        yield client


# Type def for graphql query function
class GraphQLQueryFunction(Protocol):
    def __call__(
        self, query: str, variables: dict[str, Any] | None = None
    ) -> Awaitable[dict[str, Any]]: ...


@pytest.fixture
async def graphql_query(
    app_client: AsyncClient,
) -> AsyncGenerator[GraphQLQueryFunction, None]:
    """
    Fixture that provides a function to make GraphQL queries.
    """

    async def _query(
        query: str, variables: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        response = await app_client.post(
            '/graphql',
            json={'query': query, 'variables': variables},
        )
        response.raise_for_status()
        return response.json()

    yield _query


@pytest.fixture
async def connection(
    db_pool: AsyncConnectionPool[AsyncConnection[DictRow]],
    seeded_db: None,  # Fixture dependency - ensures database is seeded first
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[Connection, None]:
    """
    Provides a Connection object for direct database layer/table testing.

    This fixture patches the global postgres_pool and creates a Connection
    with the test user as author. The connection has no project attached by
    default - use `connection_with_project` for tests that require a project.

    Example:
        async def test_sample_layer(connection):
            layer = SampleLayer(connection)
            samples = await layer.get_by_id(1)
    """
    # Patch the SMConnections class to use our test pool
    monkeypatch.setattr(SMConnections, '_postgres_pool', db_pool)

    # Create a connection with empty project maps (no project access yet)
    conn = Connection(
        postgres_pool=db_pool,
        project=None,
        project_id_map={},
        project_name_map={},
        author=TEST_USER,
        on_behalf_of=None,
        ar_guid=None,
        meta={'test': 'true'},
    )

    yield conn


@pytest.fixture
async def connection_with_project(
    db_pool: AsyncConnectionPool[AsyncConnection[DictRow]],
    seeded_db: None,  # Fixture dependency - ensures database is seeded first
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[Connection, None]:
    """
    Provides a Connection object with an attached project for testing.

    Use this for testing layer/table methods that require project context.

    Example:
        async def test_sample_creation(connection_with_project):
            layer = SampleLayer(connection_with_project)
            sample_id = await layer.upsert_sample(...)
    """
    # Patch the SMConnections class to use our test pool
    monkeypatch.setattr(SMConnections, '_postgres_pool', db_pool)

    # Create a connection
    conn = Connection(
        postgres_pool=db_pool,
        project=None,
        project_id_map={},
        project_name_map={},
        author=TEST_USER,
        on_behalf_of=None,
        ar_guid=None,
        meta={'test': 'true'},
    )

    # Refresh projects to load the seeded test-project
    await conn.refresh_projects()

    # Set the project to test-project
    conn.update_project('test-project')

    yield conn


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers."""
    config.addinivalue_line(
        'markers',
        'project_roles(roles: list[str]): Specify roles for the test user in the seeded test project.',
    )
    config.addinivalue_line(
        'markers',
        'admin_groups(groups: list[str]): Specify admin groups for the test user in the seeded db.',
    )
