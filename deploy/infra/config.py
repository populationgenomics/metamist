import pulumi_gcp as gcp
from pulumi import Config, get_stack
from pydantic import BaseModel


class DatabaseVpcConfig(BaseModel):
    """Config for the main VPC that metamist uses to connect to the database"""

    network: str
    subnet: str


class ServerConfig(BaseModel):
    """Server-specific configuration."""

    web_domain: str
    oauth_client_config_secret_name: str
    db_credentials_secret_name: str
    iap_audience: str | None = None


class MigrationsConfig(BaseModel):
    """Migrations-specific configuration."""

    db_credentials_secret_name: str
    github_token_secret_name: str


class InfraConfig(BaseModel):
    """Infrastructure configuration."""

    stack: str
    project: str  # GCP project ID
    region: str

    # VPC settings (production only)
    db_vpc: DatabaseVpcConfig | None

    # Server configuration
    server: ServerConfig

    # Migrations configuration
    migrations: MigrationsConfig

    @property
    def registry_url(self) -> str:
        return f'{self.region}-docker.pkg.dev/{self.project}'


def load_config() -> InfraConfig:
    """Load configuration from Pulumi config."""
    stack = get_stack()
    gcp_config = Config('gcp')
    common_config = Config('metamist:common')
    server_config = Config('metamist:server')
    migrations_config = Config('metamist:migrations')

    project_info = gcp.organizations.get_project()
    project = project_info.project_id or gcp_config.require('project')

    # Load VPC config from common section (optional)
    vpc_network = common_config.get('vpc_network')
    vpc_subnet = common_config.get('vpc_subnet')
    vpc_config = (
        DatabaseVpcConfig(network=vpc_network, subnet=vpc_subnet)
        if vpc_network and vpc_subnet
        else None
    )

    # Load server configuration
    server = ServerConfig(
        web_domain=server_config.require('web_domain'),
        oauth_client_config_secret_name=server_config.require('oauth_client_config_secret_name'),
        db_credentials_secret_name=server_config.require('db_credentials_secret_name'),
        iap_audience=server_config.get('iap_audience'),
    )

    # Load migrations configuration
    migrations = MigrationsConfig(
        db_credentials_secret_name=migrations_config.require('db_credentials_secret_name'),
        github_token_secret_name=migrations_config.require('github_token_secret_name'),
    )

    return InfraConfig(
        stack=stack,
        project=project,
        region=gcp_config.require('region'),
        db_vpc=vpc_config,
        server=server,
        migrations=migrations,
    )
