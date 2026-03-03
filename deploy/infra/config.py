from typing import Any

import pulumi_gcp as gcp
from pulumi import Config, get_stack
from pydantic import BaseModel


class CommonConfig(BaseModel):
    """Config that is common across infra modules"""

    vpc_network: str | None = None
    vpc_subnet: str | None = None


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
    common: CommonConfig

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
    project_config = Config()

    project_info = gcp.organizations.get_project()
    project = project_info.project_id or gcp_config.require('project')

    # Load VPC config from common section (optional)
    common_config: dict[str, Any] = project_config.get_object('common') or {}
    common = CommonConfig.model_validate(common_config)

    # Load server configuration
    server_config = project_config.require_object('server')
    server = ServerConfig.model_validate(server_config)

    # Load migrations configuration
    migrations_config = project_config.require_object('migrations')
    migrations = MigrationsConfig(
        db_credentials_secret_name=migrations_config['db_credentials_secret_name'],
        github_token_secret_name=migrations_config['github_token_secret_name'],
    )

    return InfraConfig(
        stack=stack,
        project=project,
        region=gcp_config.require('region'),
        common=common,
        server=server,
        migrations=migrations,
    )
