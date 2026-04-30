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
    slack_token_secret_name: str
    enable_gcp_logging: str
    logging_level: str
    sample_prefix: str
    sample_check_offset: str
    environment: str
    seqr_url: str
    seqr_audience: str
    seqr_map_location: str
    seqr_slack_notification_channel: str | None = None
    sequencing_group_prefix: str
    sequencing_group_check_offset: str
    gcp_bq_aggreg_view: str
    gcp_bq_aggreg_raw: str
    gcp_bq_aggreg_ext_view: str
    gcp_bq_budget_view: str
    gcp_bq_billing_view: str
    gcp_bq_batches_view: str
    oauth_audience: str
    sm_legacy_proxy_sa: str

    # Cloud run and db tuning
    db_min_pool_size: int
    db_max_pool_size: int
    cloudrun_min_instances: int
    cloudrun_max_instances: int
    cloudrun_max_concurrent_requests: int


class MigrationsConfig(BaseModel):
    """Migrations-specific configuration."""

    db_credentials_secret_name: str
    github_token_secret_name: str
    allowed_repository: str
    allowed_branch: str | None = None


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

    # Load the entire metamist config object
    metamist_config: dict[str, Any] = project_config.require_object('metamist')

    # Load VPC config from common section (optional)
    common_config: dict[str, Any] = metamist_config.get('common') or {}
    common = CommonConfig.model_validate(common_config)

    # Load server configuration
    server_config = metamist_config['server']
    server = ServerConfig.model_validate(server_config)

    # Load migrations configuration
    migrations_config = metamist_config['migrations']
    migrations = MigrationsConfig.model_validate(migrations_config)

    return InfraConfig(
        stack=stack,
        project=project,
        region=gcp_config.require('region'),
        common=common,
        server=server,
        migrations=migrations,
    )
