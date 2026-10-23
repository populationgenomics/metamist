from dataclasses import dataclass

import pulumi
import pulumi_docker as docker
import pulumi_gcp as gcp
from pulumi_docker import BuilderVersion

from .config import InfraConfig


@dataclass
class MigrationResources:
    """Useful pieces of migration infra"""

    service: gcp.cloudrunv2.Service


def create_migration_resources(
    config: InfraConfig, image_repository: gcp.artifactregistry.Repository
) -> MigrationResources:
    """
    Create all db migration related infra resources.
    """
    # Docker image for migrations
    migration_image = docker.Image(
        'metamist-migration-image',
        image_name=image_repository.registry_uri.apply(
            lambda registry_uri: f'{registry_uri}/metamist-image:latest'
        ),
        build=docker.DockerBuildArgs(
            context='../',
            dockerfile='./migrations/Dockerfile',
            args={
                'BUILDKIT_INLINE_CACHE': '1',
            },
            builder_version=BuilderVersion.BUILDER_BUILD_KIT,
            platform='linux/amd64',
        ),
        opts=pulumi.ResourceOptions(depends_on=[image_repository]),
    )

    # Service account for migration service
    migration_service_account = gcp.serviceaccount.Account(
        'metamist-migration-service-account',
        account_id=f'metamist-migration-{config.stack}',
        display_name='metamist migration service account',
    )

    # Grant the migration service account access to the database credentials secret
    gcp.secretmanager.SecretIamMember(
        'metamist-migration-secret-accessor',
        secret_id=config.migrations.db_credentials_secret_name,
        role='roles/secretmanager.secretAccessor',
        member=migration_service_account.email.apply(
            lambda email: f'serviceAccount:{email}'
        ),
    )

    # Grant the migration service account access to the GitHub token secret
    gcp.secretmanager.SecretIamMember(
        'metamist-migration-github-token-accessor',
        secret_id=config.migrations.github_token_secret_name,
        role='roles/secretmanager.secretAccessor',
        member=migration_service_account.email.apply(
            lambda email: f'serviceAccount:{email}'
        ),
    )

    # Cloud Run Service for migrations
    migration_service = gcp.cloudrunv2.Service(
        'metamist-migration-service',
        name=f'metamist-migration-{config.stack}',
        location=config.region,
        labels={
            'metamist-private-sha': config.metamist_private_sha,
        },
        template=gcp.cloudrunv2.ServiceTemplateArgs(
            service_account=migration_service_account.email,
            timeout='600s',
            scaling=gcp.cloudrunv2.ServiceTemplateScalingArgs(
                min_instance_count=0,
                # We only need one instance for migrations to avoid concurrency issues
                max_instance_count=1,
            ),
            # Similarily, only allow one request at a time. Don't want multiple migrations running
            max_instance_request_concurrency=1,
            vpc_access=(
                gcp.cloudrunv2.ServiceTemplateVpcAccessArgs(
                    network_interfaces=[
                        gcp.cloudrunv2.ServiceTemplateVpcAccessNetworkInterfaceArgs(
                            network=config.common.vpc_network,
                            subnetwork=config.common.vpc_subnet,
                        )
                    ],
                    egress='PRIVATE_RANGES_ONLY',
                )
                if config.common.vpc_network is not None
                and config.common.vpc_subnet is not None
                else None
            ),
            containers=[
                gcp.cloudrunv2.ServiceTemplateContainerArgs(
                    image=migration_image.repo_digest,
                    resources=gcp.cloudrunv2.ServiceTemplateContainerResourcesArgs(
                        limits={
                            'memory': '512Mi',
                            'cpu': '1',
                        },
                    ),
                    envs=[
                        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                            name='DATABASE_URL_SECRET',
                            value_source=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceArgs(
                                secret_key_ref=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceSecretKeyRefArgs(
                                    secret=config.migrations.db_credentials_secret_name,
                                    version='latest',
                                )
                            ),
                        ),
                        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                            name='GITHUB_TOKEN_SECRET',
                            value_source=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceArgs(
                                secret_key_ref=gcp.cloudrunv2.ServiceTemplateContainerEnvValueSourceSecretKeyRefArgs(
                                    secret=config.migrations.github_token_secret_name,
                                    version='latest',
                                )
                            ),
                        ),
                        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                            name='ALLOWED_REPOSITORY',
                            value=config.migrations.allowed_repository,
                        ),
                        gcp.cloudrunv2.ServiceTemplateContainerEnvArgs(
                            name='ALLOWED_BRANCH',
                            value=config.migrations.allowed_branch or '',
                        ),
                    ],
                    ports=gcp.cloudrunv2.ServiceTemplateContainerPortsArgs(
                        container_port=8080,
                    ),
                ),
            ],
        ),
    )

    return MigrationResources(
        service=migration_service,
    )
