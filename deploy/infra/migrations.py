from dataclasses import dataclass

import pulumi
import pulumi_docker as docker
import pulumi_gcp as gcp
from pulumi_docker import BuilderVersion

from .config import InfraConfig


@dataclass
class MigrationResources:
    """Useful pieces of migration infra"""

    job: gcp.cloudrunv2.Job


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

    # Service account for migration job
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

    # Cloud Run Job for migrations
    migration_job = gcp.cloudrunv2.Job(
        'metamist-migration-job',
        name=f'metamist-migration-{config.stack}',
        location=config.region,
        template=gcp.cloudrunv2.JobTemplateArgs(
            task_count=1,
            template=gcp.cloudrunv2.JobTemplateTemplateArgs(
                service_account=migration_service_account.email,
                timeout='600s',
                max_retries=0,  # Don't retry migrations automatically
                vpc_access=(
                    gcp.cloudrunv2.JobTemplateTemplateVpcAccessArgs(
                        network_interfaces=[
                            gcp.cloudrunv2.JobTemplateTemplateVpcAccessNetworkInterfaceArgs(
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
                    gcp.cloudrunv2.JobTemplateTemplateContainerArgs(
                        image=migration_image.repo_digest,
                        resources=gcp.cloudrunv2.JobTemplateTemplateContainerResourcesArgs(
                            limits={
                                'memory': '512Mi',
                                'cpu': '1',
                            },
                        ),
                        envs=[
                            gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                                name='DATABASE_URL_SECRET',
                                value=f'projects/{config.project}/secrets/{config.migrations.db_credentials_secret_name}/versions/latest',
                            ),
                            gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                                name='MIGRATION_COMMAND',
                                value='status',  # Default to showing status (safe)
                            ),
                            gcp.cloudrunv2.JobTemplateTemplateContainerEnvArgs(
                                name='GITHUB_TOKEN_SECRET',
                                value=f'projects/{config.project}/secrets/{config.migrations.github_token_secret_name}/versions/latest',
                            ),
                            # GITHUB_REPOSITORY and GITHUB_REF are passed at
                            # execution time via workflow env var overrides
                        ],
                    ),
                ],
            ),
        ),
    )

    return MigrationResources(
        job=migration_job,
    )
