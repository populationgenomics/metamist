from dataclasses import dataclass

import pulumi_gcp as gcp

from .config import InfraConfig


@dataclass
class CommonResources:
    """Common infrastructure resources."""

    image_repository: gcp.artifactregistry.Repository


def create_common_resources(config: InfraConfig) -> CommonResources:
    """
    Create any resources that are shared across sections of infra
    """
    repository = gcp.artifactregistry.Repository(
        'metamist-repository',
        location=config.region,
        repository_id=f'metamist-repository-{config.stack}',
        format='DOCKER',
        description='metamist docker repository',
        cleanup_policies=[
            {
                'id': 'delete-untagged',
                'action': 'DELETE',
                'condition': {'tag_state': 'UNTAGGED', 'older_than': '30d'},
            },
        ],
    )

    return CommonResources(image_repository=repository)
