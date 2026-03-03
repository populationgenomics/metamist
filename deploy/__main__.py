import pulumi
from infra.common import create_common_resources
from infra.config import load_config
from infra.migrations import create_migration_resources

from deploy.infra.server import create_server_resources


# Load shared configuration
config = load_config()

# Create infrastructure resources
common = create_common_resources(config)
server = create_server_resources(config, common.image_repository)
migrations = create_migration_resources(config, common.image_repository)

# Export key resource identifiers
pulumi.export('load balancer ip', server.ip_address.address)
pulumi.export('migration job name', migrations.job.name)
