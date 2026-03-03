import json
from dataclasses import dataclass

import pulumi
import pulumi_docker as docker
import pulumi_gcp as gcp
from pulumi_docker import BuilderVersion

from .config import InfraConfig


@dataclass
class ServerResources:
    """Useful outputs from api resources"""

    ip_address: gcp.compute.GlobalAddress


def create_server_resources(
    config: InfraConfig, image_repository: gcp.artifactregistry.Repository
) -> ServerResources:
    """
    Create infra for metamist server
    """
    # Build server Docker image
    image = docker.Image(
        'metamist-image',
        image_name=f'{image_repository.registry_uri}/metamist-image:latest',
        build=docker.DockerBuildArgs(
            context='../',
            dockerfile='./api/Dockerfile',
            args={'BUILDKIT_INLINE_CACHE': '1'},
            builder_version=BuilderVersion.BUILDER_BUILD_KIT,
            platform='linux/amd64',
        ),
        opts=pulumi.ResourceOptions(depends_on=[image_repository]),
    )

    # Service account for API
    service_account = gcp.serviceaccount.Account(
        'metamist-service-account',
        account_id=f'metamist-{config.stack}',
        display_name='metamist service account',
    )

    # Cloud Run service
    cloud_run = gcp.cloudrunv2.Service(
        'metamist',
        name=f'metamist-{config.stack}',
        ingress='INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER',
        location=config.region,
        default_uri_disabled=True,
        template=gcp.cloudrunv2.ServiceTemplateArgs(
            service_account=service_account.email,
            timeout='300s',
            scaling=gcp.cloudrunv2.ServiceTemplateScalingArgs(
                min_instance_count=0,
                max_instance_count=10,
            ),
            vpc_access=(
                gcp.cloudrunv2.ServiceTemplateVpcAccessArgs(
                    network_interfaces=[
                        gcp.cloudrunv2.ServiceTemplateVpcAccessNetworkInterfaceArgs(
                            network=config.db_vpc.network,
                            subnetwork=config.db_vpc.subnet,
                        )
                    ],
                    egress='PRIVATE_RANGES_ONLY',
                )
                if config.db_vpc
                else None
            ),
            containers=[
                gcp.cloudrunv2.ServiceTemplateContainerArgs(
                    image=image.repo_digest,
                    commands=['hypercorn', '--bind', '0.0.0.0:8080', 'api.server:app'],
                    resources=gcp.cloudrunv2.ServiceTemplateContainerResourcesArgs(
                        limits={'memory': '8Gi', 'cpu': '4'},
                        startup_cpu_boost=True,
                    ),
                    envs=[],
                    ports=gcp.cloudrunv2.ServiceTemplateContainerPortsArgs(
                        name='h2c',
                        container_port=8080,
                    ),
                ),
            ],
        ),
    )

    # IAP service identity
    iap_sa = gcp.projects.ServiceIdentity(
        'metamist-iap-sa',
        project=config.project,
        service='iap.googleapis.com',
    )

    # Grant IAP access to invoke Cloud Run
    gcp.cloudrunv2.ServiceIamMember(
        'metamist-iap-cloudrun-role',
        project=config.project,
        location=config.region,
        name=cloud_run.name,
        role='roles/run.invoker',
        member=iap_sa.email.apply(lambda email: f'serviceAccount:{email}'),
    )

    # Network endpoint group for serverless NEG
    neg = gcp.compute.RegionNetworkEndpointGroup(
        'metamist-neg',
        network_endpoint_type='SERVERLESS',
        region=config.region,
        cloud_run=gcp.compute.RegionNetworkEndpointGroupCloudRunArgs(
            service=cloud_run.name,
        ),
    )

    # OAuth config from Secret Manager
    oauth_config_secret = gcp.secretmanager.get_secret_version(
        secret=config.server.oauth_client_config_secret_name
    )
    oauth_config = json.loads(oauth_config_secret.secret_data)['web']

    # Backend service with IAP
    backend_service = gcp.compute.BackendService(
        'metamist-backend-service',
        enable_cdn=False,
        log_config=gcp.compute.BackendServiceLogConfigArgs(enable=True),
        protocol='HTTPS',
        backends=[gcp.compute.BackendServiceBackendArgs(group=neg.id)],
        iap=gcp.compute.BackendServiceIapArgs(
            enabled=True,
            oauth2_client_id=pulumi.Output.secret(oauth_config['client_id']),
            oauth2_client_secret=pulumi.Output.secret(oauth_config['client_secret']),
        ),
    )

    # Global IP address
    ip_address = gcp.compute.GlobalAddress(
        'metamist-ip-address',
        name=f'metamist-ip-address-{config.stack}',
    )

    # SSL certificate
    ssl_cert = gcp.compute.ManagedSslCertificate(
        'metamist-ssl-cert',
        name=f'metamist-ssl-cert-{config.stack}',
        managed={'domains': [config.server.web_domain]},
    )

    # URL maps
    url_map = gcp.compute.URLMap(
        'metamist-url-map',
        name=f'metamist-url-map-{config.stack}',
        default_service=backend_service.id,
    )

    http_redirect_url_map = gcp.compute.URLMap(
        'metamist-http-redirect-url-map',
        name=f'metamist-http-redirect-url-map-{config.stack}',
        default_url_redirect={
            'https_redirect': True,
            'redirect_response_code': 'MOVED_PERMANENTLY_DEFAULT',
            'strip_query': False,
        },
    )

    # HTTP/HTTPS proxies
    http_proxy = gcp.compute.TargetHttpProxy(
        'metamist-http-proxy',
        name=f'metamist-http-proxy-{config.stack}',
        url_map=http_redirect_url_map.id,
    )

    https_proxy = gcp.compute.TargetHttpsProxy(
        'metamist-https-proxy',
        name=f'metamist-https-proxy-{config.stack}',
        url_map=url_map.id,
        ssl_certificates=[ssl_cert.id],
    )

    # Forwarding rules
    gcp.compute.GlobalForwardingRule(
        'metamist-forwarding-rule',
        name=f'metamist-forwarding-rule-{config.stack}',
        target=https_proxy.id,
        port_range='443',
        ip_address=ip_address.address,
    )

    gcp.compute.GlobalForwardingRule(
        'metamist-http-forwarding-rule',
        name=f'metamist-http-forwarding-rule-{config.stack}',
        target=http_proxy.id,
        port_range='80',
        ip_address=ip_address.address,
    )

    return ServerResources(
        ip_address=ip_address,
    )
