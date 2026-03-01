import json

import pulumi
import pulumi_docker as docker
import pulumi_gcp as gcp
from pulumi import Config, get_stack
from pulumi_docker import BuilderVersion


stack = get_stack()
project = gcp.organizations.get_project()
gcp_config = Config('gcp')
app_config = Config('app')


repository = gcp.artifactregistry.Repository(
    'metamist-repository',
    location=gcp_config.require('region'),
    repository_id=f'metamist-repository-{stack}',
    format='DOCKER',
    description='metamist docker repository',
    # Remove any versions that are older than 30 days and are untagged
    # this means that the latest version will always be kept but older
    # versions that are already deployed will not.
    cleanup_policies=[
        {
            'id': 'delete-untagged',
            'action': 'DELETE',
            'condition': {'tag_state': 'UNTAGGED', 'older_than': '30d'},
        },
    ],
)


image = docker.Image(
    'metamist-image',
    image_name=f'{gcp_config.require("region")}-docker.pkg.dev/{gcp_config.require("project")}/metamist-repository-{stack}/metamist-image:latest',
    build=docker.DockerBuildArgs(
        context='../../',
        dockerfile='Dockerfile',
        args={
            'BUILDKIT_INLINE_CACHE': '1',
        },
        builder_version=BuilderVersion.BUILDER_BUILD_KIT,
        platform='linux/amd64',
    ),
)


service_account = gcp.serviceaccount.Account(
    'metamist-service-account',
    account_id=f'metamist-{stack}',
    display_name='metamist service account',
)

cloud_run = gcp.cloudrunv2.Service(
    'metamist',
    name=f'metamist-{stack}',
    ingress='INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER',
    location=gcp_config.require('region'),
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
                        network=app_config.require('vpc_network'),
                        subnetwork=app_config.require('vpc_subnetwork'),
                    )
                ],
                egress='PRIVATE_RANGES_ONLY',
            )
            if stack == 'production'
            else None
        ),
        containers=[
            gcp.cloudrunv2.ServiceTemplateContainerArgs(
                image=image.repo_digest,
                commands=['hypercorn', '--bind', '0.0.0.0:8080', 'api.server:app'],
                resources=gcp.cloudrunv2.ServiceTemplateContainerResourcesArgs(
                    limits={
                        'memory': '8Gi',
                        'cpu': '4',
                    },
                    startup_cpu_boost=True,  # Allocate extra CPU during startup to improve cold start times
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


# Give IAP access to invoke cloudrun
iap_sa = gcp.projects.ServiceIdentity(
    'metamist-iap-sa',
    project=project.project_id,
    service='iap.googleapis.com',
)

cloud_run_iap_user = gcp.cloudrunv2.ServiceIamMember(
    'metamist-iap-cloudrun-role',
    project=project.project_id,
    location=gcp_config.require('region'),
    name=cloud_run.name,
    role='roles/run.invoker',
    member=iap_sa.email.apply(lambda email: f'serviceAccount:{email}'),
)


neg = gcp.compute.RegionNetworkEndpointGroup(
    'metamist-neg',
    network_endpoint_type='SERVERLESS',
    region=gcp_config.require('region'),
    cloud_run=gcp.compute.RegionNetworkEndpointGroupCloudRunArgs(
        service=cloud_run.name,
    ),
)


oauth_config_secret_name = (
    'metamist-oauth-client-config'
    if stack == 'production'
    else 'metamist-dev-oauth-client-config'
)
oauth_config_secret = gcp.secretmanager.get_secret_version(
    secret=oauth_config_secret_name
)
oauth_config = json.loads(oauth_config_secret.secret_data)['web']

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


# Give authenticated users access to the backend service via IAP
# Granular authorization controls are handled by the application itself
iap_https_member = gcp.iap.WebBackendServiceIamMember(
    'metamist-iap-https-member',
    web_backend_service=backend_service.name,
    role='roles/iap.httpsResourceAccessor',
    member='allAuthenticatedUsers',
)


ip_address = gcp.compute.GlobalAddress(
    'metamist-ip-address',
    name=f'metamist-ip-address-{stack}',
)

ssl_cert = gcp.compute.ManagedSslCertificate(
    'metamist-ssl-cert',
    name=f'metamist-ssl-cert-{stack}',
    managed={
        'domains': [app_config.require('domain')],
    },
)

url_map = gcp.compute.URLMap(
    'metamist-url-map',
    name=f'metamist-url-map-{stack}',
    default_service=backend_service.id,
)

http_redirect_url_map = gcp.compute.URLMap(
    'metamist-http-redirect-url-map',
    name=f'metamist-http-redirect-url-map-{stack}',
    default_url_redirect={
        'https_redirect': True,
        'redirect_response_code': 'MOVED_PERMANENTLY_DEFAULT',
        'strip_query': False,
    },
)

http_proxy = gcp.compute.TargetHttpProxy(
    'metamist-http-proxy',
    name=f'metamist-http-proxy-{stack}',
    url_map=http_redirect_url_map.id,
)

https_proxy = gcp.compute.TargetHttpsProxy(
    'metamist-https-proxy',
    name=f'metamist-https-proxy-{stack}',
    url_map=url_map.id,
    ssl_certificates=[ssl_cert.id],
)

global_forwarding_rule = gcp.compute.GlobalForwardingRule(
    'metamist-forwarding-rule',
    name=f'metamist-forwarding-rule-{stack}',
    target=https_proxy.id,
    port_range='443',
    ip_address=ip_address.address,
)


http_global_forwarding_rule = gcp.compute.GlobalForwardingRule(
    'metamist-http-forwarding-rule',
    name=f'metamist-http-forwarding-rule-{stack}',
    target=http_proxy.id,
    port_range='80',
    ip_address=ip_address.address,
)


pulumi.export('load balancer ip', ip_address.address)
