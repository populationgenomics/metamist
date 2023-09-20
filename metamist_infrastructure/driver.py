"""
Make metamist architecture available to production pulumi stack
so it can be centrally deployed. Do this through a plugin, and submodule.
"""
import contextlib
import os
import json
from functools import cached_property
from pathlib import Path
from typing import Any, Callable

import pulumi
import pulumi_gcp as gcp
from cpg_infra.plugin import CpgInfrastructurePlugin

# from cpg_infra.utils import archive_folder
from slack_notification import (
    SlackNotification,
    SlackNotificationType,
    SlackNotificationConfig,
)

# this gets moved around during the pip install
ETL_FOLDER = Path(__file__).parent / 'etl'
# ETL_FOLDER = Path(__file__).parent.parent / 'etl'
PATH_TO_ETL_BQ_SCHEMA = ETL_FOLDER / 'bq_schema.json'
PATH_TO_ETL_BQ_LOG_SCHEMA = ETL_FOLDER / 'bq_log_schema.json'


# TODO: update implementation in cpg_infra to enable binary files & private repos?
def append_private_repositories_to_requirements(
    file_content: str, private_repo_url: str, private_repos: list[str]
) -> str:
    """
    Append private repositories to requirements.txt
    """
    return (
        file_content  # original content
        + f'\n--extra-index-url {private_repo_url}\n'  # private repo url
        + '\n'.join(private_repos)  # private repositories
    )


def add_binary_file(**kwargs):
    """Prepare FIle Asset for binary files"""
    path = kwargs.get('path')
    filename = kwargs.get('filename')
    return pulumi.FileAsset(f'{path}/{filename}')


def update_requirements(**kwargs):
    """Prepare FIle Asset for binary files"""

    filename = kwargs.get('filename')
    private_repo_url = kwargs.get('private_repo_url')
    private_repos = kwargs.get('private_repos')

    with open(filename, encoding='utf-8') as file:
        # do it this way to stop any issues with changing paths
        file_content = file.read()
        if private_repo_url and private_repos:
            file_content = append_private_repositories_to_requirements(
                file_content, private_repo_url, private_repos
            )
        return pulumi.StringAsset(file_content)


# TODO -> move this to cpg_infra.utils, replace existing function there
def archive_folder(
    path: str,
    allowed_extensions: frozenset[str],
    apply_fun_map: dict[str, tuple[Callable, Any]] | None = None,
) -> pulumi.AssetArchive:
    """Archive a folder into a pulumi asset archive

    apply_fun_map contains map of file name to callable function and its extra parameter

    e.g. apply_fun_map = {
        'metamist-6.2.0.tar.gz': (add_binary_file, None),
        'requirements.tx't: (update_requirements, {
            'private_repo_url': 'https://...',
            'private_repos': ['repo1','repo2'],
        }),
    }

    """
    assets = {}

    # python 3.11 thing, but allows you to temporarily change directory
    # into the path we're archiving, so we're not archiving the directory,
    # but just the code files. Otherwise the deploy fails.
    with contextlib.chdir(path):
        for filename in os.listdir('.'):
            if not any(filename.endswith(ext) for ext in allowed_extensions):
                # print(f'Skipping {filename} for invalid extension')
                continue

            # apply function to extract / update file content
            if apply_fun_map and filename in apply_fun_map:
                (fun_name, extra_fun_params) = apply_fun_map[filename]
                fun_params = {'filename': filename, 'path': path}
                if extra_fun_params:
                    # if extra params provided, then update fun_params
                    fun_params.update(extra_fun_params)

                # call the function to prepare the file asset
                file_as_asset = fun_name(**fun_params)
                assets[filename] = file_as_asset
                continue

            with open(filename, encoding='utf-8') as file:
                # do it this way to stop any issues with changing paths
                assets[filename] = pulumi.StringAsset(file.read())
        return pulumi.AssetArchive(assets)


class MetamistInfrastructure(CpgInfrastructurePlugin):
    """
    Metamist Infrastructure (as code) for Pulumi
    """

    def main(self):
        """Driver for the metamist infrastructure as code plugin"""
        # todo, eventually configure metamist cloud run server
        # to be deployed here, but for now it's manually deployed

        # TODO: the following should be added to SampleMetadataConfig in cpg_infra
        self.extra_sample_metadata_config = {  # pylint: disable=attribute-defined-outside-init
            'private_repo_url': 'https://australia-southeast1-python.pkg.dev/milo-dev-396001/python-repo/simple',
            'private_repos': ['metamist_private'],
            'environment': 'DEVELOPMENT',
            'etl_load_default_config': {
                # Order of config overides:
                # 1. parser default config values
                # 2. etl_load_default_config
                # 3. config from payload
                'project': 'milo-dev',
                'default_sequencing_type': 'genome',
                'default_sequencing_technology': 'long-read',
                'default_sample_type': 'blood',
            },
        }
        self._setup_etl()

    @cached_property
    def _svc_cloudresourcemanager(self):
        return gcp.projects.Service(
            'metamist-cloudresourcemanager-service',
            service='cloudresourcemanager.googleapis.com',
            disable_on_destroy=False,
            project=self.config.sample_metadata.gcp.project,
        )

    @cached_property
    def _svc_iam(self):
        return gcp.projects.Service(
            'metamist-iam-service',
            service='iam.googleapis.com',
            disable_on_destroy=False,
            project=self.config.sample_metadata.gcp.project,
            opts=pulumi.resource.ResourceOptions(
                depends_on=[self._svc_cloudresourcemanager]
            ),
        )

    @cached_property
    def _svc_functions(self):
        return gcp.projects.Service(
            'metamist-cloudfunctions-service',
            service='cloudfunctions.googleapis.com',
            project=self.config.sample_metadata.gcp.project,
            disable_on_destroy=False,
        )

    @cached_property
    def _svc_pubsub(self):
        return gcp.projects.Service(
            'metamist-pubsub-service',
            service='pubsub.googleapis.com',
            project=self.config.sample_metadata.gcp.project,
            disable_on_destroy=False,
        )

    @cached_property
    def _svc_scheduler(self):
        return gcp.projects.Service(
            'metamist-cloudscheduler-service',
            service='cloudscheduler.googleapis.com',
            project=self.config.sample_metadata.gcp.project,
            disable_on_destroy=False,
        )

    @cached_property
    def _svc_build(self):
        return gcp.projects.Service(
            'metamist-cloudbuild-service',
            service='cloudbuild.googleapis.com',
            project=self.config.sample_metadata.gcp.project,
            disable_on_destroy=False,
        )

    @cached_property
    def _svc_bigquery(self):
        return gcp.projects.Service(
            'metamist-bigquery-service',
            service='bigquery.googleapis.com',
            project=self.config.sample_metadata.gcp.project,
            disable_on_destroy=False,
        )

    @cached_property
    def source_bucket(self):
        """
        We will store the source code to the Cloud Function
        in a Google Cloud Storage bucket.
        """
        return gcp.storage.Bucket(
            'metamist-source-bucket',
            name=f'{self.config.gcp.dataset_storage_prefix}metamist-source-bucket',
            location=self.config.gcp.region,
            project=self.config.sample_metadata.gcp.project,
            uniform_bucket_level_access=True,
        )

    def _etl_function_account(self, f_name: str):
        """
        Service account for cloud function
        """
        return gcp.serviceaccount.Account(
            f'metamist-etl-{f_name}service-account',
            account_id=f'metamist-etl-{f_name}sa',
            project=self.config.sample_metadata.gcp.project,
            opts=pulumi.ResourceOptions(
                depends_on=[self._svc_iam],
            ),
        )

    @cached_property
    def etl_service_account(self):
        """Service account to run notification functionality and other services"""
        return self._etl_function_account('')

    @cached_property
    def etl_load_service_account(self):
        """Service account to run load/transform functionality"""
        return self._etl_function_account('load-')

    @cached_property
    def etl_extract_service_account(self):
        """Service account to run extract functionality"""
        return self._etl_function_account('extract-')

    @cached_property
    def etl_accessors(self):
        """Service account to run endpoint + ingestion as"""
        return {
            name: gcp.serviceaccount.Account(
                f'metamist-etl-accessor-{name}',
                account_id=f'metamist-etl-{name}',
                project=self.config.sample_metadata.gcp.project,
                opts=pulumi.ResourceOptions(
                    depends_on=[self._svc_iam],
                ),
            )
            for name in self.config.sample_metadata.etl_accessors
        }

    @cached_property
    def etl_pubsub_topic(self):
        """
        Pubsub topic to trigger the etl function
        """
        return gcp.pubsub.Topic(
            'metamist-etl-topic',
            project=self.config.sample_metadata.gcp.project,
            opts=pulumi.ResourceOptions(depends_on=[self._svc_pubsub]),
        )

    @cached_property
    def etl_pubsub_dead_letters_topic(self):
        """
        Pubsub dead_letters topic to capture failed jobs
        """
        topic = gcp.pubsub.Topic(
            'metamist-etl-dead-letters-topic',
            project=self.config.sample_metadata.gcp.project,
            opts=pulumi.ResourceOptions(depends_on=[self._svc_pubsub]),
        )

        # give publisher permission to service account
        gcp.pubsub.TopicIAMPolicy(
            'metamist-etl-dead-letters-topic-iam-policy',
            project=self.config.sample_metadata.gcp.project,
            topic=topic.name,
            policy_data=self.prepare_service_account_policy_data(
                'roles/pubsub.publisher'
            ),
        )

        return topic

    @cached_property
    def etl_pubsub_push_subscription(self):
        """
        Pubsub push_subscription to topic,
        new messages to topic triggeres load process
        """
        subscription = gcp.pubsub.Subscription(
            'metamist-etl-subscription',
            topic=self.etl_pubsub_topic.name,
            ack_deadline_seconds=20,
            dead_letter_policy=gcp.pubsub.SubscriptionDeadLetterPolicyArgs(
                dead_letter_topic=self.etl_pubsub_dead_letters_topic.id,
                max_delivery_attempts=5,
            ),
            push_config=gcp.pubsub.SubscriptionPushConfigArgs(
                push_endpoint=self.etl_load_function.service_config.uri,
                oidc_token=gcp.pubsub.SubscriptionPushConfigOidcTokenArgs(
                    service_account_email=self.etl_extract_service_account.email,
                ),
                attributes={
                    'x-goog-version': 'v1',
                },
            ),
            project=self.config.sample_metadata.gcp.project,
            opts=pulumi.ResourceOptions(
                depends_on=[
                    self._svc_pubsub,
                    self.etl_pubsub_dead_letter_subscription,
                ]
            ),
        )

        # give subscriber permission to service account
        gcp.pubsub.SubscriptionIAMPolicy(
            'metamist-etl-pubsub-topic-subscription-policy',
            project=self.config.sample_metadata.gcp.project,
            subscription=subscription.name,
            policy_data=self.prepare_service_account_policy_data(
                'roles/pubsub.subscriber'
            ),
        )

        return subscription

    @cached_property
    def etl_pubsub_dead_letter_subscription(self):
        """
        Dead letter subscription
        """
        return gcp.pubsub.Subscription(
            'metamist-etl-dead-letter-subscription',
            topic=self.etl_pubsub_dead_letters_topic.name,
            project=self.config.sample_metadata.gcp.project,
            ack_deadline_seconds=20,
        )

    @cached_property
    def etl_bigquery_dataset(self):
        """
        Bigquery dataset to contain the bigquery table
        """
        return gcp.bigquery.Dataset(
            'metamist-etl-bigquery-dataset',
            dataset_id='metamist',
            friendly_name='metamist bigquery dataset',
            description='Metamist related bigquery tables',
            location=self.config.gcp.region,
            # default_table_expiration_ms=3600000,
            labels={
                'project': 'metamist',
            },
            project=self.config.sample_metadata.gcp.project,
            opts=pulumi.ResourceOptions(
                depends_on=[self._svc_bigquery],
            ),
        )

    def _setup_bq_table(self, schema_file_name: Path, table_name: str):
        """Setup Bigquery table"""
        with open(schema_file_name) as f:
            schema = f.read()

        etl_table = gcp.bigquery.Table(
            f'metamist-etl-bigquery-table-{table_name}',
            table_id=f'etl-{table_name}',
            dataset_id=self.etl_bigquery_dataset.dataset_id,
            labels={'project': 'metamist'},
            schema=schema,
            project=self.config.sample_metadata.gcp.project,
            # docs say: Note: On newer versions of the provider,
            # you must explicitly set
            deletion_protection=False,
        )
        return etl_table

    @cached_property
    def etl_bigquery_table(self):
        """
        Bigquery table to contain the etl data
        """
        return self._setup_bq_table(PATH_TO_ETL_BQ_SCHEMA, 'data')

    @cached_property
    def etl_bigquery_log_table(self):
        """
        Bigquery table to contain the etl logs
        """
        return self._setup_bq_table(PATH_TO_ETL_BQ_LOG_SCHEMA, 'logs')

    def prepare_service_account_policy_data(self, role):
        """
        Prepare gcp service account policy, to be used in the pubsub subscription

        serviceAccount:service-<project_number>@gcp-sa-pubsub.iam.gserviceaccount.com
        is the service account that is used to publish messages to the topic

        We need to give this account the ability to publish and read the topic
        """
        # get project
        project = gcp.organizations.get_project()

        return gcp.organizations.get_iam_policy(
            bindings=[
                gcp.organizations.GetIAMPolicyBindingArgs(
                    role=role,
                    members=[
                        pulumi.Output.concat(
                            'serviceAccount:service-',
                            project.number,
                            '@gcp-sa-pubsub.iam.gserviceaccount.com',
                        )
                    ],
                )
            ]
        ).policy_data

    def _setup_etl(self):
        """
        setup_etl
        """
        # give the etl_load/extract service_accounts ability to read/write to bq table
        gcp.bigquery.DatasetAccess(
            'metamist-etl-bq-dataset-extract-service-access',
            project=self.config.sample_metadata.gcp.project,
            dataset_id=self.etl_bigquery_dataset.dataset_id,
            role='WRITER',
            user_by_email=self.etl_extract_service_account.email,
        )
        gcp.bigquery.DatasetAccess(
            'metamist-etl-bq-dataset-load-service-access',
            project=self.config.sample_metadata.gcp.project,
            dataset_id=self.etl_bigquery_dataset.dataset_id,
            role='WRITER',
            user_by_email=self.etl_load_service_account.email,
        )
        # give the etl_load_service_account ability to execute bigquery jobs
        gcp.projects.IAMMember(
            'metamist-etl-bq-job-user-role',
            project=self.config.sample_metadata.gcp.project,
            role='roles/bigquery.jobUser',
            member=pulumi.Output.concat(
                'serviceAccount:', self.etl_load_service_account.email
            ),
        )
        # give the etl_extract_service_account ability to push to pub/sub
        gcp.projects.IAMMember(
            'metamist-etl-extract-editor-role',
            project=self.config.sample_metadata.gcp.project,
            role='roles/editor',
            member=pulumi.Output.concat(
                'serviceAccount:', self.etl_extract_service_account.email
            ),
        )
        # give the etl_load_service_account ability to push to pub/sub
        gcp.projects.IAMMember(
            'metamist-etl-load-editor-role',
            project=self.config.sample_metadata.gcp.project,
            role='roles/editor',
            member=pulumi.Output.concat(
                'serviceAccount:', self.etl_load_service_account.email
            ),
        )

        # serverless-robot-prod.iam.gserviceaccount.com is used
        # by gcloud to setup cloud run service
        # if not present functions could not be deployed
        project = gcp.organizations.get_project()
        robot_account = pulumi.Output.concat(
            'serviceAccount:service-',
            project.number,
            '@serverless-robot-prod.iam.gserviceaccount.com',
        )
        gcp.projects.IAMMember(
            'metamist-etl-robot-service-agent-role',
            project=self.config.sample_metadata.gcp.project,
            role='roles/run.serviceAgent',
            member=robot_account,
        )

        self._setup_etl_functions()
        self._setup_etl_pubsub()

        self._setup_metamist_etl_accessors()

    def _setup_etl_functions(self):
        """
        setup_etl_functions
        """
        # TODO is this the best way to do this?
        return pulumi.ResourceOptions(
            depends_on=[self.etl_extract_function, self.etl_load_function],
        )

    def _setup_etl_pubsub(self):
        """
        setup_etl_pubsub
        """
        return pulumi.ResourceOptions(
            depends_on=[
                self.etl_pubsub_dead_letter_subscription,
                self.etl_pubsub_push_subscription,
            ],
        )

    @cached_property
    def etl_extract_function(self):
        """etl_extract_function"""
        return self._etl_function('extract', self.etl_extract_service_account)

    @cached_property
    def etl_load_function(self):
        """etl_load_function"""
        return self._etl_function('load', self.etl_load_service_account)

    def _etl_function(self, f_name: str, sa: gcp.serviceaccount.Account):
        """
        Driver function to setup the etl cloud function
        """

        path_to_func_folder = ETL_FOLDER / f_name

        # The Cloud Function source code itself needs to be zipped up into an
        # archive, which we create using the pulumi.AssetArchive primitive.
        archive = archive_folder(
            str(path_to_func_folder.absolute()),
            allowed_extensions=frozenset({'.gz', '.py', '.txt', '.json'}),
            apply_fun_map={
                'metamist-6.2.0.tar.gz': (add_binary_file, None),
                'requirements.txt': (
                    update_requirements,
                    {
                        'private_repo_url': str(
                            self.extra_sample_metadata_config['private_repo_url']
                        ),
                        'private_repos': self.extra_sample_metadata_config[
                            'private_repos'
                        ],
                    },
                ),
            }
            # TODO replace with metamist config, once it's available
            # private_repo_url=str(self.extra_sample_metadata_config['private_repo_url']),
            # private_repos=str(self.extra_sample_metadata_config['private_repos']),
        )

        # Create the single Cloud Storage object,
        # which contains the source code
        source_archive_object = gcp.storage.BucketObject(
            f'metamist-etl-{f_name}-source-code',
            # updating the source archive object does not trigger the cloud
            # function to actually updating the source because
            # it's based on the name,
            # allow Pulumi to create a new name each time it gets updated
            bucket=self.source_bucket.name,
            source=archive,
            opts=pulumi.ResourceOptions(replace_on_changes=['*']),
        )

        fxn = gcp.cloudfunctionsv2.Function(
            f'metamist-etl-{f_name}',
            name=f'metamist-etl-{f_name}',
            build_config=gcp.cloudfunctionsv2.FunctionBuildConfigArgs(
                runtime='python311',
                entry_point=f'etl_{f_name}',
                environment_variables={},
                source=gcp.cloudfunctionsv2.FunctionBuildConfigSourceArgs(
                    storage_source=gcp.cloudfunctionsv2.FunctionBuildConfigSourceStorageSourceArgs(
                        bucket=self.source_bucket.name,
                        object=source_archive_object.name,
                    ),
                ),
            ),
            service_config=gcp.cloudfunctionsv2.FunctionServiceConfigArgs(
                max_instance_count=1,  # Keep max instances to 1 to avoid racing conditions
                min_instance_count=0,
                available_memory='2Gi',
                available_cpu=1,
                timeout_seconds=540,
                environment_variables={
                    # format: 'project.dataset.table_id
                    'BIGQUERY_TABLE': pulumi.Output.concat(
                        self.etl_bigquery_table.project,
                        '.',
                        self.etl_bigquery_table.dataset_id,
                        '.',
                        self.etl_bigquery_table.table_id,
                    ),
                    'BIGQUERY_LOG_TABLE': pulumi.Output.concat(
                        self.etl_bigquery_log_table.project,
                        '.',
                        self.etl_bigquery_log_table.dataset_id,
                        '.',
                        self.etl_bigquery_log_table.table_id,
                    ),
                    'PUBSUB_TOPIC': self.etl_pubsub_topic.id,
                    'NOTIFICATION_PUBSUB_TOPIC': self.etl_slack_notification_topic.id
                    if self.etl_slack_notification_topic
                    else '',
                    # TODO replace with metamist config, once it's available
                    'SM_ENVIRONMENT': self.extra_sample_metadata_config['environment'],
                    # TODO replace with etl_load_default_config config, once it's available
                    'DEFAULT_LOAD_CONFIG': json.dumps(
                        self.extra_sample_metadata_config['etl_load_default_config']
                    ),
                },
                ingress_settings='ALLOW_ALL',
                all_traffic_on_latest_revision=True,
                service_account_email=sa.email,
            ),
            project=self.config.sample_metadata.gcp.project,
            location=self.config.gcp.region,
            opts=pulumi.ResourceOptions(
                depends_on=[
                    self._svc_functions,
                    self._svc_build,
                    sa,
                ]
            ),
        )

        return fxn

    def _setup_metamist_etl_accessors(self):
        for name, sa in self.etl_accessors.items():
            gcp.cloudfunctionsv2.FunctionIamMember(
                f'metamist-etl-accessor-{name}',
                location=self.etl_extract_function.location,
                project=self.etl_extract_function.project,
                cloud_function=self.etl_extract_function.name,
                role='roles/cloudfunctions.invoker',
                member=pulumi.Output.concat('serviceAccount:', sa.email),
            )

            gcp.cloudrun.IamMember(
                f'metamist-etl-run-accessor-{name}',
                location=self.etl_extract_function.location,
                project=self.etl_extract_function.project,
                service=self.etl_extract_function.name,  # it shared the name
                role='roles/run.invoker',
                member=pulumi.Output.concat('serviceAccount:', sa.email),
            )

    @cached_property
    def etl_slack_notification(self):
        """
        Setup Slack notification
        """

        slack_config = SlackNotificationConfig(
            project_name=self.config.sample_metadata.gcp.project,
            location=self.config.gcp.region,
            service_account=self.etl_service_account,  # can be some other account
            source_bucket=self.source_bucket,
            slack_secret_project_id=self.config.billing.gcp.project_id,
            slack_token_secret_name=self.config.billing.aggregator.slack_token_secret_name,
            slack_channel_name=self.config.sample_metadata.slack_channel,
        )

        notification = SlackNotification(
            slack_config=slack_config,
            topic_name='metamist-etl-notification',
            func_to_monitor=[
                'metamist-etl-notification-func',
                'metamist-etl-extract',
                'metamist-etl-load',
            ],
            notification_type=SlackNotificationType.NOTIFICATION,
            depends_on=[
                self._svc_iam,
                self._svc_functions,
                self._svc_build,
                self._svc_pubsub,
                self.etl_service_account,
            ],
        )

        # setup notification
        return notification.main()

    @cached_property
    def etl_slack_notification_channel(self):
        """etl_slack_notification_channel"""
        (alerts_channel, _) = self.etl_slack_notification
        return alerts_channel

    @cached_property
    def etl_slack_notification_topic(self):
        """etl_slack_notification_topic"""
        (_, pubsub_topic) = self.etl_slack_notification
        return pubsub_topic
