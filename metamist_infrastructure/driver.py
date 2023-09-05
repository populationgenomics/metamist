# pylint: disable=missing-function-docstring,import-error
"""
Make metamist architecture available to production pulumi stack
so it can be centrally deployed. Do this through a plugin, and submodule.
"""
import os
import contextlib
from functools import cached_property
from pathlib import Path

import pulumi
import pulumi_gcp as gcp
from cpg_infra.plugin import CpgInfrastructurePlugin
# from cpg_infra.utils import archive_folder
from cpg_utils.cloud import read_secret

# this gets moved around during the pip install
# ETL_FOLDER = Path(__file__).parent / 'etl'
ETL_FOLDER = Path(__file__).parent.parent / 'etl'
PATH_TO_ETL_BQ_SCHEMA = ETL_FOLDER / 'bq_schema.json'


# TODO: update implementation in cpg_infra project to enable binary files
def archive_folder(
    path: str, allowed_extensions: frozenset[str]
) -> pulumi.AssetArchive:
    """Archive a folder into a pulumi asset archive"""
    assets = {}

    # python 3.11 thing, but allows you to temporarily change directory
    # into the path we're archiving, so we're not archiving the directory,
    # but just the code files. Otherwise the deploy fails.
    with contextlib.chdir(path):
        for filename in os.listdir('.'):
            if not any(filename.endswith(ext) for ext in allowed_extensions):
                # print(f'Skipping {filename} for invalid extension')
                continue

            if filename.endswith('.gz'):
                # tarfile/zipped files need to be read as binary
                assets[filename] = pulumi.FileAsset(f'{path}/{filename}')
            else:
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
        self.setup_etl()

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

    @cached_property
    def etl_service_account(self):
        """Service account to run endpoint + ingestion as"""
        return gcp.serviceaccount.Account(
            'metamist-etl-service-account',
            account_id='metamist-etl-sa',
            project=self.config.sample_metadata.gcp.project,
            opts=pulumi.ResourceOptions(
                depends_on=[self._svc_iam],
            ),
        )

    def etl_function_account(self, f_name: str):
        """
        Service account for cloud function
        """
        return gcp.serviceaccount.Account(
            f'metamist-etl-{f_name}-service-account',
            account_id=f'metamist-etl-{f_name}-sa',
            project=self.config.sample_metadata.gcp.project,
            opts=pulumi.ResourceOptions(
                depends_on=[self._svc_iam],
            ),
        )

    @cached_property
    def etl_load_service_account(self):
        """Service account to run load/transform functionality"""
        return self.etl_function_account('load')

    @cached_property
    def etl_extract_service_account(self):
        """Service account to run extract functionality"""
        return self.etl_function_account('extract')

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
                    self.etl_pubsub_topic,
                    self.etl_load_function,
                    self.etl_pubsub_dead_letters_topic,
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
            opts=pulumi.ResourceOptions(
                depends_on=[self.etl_pubsub_dead_letters_topic]
            ),
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

    @cached_property
    def etl_bigquery_table(self):
        """
        Bigquery table to contain the etl data
        """
        with open(PATH_TO_ETL_BQ_SCHEMA) as f:
            schema = f.read()

        etl_table = gcp.bigquery.Table(
            'metamist-etl-bigquery-table',
            table_id='etl-data',
            dataset_id=self.etl_bigquery_dataset.dataset_id,
            labels={'project': 'metamist'},
            schema=schema,
            project=self.config.sample_metadata.gcp.project,
            # docs say: Note: On newer versions of the provider,
            # you must explicitly set
            deletion_protection=False,
            opts=pulumi.ResourceOptions(
                depends_on=[self.etl_bigquery_dataset],
            ),
        )

        return etl_table

    @cached_property
    def slack_channel(self):
        """
        Create a Slack notification channel for all functions
        Use cli command below to retrieve the required 'labels'
        $ gcloud beta monitoring channel-descriptors describe slack
        """
        if not self.config.sample_metadata.slack_channel:
            return None
        return gcp.monitoring.NotificationChannel(
            'metamist-etl-slack-notification-channel',
            display_name='Metamist ETL Slack Notification Channel',
            type='slack',
            labels={'channel_name': self.config.sample_metadata.slack_channel},
            sensitive_labels=gcp.monitoring.NotificationChannelSensitiveLabelsArgs(
                auth_token=read_secret(
                    # reuse this secret :)
                    project_id=self.config.billing.gcp.project_id,
                    secret_name=self.config.billing.aggregator.slack_token_secret_name,
                    fail_gracefully=False,
                ),
            ),
            description='Slack notification channel for all cost aggregator functions',
            project=self.config.sample_metadata.gcp.project,
        )

    def prepare_service_account_policy_data(self, role):
        """
        Prepare gcp service account policy
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

    def setup_etl(self):
        """
        setup_etl
        """
        # give the etl_load/extracr_service_accounts ability to read/write to bq table
        gcp.bigquery.DatasetAccess(
            'metamist-etl-bq-dataset-write-access',
            project=self.config.sample_metadata.gcp.project,
            dataset_id=self.etl_bigquery_dataset.dataset_id,
            role='WRITER',
            user_by_email=self.etl_extract_service_account.email,
        )
        gcp.bigquery.DatasetAccess(
            'metamist-etl-bq-dataset-read-access',
            project=self.config.sample_metadata.gcp.project,
            dataset_id=self.etl_bigquery_dataset.dataset_id,
            role='READER',
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
            'metamist-etl-editor-role',
            project=self.config.sample_metadata.gcp.project,
            role='roles/editor',
            member=pulumi.Output.concat(
                'serviceAccount:', self.etl_extract_service_account.email
            ),
        )

        self._setup_etl_functions()
        self._setup_etl_pubsub()

        self._setup_metamist_etl_accessors()
        self._setup_slack_notification()

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
        return self._etl_function(
            'extract', self.etl_extract_service_account.email
        )

    @cached_property
    def etl_load_function(self):
        """etl_load_function"""
        return self._etl_function('load', self.etl_load_service_account.email)

    def _etl_function(self, f_name: str, sa_email: str):
        """
        Driver function to setup the etl cloud function
        """

        path_to_func_folder = ETL_FOLDER / f_name

        # The Cloud Function source code itself needs to be zipped up into an
        # archive, which we create using the pulumi.AssetArchive primitive.
        archive = archive_folder(str(path_to_func_folder.absolute()), allowed_extensions=frozenset({'.gz', '.py', '.txt', '.json'}))

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
            f'metamist-etl-{f_name}-function',
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
                max_instance_count=1,
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
                    'PUBSUB_TOPIC': self.etl_pubsub_topic.id,
                },
                ingress_settings='ALLOW_ALL',
                all_traffic_on_latest_revision=True,
                service_account_email=sa_email,
            ),
            project=self.config.sample_metadata.gcp.project,
            location=self.config.gcp.region,
            opts=pulumi.ResourceOptions(
                depends_on=[self._svc_functions, self._svc_build]
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

    def _setup_function_slack_notification(self, etl_fun_name: str):
        """
        setup slack notification for etl_fun cloud function
        """
        etl_fun = getattr(self, f'etl_{etl_fun_name}_function')

        # Slack notifications
        filter_string = etl_fun.name.apply(
            lambda fxn_name: f"""
                resource.type="cloud_run_revision"
                AND resource.labels.service_name="{fxn_name}"
                AND severity>=WARNING
            """  # noqa: B028
        )

        # Create the Cloud Function's event alert
        alert_condition = gcp.monitoring.AlertPolicyConditionArgs(
            condition_matched_log=(
                gcp.monitoring.AlertPolicyConditionConditionMatchedLogArgs(
                    filter=filter_string,
                )
            ),
            display_name='Function warning/error',
        )
        gcp.monitoring.AlertPolicy(
            resource_name=f'metamist-etl-{etl_fun_name}-alert-policy',
            display_name=f'Metamist ETL {etl_fun_name.capitalize()} Function Error Alert',
            combiner='OR',
            notification_channels=[self.slack_channel.id],
            conditions=[alert_condition],
            alert_strategy=gcp.monitoring.AlertPolicyAlertStrategyArgs(
                notification_rate_limit=(
                    gcp.monitoring.AlertPolicyAlertStrategyNotificationRateLimitArgs(
                        # One notification per 5 minutes
                        period='300s'
                    )
                ),
                # Autoclose Incident after 30 minutes
                auto_close='1800s',
            ),
            opts=pulumi.ResourceOptions(depends_on=[etl_fun]),
        )

    def _setup_slack_notification(self):
        if self.slack_channel is None:
            return

        self._setup_function_slack_notification('extract')
        self._setup_function_slack_notification('load')
