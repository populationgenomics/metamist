# pylint: disable=missing-function-docstring,import-error
"""
Make metamist architecture available to production pulumi stack
so it can be centrally deployed. Do this through a plugin, and submodule.
"""
from functools import cached_property
from pathlib import Path

import pulumi
import pulumi_gcp as gcp
from cpg_utils.cloud import read_secret

from cpg_infra.plugin import CpgInfrastructurePlugin
from cpg_infra.utils import archive_folder

# this gets moved around during the pip install
ETL_FOLDER = Path(__file__).parent / 'etl'
PATH_TO_ETL_BQ_SCHEMA = ETL_FOLDER / 'bq_schema.json'
PATH_TO_ETL_ENDPOINT = ETL_FOLDER / 'endpoint'


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
            f'metamist-source-bucket',
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
            f'metamist-etl-topic',
            project=self.config.sample_metadata.gcp.project,
            opts=pulumi.ResourceOptions(depends_on=[self._svc_pubsub]),
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
            # docs say: Note: On newer versions of the provider, you must explicitly set
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

    def setup_etl(self):
        # give the etl_service_account ability to write to bigquery
        gcp.bigquery.DatasetAccess(
            'metamist-etl-bq-dataset-access',
            project=self.config.sample_metadata.gcp.project,
            dataset_id=self.etl_bigquery_dataset.dataset_id,
            role='OWNER',
            user_by_email=self.etl_service_account.email,
        )

        self.setup_metamist_etl_accessors()
        self.setup_slack_notification()

    @cached_property
    def etl_function(self):
        """
        Driver function to setup the etl infrastructure
        """
        # The Cloud Function source code itself needs to be zipped up into an
        # archive, which we create using the pulumi.AssetArchive primitive.
        archive = archive_folder(str(PATH_TO_ETL_ENDPOINT.absolute()))

        # Create the single Cloud Storage object, which contains the source code
        source_archive_object = gcp.storage.BucketObject(
            'metamist-etl-endpoint-source-code',
            # updating the source archive object does not trigger the cloud function
            # to actually updating the source because it's based on the name,
            # allow Pulumi to create a new name each time it gets updated
            bucket=self.source_bucket.name,
            source=archive,
            opts=pulumi.ResourceOptions(replace_on_changes=['*']),
        )

        fxn = gcp.cloudfunctionsv2.Function(
            'metamist-etl-endpoint-source-code',
            name='metamist-etl',
            build_config=gcp.cloudfunctionsv2.FunctionBuildConfigArgs(
                runtime='python311',
                entry_point='etl_post',
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
                    # format: "project.dataset.table_id
                    'BIGQUERY_TABLE': pulumi.Output.concat(
                        self.etl_bigquery_table.project,
                        '.',
                        self.etl_bigquery_table.dataset_id,
                        '.',
                        self.etl_bigquery_table.table_id,
                    ),
                    'PUBSUB_TOPIC': self.etl_pubsub_topic.id,
                    'ALLOWED_USERS': 'michael.franklin@populationgenomics.org.au',
                },
                ingress_settings='ALLOW_ALL',
                all_traffic_on_latest_revision=True,
                service_account_email=self.etl_service_account.email,
            ),
            project=self.config.sample_metadata.gcp.project,
            location=self.config.gcp.region,
            opts=pulumi.ResourceOptions(
                depends_on=[self._svc_functions, self._svc_build]
            ),
        )

        return fxn

    def setup_metamist_etl_accessors(self):
        for name, sa in self.etl_accessors.items():
            gcp.cloudfunctionsv2.FunctionIamMember(
                f'metamist-etl-accessor-{name}',
                location=self.etl_function.location,
                project=self.etl_function.project,
                cloud_function=self.etl_function.name,
                role='roles/cloudfunctions.invoker',
                member=pulumi.Output.concat('serviceAccount:', sa.email),
            )

            gcp.cloudrun.IamMember(
                f'metamist-etl-run-accessor-{name}',
                location=self.etl_function.location,
                project=self.etl_function.project,
                service=self.etl_function.name,  # it shared the name
                role='roles/run.invoker',
                member=pulumi.Output.concat('serviceAccount:', sa.email),
            )

    def setup_slack_notification(self):
        if self.slack_channel is None:
            return

        # Slack notifications
        filter_string = self.etl_function.name.apply(
            lambda fxn_name: f"""
                        resource.type="cloud_function"
                        AND resource.labels.function_name="{fxn_name}"
                        AND severity >= WARNING
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
            f'metamist-etl-alert-policy',
            display_name=f'Metamist ETL Function Error Alert',
            combiner='OR',
            notification_channels=[self.slack_channel.id],
            conditions=[alert_condition],
            alert_strategy=gcp.monitoring.AlertPolicyAlertStrategyArgs(
                notification_rate_limit=(
                    gcp.monitoring.AlertPolicyAlertStrategyNotificationRateLimitArgs(
                        period='300s'
                    )
                ),
            ),
            opts=pulumi.ResourceOptions(depends_on=[self.etl_function]),
        )
