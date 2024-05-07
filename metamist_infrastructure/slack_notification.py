# pylint: disable=missing-function-docstring,import-error,no-member
"""
Make metamist architecture available to production pulumi stack
so it can be centrally deployed. Do this through a plugin, and submodule.
"""
from enum import Enum
from functools import cached_property
from pathlib import Path

import pulumi
import pulumi_gcp as gcp

from cpg_infra.utils import archive_folder
from cpg_utils.cloud import read_secret

# this gets moved around during the pip install
ETL_FOLDER = Path(__file__).parent / 'etl'
PATH_TO_ETL_NOTIFICATION = ETL_FOLDER / 'notification'


def prepare_policy_data(role, members):
    """
    Prepare policy policy for pub/sub subscription and topic
    """
    return gcp.organizations.get_iam_policy(
        bindings=[gcp.organizations.GetIAMPolicyBindingArgs(role=role, members=members)]
    ).policy_data


class SlackNotificationType(Enum):
    """
    Enum type to distinguish between alert and notification:

    INCIDENT_ALERT - is a notification that requires action,
        very generic GC Monitorin alert, which batch multiple incidents/errors into one slack message
        It monitors logs for errors/warnings and sends notification to slack channel
        "resources_to_monitor" is a list of resource types to monitor for errors/warnings

    NOTIFICATION - is a notification that sends details about the event to the slack channel
        more granualar and with more details, it requires error messages to be pushed to pubsub topic.
        It monitors pubsub topic for new messages and sends notification to slack channel
        "topic_name" is the name of the pubsub topic to monitor for new messages

    BOTH - setup both alert and notification
    """

    INCIDENT_ALERT = 1
    NOTIFICATION = 2
    BOTH = 3


class SlackNotificationConfig:
    """Slack token, channel and project id wrapped in the config class"""

    def __init__(
        self,
        project_name: str,
        location: str,  # e.g. self.config.gcp.region
        service_account: gcp.serviceaccount.Account,
        source_bucket: gcp.storage.Bucket,
        slack_secret_project_id: str,
        slack_token_secret_name: str,
        slack_channel_name: str,
    ):
        """Slack notification config constructor"""
        self.project_name = project_name
        self.location = location
        self.service_account = service_account
        self.source_bucket = source_bucket
        self.slack_secret_project_id = slack_secret_project_id
        self.slack_token_secret_name = slack_token_secret_name
        self.slack_channel_name = slack_channel_name


class SlackNotification:
    """
    Metamist Infrastructure Notification Slack Plugin
    """

    def __init__(
        self,
        slack_config: SlackNotificationConfig,
        topic_name: str,  # e.g. 'metamist-etl-notification'
        func_to_monitor: list[str],
        notification_type: SlackNotificationType,
        depends_on: list | None,
    ):
        """Slack notification constructor"""
        self.config = slack_config
        self.topic_name = topic_name
        self.func_to_monitor = func_to_monitor
        self.notification_type = notification_type
        self.depends_on = depends_on

    def _setup_notification_permissions(self):
        """Setup permissions for the service account"""
        gcp.projects.IAMMember(
            f'{self.topic_name}-sa-run-invoker-role',
            project=self.config.project_name,
            role='roles/run.invoker',
            member=pulumi.Output.concat(
                'serviceAccount:', self.config.service_account.email
            ),
        )

    def _incident_setup_alerts_slack_notification(self):
        """
        setup slack notification for etl_fun in  self.logs_keywords_to_monitor
        """
        for func_name in self.func_to_monitor:
            # Slack notifications

            filter_string = f"""
            resource.type="cloud_run_revision"
            AND resource.labels.service_name="{func_name}"
            AND severity>=WARNING
            """

            # Create the Cloud Function's event alert
            alert_condition = gcp.monitoring.AlertPolicyConditionArgs(
                condition_matched_log=(
                    gcp.monitoring.AlertPolicyConditionConditionMatchedLogArgs(
                        filter=filter_string,
                    )
                ),
                display_name=f'Warning/error for {self.topic_name} {func_name}',
            )
            gcp.monitoring.AlertPolicy(
                resource_name=f'{self.topic_name}-{func_name}-alert-policy',
                display_name=f'{self.topic_name.capitalize()} {func_name} Error Alert',
                combiner='OR',
                notification_channels=[self.incident_alerts_channel.id],
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
            )

    @cached_property
    def notification_cloudfun(self):
        """
        Driver function to setup the etl cloud function
        """

        # The Cloud Function source code itself needs to be zipped up into an
        # archive, which we create using the pulumi.AssetArchive primitive.
        archive = archive_folder(
            str(PATH_TO_ETL_NOTIFICATION.absolute()),
            allowed_extensions=frozenset({'.gz', '.py', '.txt', '.json'}),
        )

        # Create the single Cloud Storage object,
        # which contains the source code
        source_archive_object = gcp.storage.BucketObject(
            f'{self.topic_name}-func-source-code',
            # updating the source archive object does not trigger the cloud
            # function to actually updating the source because
            # it's based on the name,
            # allow Pulumi to create a new name each time it gets updated
            bucket=self.config.source_bucket.name,
            source=archive,
            opts=pulumi.ResourceOptions(replace_on_changes=['*']),
        )

        fxn = gcp.cloudfunctionsv2.Function(
            f'{self.topic_name}-func',
            name=f'{self.topic_name}-func',
            build_config=gcp.cloudfunctionsv2.FunctionBuildConfigArgs(
                runtime='python311',
                entry_point='etl_notify',
                environment_variables={},
                # this one is set on an output, so specifying it keeps the function
                # from being updated, or appearing to update
                docker_repository=f'projects/{self.config.project_name}/locations/australia-southeast1/repositories/gcf-artifacts',
                source=gcp.cloudfunctionsv2.FunctionBuildConfigSourceArgs(
                    storage_source=gcp.cloudfunctionsv2.FunctionBuildConfigSourceStorageSourceArgs(
                        bucket=self.config.source_bucket.name,
                        object=source_archive_object.name,
                    ),
                ),
            ),
            service_config=gcp.cloudfunctionsv2.FunctionServiceConfigArgs(
                max_instance_count=1,  # Keep max instances to 1 to avoid racing conditions
                min_instance_count=0,
                available_memory='2Gi',
                available_cpu='1',
                timeout_seconds=540,
                environment_variables={
                    'SLACK_BOT_TOKEN': read_secret(
                        # reuse this secret :)
                        project_id=self.config.slack_secret_project_id,
                        secret_name=self.config.slack_token_secret_name,
                        fail_gracefully=False,
                    ),
                    'SLACK_CHANNEL': self.config.slack_channel_name,
                },  # type: ignore
                ingress_settings='ALLOW_ALL',
                all_traffic_on_latest_revision=True,
                service_account_email=self.config.service_account.email,
            ),
            project=self.config.project_name,
            location=self.config.location,
            opts=pulumi.ResourceOptions(depends_on=self.depends_on),
        )
        return fxn

    @cached_property
    def incident_alerts_channel(self):
        """
        Create a Slack notification channel for all functions
        Use cli command below to retrieve the required 'labels'
        $ gcloud beta monitoring channel-descriptors describe slack
        """
        return gcp.monitoring.NotificationChannel(
            f'{self.topic_name}-incidents-channel',
            display_name=f'{self.topic_name} incidents slack notification channel',
            type='slack',
            labels={'channel_name': self.config.slack_channel_name},
            sensitive_labels=gcp.monitoring.NotificationChannelSensitiveLabelsArgs(
                auth_token=read_secret(
                    # reuse this secret :)
                    project_id=self.config.slack_secret_project_id,
                    secret_name=self.config.slack_token_secret_name,
                    fail_gracefully=False,
                ),
            ),
            description=f'Slack notification channel for {self.topic_name}',
            project=self.config.project_name,
        )

    @cached_property
    def notification_pubsub_topic(self):
        """
        Pubsub topic to trigger send notification message to slack
        """
        return gcp.pubsub.Topic(self.topic_name, project=self.config.project_name)

    @cached_property
    def notification_pubsub_push_subscription(self):
        """
        Pubsub push_subscription to topic,
        new messages to topic triggeres slack notification function
        """
        subscription = gcp.pubsub.Subscription(
            f'{self.topic_name}-subscription',
            topic=self.notification_pubsub_topic.name,
            ack_deadline_seconds=20,
            dead_letter_policy=gcp.pubsub.SubscriptionDeadLetterPolicyArgs(
                dead_letter_topic=self.notification_dead_letters_pubsub_topic.id,
                max_delivery_attempts=5,
            ),
            push_config=gcp.pubsub.SubscriptionPushConfigArgs(
                push_endpoint=self.notification_cloudfun.service_config.uri,
                oidc_token=gcp.pubsub.SubscriptionPushConfigOidcTokenArgs(
                    service_account_email=self.config.service_account.email,
                ),
                attributes={
                    'x-goog-version': 'v1',
                },
            ),
            project=self.config.project_name,
            # opts=pulumi.ResourceOptions(
            #     depends_on=[
            #         self.notification_pubsub_topic,
            #         self.notification_cloudfun,
            #         self.notification_dead_letters_pubsub_topic,
            #     ]
            # ),
        )

        # give subscriber permission to service account
        project = gcp.organizations.get_project()
        members = [
            pulumi.Output.concat(
                'serviceAccount:service-',
                project.number,
                '@gcp-sa-pubsub.iam.gserviceaccount.com',
            )
        ]

        # give publisher permission to service account
        gcp.pubsub.SubscriptionIAMPolicy(
            f'{self.topic_name}-subscription-iam-policy',
            project=self.config.project_name,
            subscription=subscription.name,
            policy_data=prepare_policy_data('roles/pubsub.subscriber', members),
        )

        return subscription

    @cached_property
    def notification_dead_letters_pubsub_topic(self):
        """
        Dead letters pubsub topic to capture failed jobs
        """
        topic = gcp.pubsub.Topic(
            f'{self.topic_name}-dead-letters-topic', project=self.config.project_name
        )

        project = gcp.organizations.get_project()
        members = [
            pulumi.Output.concat(
                'serviceAccount:service-',
                project.number,
                '@gcp-sa-pubsub.iam.gserviceaccount.com',
            )
        ]

        # give publisher permission to service account
        gcp.pubsub.TopicIAMPolicy(
            f'{self.topic_name}-dead-letters-topic-iam-policy',
            project=self.config.project_name,
            topic=topic.name,
            policy_data=prepare_policy_data('roles/pubsub.publisher', members),
        )

        return topic

    @cached_property
    def notification_dead_letters_pubsub_subscription(self):
        """
        Dead letter subscription
        """
        return gcp.pubsub.Subscription(
            f'{self.topic_name}-dead-letters-subscription',
            topic=self.notification_dead_letters_pubsub_topic.name,
            project=self.config.project_name,
            ack_deadline_seconds=20,
            # opts=pulumi.ResourceOptions(
            #     depends_on=[self.notification_dead_letters_pubsub_topic]
            # ),
        )

    def setup_notification(self):
        """Setup notification, send notification to slack channel
        This notification has more details customising the message than the generic gcp alerts
        """
        self._setup_notification_permissions()

        # now hook service into TOPIC as subscription, depends on self.notification_cloudfun
        return self.notification_pubsub_push_subscription

    def setup_incident_alerts_channel(self):
        """Setup monitoring alerts, monitor logs for errors, batch them and report to slack channel
        Action has to be taken if there are errors in the logs
        """
        self._incident_setup_alerts_slack_notification()
        return self.incident_alerts_channel.name

    def main(self):
        """Main function to setup notification infrastructure"""
        alerts_channel = None
        pubsub_topic = None
        if self.notification_type in (
            SlackNotificationType.INCIDENT_ALERT,
            self.notification_type == SlackNotificationType.BOTH,
        ):
            alerts_channel = self.setup_incident_alerts_channel()

        if self.notification_type in (
            SlackNotificationType.NOTIFICATION,
            self.notification_type == SlackNotificationType.BOTH,
        ):
            self.setup_notification()
            pubsub_topic = self.notification_pubsub_topic

        return (alerts_channel, pubsub_topic)
