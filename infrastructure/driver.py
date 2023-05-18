"""
Make metamist architecture available to production pulumi stack
so it can be centrally deployed. Do this through a plugin, and submodule.
"""
from functools import cached_property
from pathlib import Path

from cpg_infra.plugin import CpgInfrastructurePlugin
from cpg_infra.utils import archive_folder

import pulumi
import pulumi_gcp as gcp

ETL_FOLDER = Path(__file__).parent.parent / 'etl'
PATH_TO_ETL_BQ_SCHEMA = ETL_FOLDER / 'bq_schema.json'
PATH_TO_ETL_ENDPOINT = ETL_FOLDER / 'endpoint'


class MetamistInfrastructure(CpgInfrastructurePlugin):
    def main(self):
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
    def source_bucket(self):
        """
        We will store the source code to the Cloud Function
        in a Google Cloud Storage bucket.
        """
        return gcp.storage.Bucket(
            f'metamist-source-bucket',
            name=f'{self.config.dataset_storage_prefix}metamist-source-bucket',
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
            table_id='etl-incoming',
            dataset_id=self.etl_bigquery_dataset.dataset_id,
            labels={'project': 'metamist'},
            schema=schema,
        )

        return etl_table

    def setup_etl(self):
        """
        Driver function to setup the etl infrastructure
        """
        # The Cloud Function source code itself needs to be zipped up into an
        # archive, which we create using the pulumi.AssetArchive primitive.
        archive = archive_folder(str(PATH_TO_ETL_ENDPOINT.absolute()))

        # give the etl_service_account ability to write to bigquery
        gcp.bigquery.IamMember(
            "metamist-etl-function-bq-table-access",
            project=self.config.sample_metadata.gcp.project,
            dataset_id=self.etl_bigquery_dataset.id,
            table_id=self.etl_bigquery_table.table_id,
            role="roles/bigquery.dataOwner",
            member=pulumi.Output.concat(
                'serviceAccount:', self.etl_service_account.email
            ),
        )

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
                available_memory='2GiB',
                available_cpu=1,
                timeout_seconds=540,
                environment_variables={
                    'BIGQUERY_TABLE': self.etl_bigquery_table.table_id,
                    'PUBSUB_TOPIC': '',
                    'ALLOWED_USERS': 'michael.franklin@populationgenomics.org.au',
                },
                ingress_settings='ALLOW_INTERNAL_ONLY',
                all_traffic_on_latest_revision=True,
                service_account_email=self.etl_service_account.email,
            ),
            project=self.config.sample_metadata.gcp.project,
            location=self.config.gcp.region,
            opts=pulumi.ResourceOptions(
                depends_on=[self._svc_functions, self._svc_build]
            ),
        )

        for name, sa in self.etl_accessors.items():
            gcp.cloudfunctionsv2.FunctionIamMember(
                f'metamist-etl-accessor-{name}',
                location=fxn.location,
                project=fxn.project,
                cloud_function=fxn.name,
                role='roles/run.invoker',
                member=pulumi.Output.concat('serviceAccount:', sa.email),
            )

        return fxn
