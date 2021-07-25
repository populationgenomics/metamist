""" This function prepares gVCFs uploaded to GCS, based off sample status
    logged in a database, for further QC and downstream analysis.
    The upload processor will determine when samples should be added
    to existing MatrixTables where appropriate and which MatrixTables
    they should be combined with in this case. Following a successful
    run, all uploaded files will be moved to archival storage.

    Assumptions
    ===========
    - Specified files must exist in either the source or the destination
      bucket
    - Source and destination bucket must exist
    - User must be authenticated with appropriate permissions """

import os
from typing import List, Optional, NamedTuple
import hailtop.batch as hb

# Import SampleAPI
from sample_metadata.api.sample_api import SampleApi
from sample_metadata.api.sequence_api import SequenceApi
from sample_metadata.model.sequence_update_model import SequenceUpdateModel


class SampleGroup(NamedTuple):
    """ Defines a group of files associated with each sample"""

    sample_id_external: str
    data_file: str
    index_file: str
    md5: str


def batch_move_files(
    batch: hb.batch,
    sample_group: SampleGroup,
    source_prefix: str,
    destination_prefix: str,
    project: str,
    docker_image: Optional[str] = None,
    key: Optional[str] = None,
) -> List:
    """Creates a list of jobs to perform a batch move operation
    to move files from a source location to a destination.

    Parameters
    ==========
    batch: hb.Batch
        An object representing the DAG of jobs to run.
    files: SampleGroup
        A NamedTuple containing 3 files to be moved.
        For example ["TOB1543.g.vcf.gz","TOB1543.g.vcf.tbi","TOB1543.g.vcf.md5"]
    source_prefix: str
        The path to the sub-directory where the files are initially located.
        For example "cpg-tob-wgs-upload" or "cpg-tob-wgs-upload/v1"
    destination_prefix: str
        The path to the sub-directory where the files should be moved.
        For example "cpg-tob-wgs-main" or "cpg-tob-wgs-upload/batch1"
    docker_image: str, optional
        The address and tag of a previously built docker image, within the
        artifact registry. Each batch job will run in this image.
        For example;
        australia-southeast1-docker.pkg.dev/project/images/driver:version'
    key: str, optional
        key-file for the service account used for authentication. In the case that this
        is not provided as an input, it is assumed that this key will exist at
        /gsa-key/key.json. This is the case when using the hail batch service backend.
        For example:
        "{
            "type": "service_account",
            "project_id": "",
            "private_key_id": "" ...
        }
        " """

    jobs = []

    # Get internal sample ID
    external_id = {"external_ids": [sample_group.sample_id_external]}
    sapi = SampleApi()
    internal_id_map = sapi.get_sample_id_map_by_external(project, external_id)
    internal_id = str(list(internal_id_map.values())[0])

    for tuple_key in sample_group._fields:
        if tuple_key == "sample_id_external":
            continue

        file_name = getattr(sample_group, tuple_key)
        previous_location = os.path.join("gs://", source_prefix, file_name)
        file_extension = file_name[len(sample_group.sample_id_external) :]
        new_file_name = internal_id + file_extension
        new_location = os.path.join("gs://", destination_prefix, new_file_name)

        j = batch.new_job(name=f"move {file_name} -> {new_file_name}")

        if docker_image is not None:
            j.image(docker_image)

        # Authenticate to service account.
        if key is not None:
            j.command(f"echo '{key}' > /tmp/key.json")
            j.command(
                f"gcloud -q auth activate-service-account --key-file=/tmp/key.json"
            )
        # Handles service backend, or a key in the same default location.
        else:
            j.command(
                "gcloud -q auth activate-service-account --key-file=/gsa-key/key.json"
            )

        # Checks file doesn't already exist at the destination, then performs move.
        j.command(
            f'gsutil -q stat "{new_location}" || '
            f'gsutil mv "{previous_location}" "{new_location}"'
        )
        jobs.append(j)

    return jobs


def setup_python_job(
    batch: hb.batch,
    name: str,
    docker_image: Optional[str],
    dependent_jobs=List[hb.batch.job],
) -> hb.batch.job:
    """ Returns a new Hail Batch job that activates the Google service account. """

    job = batch.new_python_job(name=name)

    if docker_image is not None:
        job.image(docker_image)

    job.command("gcloud -q auth activate-service-account --key-file=/gsa-key/key.json")

    job.depends_on(*dependent_jobs)

    return job


def create_analysis(sample_group: SampleGroup, proj, file_path: str, a_type: str):
    """ Creates a new analysis object"""
    sapi = SampleApi()
    aapi = AnalysisApi()
    external_id = {"external_ids": [sample_group.sample_id_external]}
    internal_id_map = sapi.get_sample_id_map(proj, external_id)
    internal_id = list(internal_id_map.values())[0]

    full_path = os.path.join("gs://", file_path, internal_id)

    new_gvcf = AnalysisModel(
        sample_ids=[internal_id],
        type=AnalysisType(a_type),
        status=AnalysisStatus("completed"),
        output=full_path,
    )

    aapi.create_new_analysis(proj, new_gvcf)


def update_sequence_meta(external_id: str, project: str):
    # Determine sequencing ID
    sequence_id = seqapi.get_sequence_id_from_sample_id(external_id, project)

    # Update Sequence Meta
    sequence_metadata = SequenceUpdateModel(
        status=SequencingStatus(status), meta=metadata
    )

    seqapi.update_sequence(sequence_id, sequence_metadata)
