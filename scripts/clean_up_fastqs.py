""" This script cleans up the fastq sequence data and metadata within GCS and Metamist """

import logging
import coloredlogs
import click

from cloudpathlib import CloudPath
from google.cloud import storage

from sample_metadata.apis import SampleApi, SequenceApi, AnalysisApi
from sample_metadata.models import SequenceUpdateModel, AnalysisType


client = storage.Client()
aapi = AnalysisApi()
sapi = SampleApi()
seqapi = SequenceApi()
coloredlogs.install()


def clean_up_db_and_storage(
    sequences: list[dict],
    clean_up_location: str,
):
    """Deleting sequencing entires for specified sequences in metamist
    This will delete all of the reads for the latest sequence."""
    sequence_ids: list[int] = []
    for sequence in sequences:
        reads = sequence['meta'].get('reads')
        sequence_id = sequence.get('id')
        current_read_locations: list[CloudPath] = []
        # Skip delete operations if there are no reads for the given sequence
        delete_reads = bool(reads)
        if not delete_reads:
            logging.warning(f'No reads found for {sequence_id}. Skipping.')
            continue

        reads_type = sequence['meta'].get('reads_type')

        if reads_type:
            if reads_type != 'fastq':
                # Currently not supporting mass deleting of crams
                logging.warning(f'Reads type is not fastq, got {reads_type} instead.')
                continue
        else:
            logging.warning(f'The reads_type was not set to fastq for {sequence_id}')
            continue

        for read in reads:
            for strand in read:
                cloud_path = strand['location']
                read_blob = CloudPath(cloud_path)
                # Validates the file that we want to delete, is in the bucket specified.
                if read_blob.exists() and cloud_path.startswith(clean_up_location):
                    current_read_locations.append(read_blob)
                else:
                    logging.error(
                        f'{read_blob.name} was not found in {clean_up_location}. Skipping delete of {sequence_id} from metamist'
                    )
                    # Skip delete operations, something might be wrong with the reads meta locations.
                    delete_reads = False
                    continue
        # Clear reads metadata, including location, basename, class, checksum & size
        if delete_reads:
            # Delete the reads meta from metamist & files from storage
            clean_up_cloud_storage(current_read_locations)
            sequence_update_model = SequenceUpdateModel(meta={'reads': []})
            api_response = seqapi.update_sequence(sequence_id, sequence_update_model)
            sequence_ids.append(int(api_response))
            logging.info(
                f'{int(api_response)}\'s reads metadata was cleared from metamist & cloud storage'
            )

    return sequence_ids


def clean_up_cloud_storage(locations: list[CloudPath]):
    """Given a list of locations of files to be deleted"""
    for location in locations:
        location.unlink()
        logging.info(f'{location.name} was deleted from cloud storage.')


def get_sequences_from_sample(sample_ids: list[str], sequence_type, batch_filter):
    """Return latest sequence ids for sample ids"""
    sequences: list[dict] = []
    all_sequences = seqapi.get_sequences_by_sample_ids(sample_ids)
    for sequence in all_sequences:
        if sequence_type == sequence.get('type'):
            if batch_filter:
                if sequence['meta'].get('batch') == batch_filter:
                    sequences.append(sequence)
            else:
                sequences.append(sequence)

    return sequences


def validate_crams(sample_ids: list[str], project):
    """Checks that crams exist for the sample. If not, it will not delete the fastqs"""
    samples_without_crams = aapi.get_all_sample_ids_without_analysis_type(
        AnalysisType('cram'), project
    )

    if samples_without_crams:
        sample_ids_without_crams = samples_without_crams['sample_ids']
        logging.warning(f'No crams found for {sample_ids_without_crams}, skipping.')
        sample_ids = list(set(sample_ids) - set(sample_ids_without_crams))
    return sample_ids


@click.command()
@click.option(
    '--clean-up-location',
    help='Cloud path to bucket where files will be deleted from e.g. "gs://cpg-bucket-upload"',
)
@click.option('--project', help='Metamist project, used to filter samples')
@click.option('--external-sample-ids', default=None, multiple=True)
@click.option('--internal-sample-ids', default=None, multiple=True)
@click.option(
    '--batch-filter',
    default=None,
    help='Batch name for filtering sequences. This will correspond to the field stored in sequence.meta.batch. e.g. BATCH001',
)
@click.option('--sequence-type', default=None, help='e.g. "genome"')
@click.option('--force', is_flag=True, help='Do not confirm updates')
def main(
    internal_sample_ids: list[str],
    clean_up_location,
    project,
    external_sample_ids,
    batch_filter,
    sequence_type,
    force,
):
    """Performs validation then deletes fastqs from metamist and cloud storage"""
    if internal_sample_ids == () and external_sample_ids == ():
        if batch_filter is None:
            logging.error(f'Please specify either a batch filter or a set of IDs.')
            return

        samples = sapi.get_samples(body_get_samples={'project_ids': [project]})
        internal_sample_ids = [d['id'] for d in samples]
    else:
        if len(external_sample_ids) > 0:
            # Get Sample Id Map By External
            external_sample_ids = list(external_sample_ids)
            sample_map = sapi.get_sample_id_map_by_external(
                project, external_sample_ids, allow_missing=False
            )
            internal_sample_ids = list(sample_map.values())

        if len(internal_sample_ids) > 0:
            internal_sample_ids = list(internal_sample_ids)

    # Validate CRAMs exist before deleting fastqs
    internal_sample_ids = validate_crams(internal_sample_ids, project)

    sequences = get_sequences_from_sample(
        internal_sample_ids, sequence_type, batch_filter
    )
    sequence_ids = [seq['id'] for seq in sequences]

    message = f'Deleting {len(sequences)} sequences: {sequences}'
    if force:
        print(message)
    elif not click.confirm(message, default=False):
        raise click.Abort()

    cleared_ids = clean_up_db_and_storage(sequences, clean_up_location)

    failed_ids = list(set(sequence_ids) - set(cleared_ids))
    if len(failed_ids) != 0:
        logging.warning(f'Some sequences were not deleted appropriately {failed_ids}')


if __name__ == '__main__':
    main()  # pylint: disable=E1120
