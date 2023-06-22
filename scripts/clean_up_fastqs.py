"""
This script cleans up the FASTQ sequences and metadata within GCS and Metamist.

    2023-04-26 mfranklin: With the new sequencing-group change, this was slightly harder
        that straight trivial to fix - therefore I've commented it out with a future
        missing to fix it, or more likely to consolidate this with the
        check_sequence_files.py scripts to produce a bunch of "ingestion utilities".

        Feel free to chat to me if there's any questions about this.
"""
# import logging
# import coloredlogs
# import click
#
# from cloudpathlib import CloudPath
# from google.cloud import storage
#
# from metamist.apis import AssayApi
# from metamist.models import AssayUpsert
#
#
# client = storage.Client()
# asapi = AssayApi()
# coloredlogs.install()
#
#
# def clean_up_db_and_storage(
#     assays: list[dict],
#     clean_up_location: str,
#     dry_run,
# ):
#     """Deleting sequencing entires for specified sequences in metamist
#     This will delete all of the reads for the latest sequence."""
#     sequence_ids: list[int] = []
#     for sequence in assays:
#         reads = sequence['meta'].get('reads')
#         sequence_id = sequence.get('id')
#         current_read_locations: list[CloudPath] = []
#         # Skip delete operations if there are no reads for the given sequence
#         delete_reads = bool(reads)
#         if not delete_reads:
#             logging.warning(f'No reads found for {sequence_id}. Skipping.')
#             continue
#
#         reads_type = sequence['meta'].get('reads_type')
#
#         if reads_type:
#             if reads_type != 'fastq':
#                 # Currently not supporting mass deleting of crams
#                 logging.warning(f'Reads type is not fastq, got {reads_type} instead.')
#                 continue
#         else:
#             logging.warning(f'The reads_type was not set to fastq for {sequence_id}')
#             continue
#
#         for read in reads:
#             for strand in read:
#                 cloud_path = strand['location']
#                 read_blob = CloudPath(cloud_path)
#                 # Validates the file that we want to delete, is in the bucket specified.
#                 if read_blob.exists() and cloud_path.startswith(clean_up_location):
#                     current_read_locations.append(read_blob)
#                 else:
#                     logging.error(
#                         f'{read_blob.name} was not found in {clean_up_location}. Skipping delete of {sequence_id} from metamist'
#                     )
#                     # Skip delete operations, something might be wrong with the reads meta locations.
#                     delete_reads = False
#                     continue
#         # Clear reads metadata, including location, basename, class, checksum & size
#         if delete_reads:
#             if dry_run:
#                 logging.info(f'dry-run :: Would delete {current_read_locations}')
#             else:
#                 # Delete the reads meta from metamist & files from storage
#                 clean_up_cloud_storage(current_read_locations)
#                 api_response = asapi.update_assay(
#                     AssayUpsert(id=sequence_id, meta={'reads': []})
#                 )
#                 sequence_ids.append(int(api_response))
#                 logging.info(
#                     f'{sequence_id} reads metadata was cleared from metamist & cloud storage'
#                 )
#
#     return sequence_ids
#
#
# def clean_up_cloud_storage(locations: list[CloudPath]):
#     """Given a list of locations of files to be deleted"""
#     for location in locations:
#         location.unlink()
#         logging.info(f'{location.name} was deleted from cloud storage.')
#
#
# def get_sequences_from_sample(sample_ids: list[str], sequencing_type, batch_filter):
#     """Return latest sequence ids for sample ids"""
#     sequences: list[dict] = []
#     all_sequences = seqapi.get_sequences_by_sample_ids(
#         sample_ids, get_latest_sequence_only=False
#     )
#     for sequence in all_sequences:
#         if sequencing_type != sequence.get('type'):
#             continue
#
#         if batch_filter:
#             if sequence['meta'].get('batch') == batch_filter:
#                 sequences.append(sequence)
#         else:
#             sequences.append(sequence)
#
#     return sequences
#
#
# def validate_crams(sample_ids: list[str], project) -> list[str]:
#     """Checks that crams exist for the sample. If not, it will not delete the fastqs"""
#     samples_without_crams = aapi.get_all_sample_ids_without_analysis_type(
#         'cram', project
#     )
#
#     if samples_without_crams:
#         sample_ids_without_crams = samples_without_crams['sample_ids']
#         logging.warning(f'No crams found for {sample_ids_without_crams}, skipping.')
#         sample_ids = list(set(sample_ids) - set(sample_ids_without_crams))
#     return sample_ids
#
#
# @click.command()
# @click.option(
#     '--clean-up-location',
#     help='Cloud path to bucket where files will be deleted from e.g. "gs://cpg-bucket-upload"',
# )
# @click.option('--project', help='Metamist project, used to filter samples')
# @click.option('--external-sample-ids', default=None, multiple=True)
# @click.option('--internal-sample-ids', default=None, multiple=True)
# @click.option(
#     '--batch-filter',
#     default=None,
#     help='Batch name for filtering sequences. This will correspond to the field stored in sequence.meta.batch. e.g. BATCH001',
# )
# @click.option('--sequence-type', default=None, help='e.g. "genome"')
# @click.option('--force', is_flag=True, help='Do not confirm updates')
# @click.option(
#     '--dry-run',
#     is_flag=True,
# )
# def main(
#     internal_sample_ids: list[str],
#     clean_up_location,
#     project,
#     external_sample_ids,
#     batch_filter,
#     sequencing_type,
#     force,
#     dry_run,
# ):
#     """Performs validation then deletes fastqs from metamist and cloud storage"""
#     if not internal_sample_ids and not external_sample_ids:
#         if batch_filter is None:
#             logging.error(f'Please specify either a batch filter or a set of IDs.')
#             return
#
#         samples = sapi.get_samples(body_get_samples={'project_ids': [project]})
#         internal_sample_ids = [d['id'] for d in samples]
#     else:
#         if len(external_sample_ids) > 0:
#             # Get Sample Id Map By External
#             external_sample_ids = list(external_sample_ids)
#             sample_map = sapi.get_sample_id_map_by_external(
#                 project, external_sample_ids, allow_missing=False
#             )
#             internal_sample_ids = list(sample_map.values())
#
#         if len(internal_sample_ids) > 0:
#             internal_sample_ids = list(internal_sample_ids)
#
#     # Validate CRAMs exist before deleting fastqs
#     # only get sample IDs that can be removed (because they have crams)
#     internal_sample_ids_to_remove = validate_crams(internal_sample_ids, project)
#
#     sequences = get_sequences_from_sample(
#         internal_sample_ids_to_remove, sequencing_type, batch_filter
#     )
#     sequence_ids = [seq['id'] for seq in sequences]
#
#     message = f'Deleting {len(sequences)} sequences: {sequence_ids}'
#     if force:
#         print(message)
#     elif not click.confirm(message, default=False):
#         raise click.Abort()
#
#     cleared_ids = clean_up_db_and_storage(sequences, clean_up_location, dry_run)
#
#     failed_ids = list(set(sequence_ids) - set(cleared_ids))
#     if len(failed_ids) != 0:
#         logging.warning(f'Some sequences were not deleted appropriately {failed_ids}')
#
#
# if __name__ == '__main__':
#     main()  # pylint: disable=E1120
