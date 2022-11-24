"""
Previously, samples where ingested before we updated our metamist model to include multiple
sequence IDs. This script is a quick fix that will back-populate sequence IDs for previously ingested projects.
The sequence ID is pulled from the file path of the fastq's.
This simple script assumes output paths are already populated, and follow the convention
FLOW-CELL-ID_LANE_SAMPLE-NAME_SPECIES_SEQUENCING-INDEX_BATCH_READ
Where the sequence ID corresponds to FLOW-CELL-ID_LANE_SAMPLE-NAME
For example:
HTJMHDSX3_4_220817_FD123456_Homo-sapiens_AGATCCATTA-TGGTAGAGAT_R_220208_BATCH_M014_R2.fastq.gz.md5
The sequence ID would be HTJMHDSX3_4_220817_FD123456
"""
import logging
import click

from sample_metadata.apis import SequenceApi
from sample_metadata.models import SequenceUpdateModel

logger = logging.getLogger(__file__)
logging.basicConfig(format='%(levelname)s (%(name)s %(lineno)s): %(message)s')
logger.setLevel(logging.INFO)


@click.command()
@click.option(
    '--project',
    required=True,
    help='The sample-metadata project ($DATASET)',
)
def main(project: str):
    """Back populate external_ids for existing sequences"""

    seqapi = SequenceApi()
    # Pull all the sequences
    sequences = seqapi.get_sequences_by_criteria(
        active=True,
        body_get_sequences_by_criteria={
            'projects': [project],
        },
    )

    # For logs
    updated_sequences: list[dict[str, str]] = []

    for sequence in sequences:
        internal_sequence_id = sequence.get('id')
        current_external_ids = sequence.get('external_ids')

        # Quick validation
        if current_external_ids:
            logging.warning(
                f'{internal_sequence_id} already has external sequence id/s set: {current_external_ids}. Skipping'
            )
            continue

        try:
            fastq_filename = sequence.get('meta').get('reads')[0][0].get('basename')
        except TypeError:
            # Can't determine seq_id
            logging.warning(f'No reads for {internal_sequence_id} skipping {sequence}')
            continue

        # Pull external sequence id
        split_filename = fastq_filename.split('_')[0:4]
        external_sequence_id = '_'.join(split_filename)

        # Add sequence id to existing sequence object
        seqapi.update_sequence(
            internal_sequence_id,
            SequenceUpdateModel(external_ids={'kccg_id': external_sequence_id}),
        )
        updated_sequences.append({internal_sequence_id: external_sequence_id})

    logging.info(f'Updated {len(updated_sequences)} sequences. {updated_sequences}')


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
