"""
Previously, samples where ingested before we updated our metamist model to include multiple
assay IDs. This script is a quick fix that will back-populate assay IDs for previously ingested projects.
The assay ID is pulled from the file path of the fastq's.
This simple script assumes output paths are already populated, and follow the convention
FLOW-CELL-ID_LANE_SAMPLE-NAME_SPECIES_SEQUENCING-INDEX_BATCH_READ
Where the assay ID corresponds to FLOW-CELL-ID_LANE_SAMPLE-NAME
For example:
HTJMHDSX3_4_220817_FD123456_Homo-sapiens_AGATCCATTA-TGGTAGAGAT_R_220208_BATCH_M014_R2.fastq.gz.md5
The assay ID would be HTJMHDSX3_4_220817_FD123456
"""
import logging
import click

from metamist.apis import AssayApi
from metamist.models import AssayUpsert

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
    """Back populate external_ids for existing assays"""

    # Pull all the assays
    assayapi = AssayApi()
    assays = assayapi.get_assays_by_criteria(
        active=True,
        body_get_assays_by_criteria={
            'projects': [project],
        },
    )

    # For logs
    updated_assays: list[dict[str, str]] = []

    for assay in assays:
        internal_assay_id = assay.get('id')
        current_external_ids = assay.get('external_ids')

        # Quick validation
        if current_external_ids:
            logging.warning(
                f'{internal_assay_id} already has external assay id/s set: {current_external_ids}. Skipping'
            )
            continue

        try:
            fastq_filename = assay.get('meta').get('reads')[0].get('basename')
        except TypeError:
            # Can't determine fastq filename, hence can't pull ID from it
            logging.warning(f'No reads for {internal_assay_id} skipping {assay}')
            continue

        # Pull external assay id
        split_filename = fastq_filename.split('_')[0:4]
        external_assay_id = '_'.join(split_filename)

        # Add assay id to existing assay object
        assayapi.update_assay(AssayUpsert(
            id=internal_assay_id,
            external_ids={'kccg_id': external_assay_id},
        ))
        updated_assays.append({internal_assay_id: external_assay_id})

    logging.info(f'Updated {len(updated_assays)} assays. {updated_assays}')


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
