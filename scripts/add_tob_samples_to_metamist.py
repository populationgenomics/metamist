from metamist.apis import SampleApi
from metamist.models import SampleUpsert, SequencingGroupUpsert, AssayUpsert
from cpg_utils import to_path
import csv

import click

sapi = SampleApi()


def add_sample_to_metamist(extID: str, project: str):
    """
    Adds samples to metamist

    Args:
        sample_ids (list[str]): List of sample IDs to add to metamist
        project (str): The name of the project to add samples to
    """
    sample_upsert = SampleUpsert(
        id=None,
        external_id=extID,
        meta={},
        project=8,
        type='blood',
        participant_id=None,
        active=None,
        sequencing_groups=[
            SequencingGroupUpsert(
                id=None,
                type='genome',
                technology='short-read',
                platform='illumina',
                meta=None,
                sample_id=None,
                external_ids=None,
                assays=[],
            ),
        ],
        non_sequencing_assays=[
            AssayUpsert(
                id=None,
                type='sequencing',
                external_ids=None,
                sample_id=None,
                meta=None,
            ),
        ],
    )

    return sample_upsert


@click.command()
@click.option(
    '--project',
    required=True,
    help='The name of the project to add samples to.',
)
@click.option(
    '--files_path',
    required=True,
    help='Path to the CSV file with sample data.',
)
def main(project: str, files_path: str):
    """
    Adds samples to metamist

    Args:
        project (str): The name of the project to add samples to
        files_path (str): Path to the CSV file with sample data
    """
    # Get list of sample IDs from bucket directory
    upsert_list = []
    with to_path(files_path).open() as f:
        reader = csv.reader(f)
        next(reader)  # skip the header
        for row in reader:
            sgid, extID, gvcf, gvcf_idx = row[0], row[1], row[2], row[3]
            upsert_list.append(add_sample_to_metamist(extID, project))

    for upsert in upsert_list:
        api_response = sapi.create_sample(
            project, upsert
        )  # returns the internal sample ID
        print(api_response)


if __name__ == '__main__':
    main()
