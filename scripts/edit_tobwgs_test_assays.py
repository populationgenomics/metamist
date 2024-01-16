from metamist.apis import AssayApi
from metamist.models import AssayUpsert
import click

from metamist.graphql import gql, query
from cpg_utils import to_path


SAMPLE_QUERY = gql(
    """
    query ($project: String!, $sample_ids: [String!]) {
        sample(project: {eq: $project}, id: {in_: $sample_ids}) {
            id
            externalId
            assays {
            id
            externalIds
            meta
            type
            }
        }
    }
    """
)


def update_assays(assayID: str, sampleID: str, number: int):
    assay_upsert = AssayUpsert(
        id=assayID,
        type=f'non_sequencing{number}',
        sample_id=sampleID,
    )
    # assay_upsert = AssayUpsert(
    #     id=None,
    #     type='sequencing',
    #     sample_id=sampleID,
    #     meta={
    #         'sequencing_type': 'genome',
    #         'sequencing_technology': 'short-read',
    #         'sequencing_platform': 'illumina',
    #     },
    # )

    return assay_upsert


aapi = AssayApi()


@click.command()
@click.option(
    '--project',
    required=True,
    help='The name of the project to add samples to.',
)
@click.option(
    '--new_samples_path',
    required=True,
    help='The path to the file containing the newly created samples.',
)
def main(project: str, new_samples_path: str):
    with to_path(new_samples_path).open() as f:
        new_samples = f.read().splitlines()

    query_response = query(
        SAMPLE_QUERY, {"project": project, "sample_ids": new_samples}
    )

    assay_upserts = []
    for sample in query_response['sample']:
        sampleID = sample['id']
        extID = sample['externalId']
        for i, assay in enumerate(sample['assays']):
            assayID = assay['id']
            assay_upserts.append(update_assays(assayID, sampleID, i))

    for upsert in assay_upserts:
        api_response = aapi.update_assay(upsert)
        print(api_response)


if __name__ == '__main__':
    main()
