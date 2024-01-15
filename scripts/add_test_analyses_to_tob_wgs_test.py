from metamist.apis import AnalysisApi
from metamist.models import AnalysisStatus
from metamist.models import Analysis
import csv
from datetime import datetime
import click

from metamist.graphql import gql, query
from cpg_utils import to_path

aapi = AnalysisApi()

SAMPLE_QUERY = gql(
    """
    query ($project: String!, $sample_ids: [String!]) {
        sample(project: {eq: $project}, id: {in_: $sample_ids}) {
            externalId
            sequencingGroups {
                id
                assays {
                    id
                    meta
                }
            }
        }
    }
    """
)


def add_analysis_to_metamist(gvcf_link: str, sgid: str, project_id: int):
    analysis_upsert = Analysis(
        type='gvcf',
        status=AnalysisStatus("COMPLETED"),
        id=None,
        output=gvcf_link,
        sequencing_group_ids=[sgid],
        author=None,
        timestamp_completed=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        project=project_id,
        active=True,
        meta={},
    )

    return analysis_upsert


@click.command()
@click.option(
    '--project',
    required=True,
    help='The name of the project to add samples to.',
)
@click.option(
    '--project_id',
    required=True,
    type=int,
    help='The ID of the project to add samples to.',
)
@click.option(
    '--new_samples_path',
    required=True,
    help='Path to the CSV file with sample data.',
)
@click.option(
    '--sgid_gvcf_mapping_file',
    required=True,
    help='Path to the CSV file with sample-gvcf data.',
)
def main(
    project: str,
    project_id: int,
    newly_added_samples_path: str,
    sgid_gvcf_mapping_file: str,
):
    # Read in the newly created samples
    with to_path(newly_added_samples_path).open() as f:
        samples = f.read().splitlines()

    # Read the CSV file into a dictionary
    extID_to_row = {}
    with to_path(sgid_gvcf_mapping_file).open() as f:
        reader = csv.reader(f)
        next(reader)  # skip the header
        for row in reader:
            sgid, extID, gvcf, gvcf_idx = row[0], row[1], row[2], row[3]
            extID_to_row[extID] = (sgid, gvcf, gvcf_idx)

    # Get the newly created samples
    query_response = query(SAMPLE_QUERY, {"project": project, "sample_ids": samples})

    # Look up each sample in the dictionary
    analysis_upserts = []
    for sample in query_response['sample']:
        ext_id = sample['externalId']
        if ext_id in extID_to_row:
            sgid, gvcf, gvcf_idx = extID_to_row[ext_id]
            analysis_upserts.append(add_analysis_to_metamist(gvcf, sgid, project_id))

    # Add the analyses to metamist
    for upsert in analysis_upserts:
        api_response = aapi.create_analysis(project, upsert)
        print(api_response)


if __name__ == '__main__':
    main()
