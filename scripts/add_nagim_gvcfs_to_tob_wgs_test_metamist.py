import csv
from dataclasses import dataclass
from pprint import pprint

import click
from cpg_utils import to_path

from metamist.apis import ParticipantApi, AnalysisApi
from metamist.graphql import gql, query
from metamist.models import (
    ParticipantUpsert,
    SampleUpsert,
    SequencingGroupUpsert,
    AssayUpsert,
    AnalysisStatus,
    Analysis,
)


@dataclass
class RowData:
    """
    A class used to represent a row of data.

    Attributes
    ----------
    sgid : str
        an identifier for the sequencing group
    ext_id : str
        an external identifier for the row
    gvcf : str
        the path to the gvcf file
    gvcf_idx : str
        the path to the gvcf index file
    """

    sgid: str
    ext_id: str
    gvcf: str
    gvcf_idx: str


PARTICIPANT_QUERY = gql(
    """
    query ($project: String!) {
        project(name: $project) {
            id
            participants {
            externalId
            id
            samples {
                id
                externalId
                sequencingGroups {
                    externalIds
                    id
                    meta
                    platform
                    technology
                    type
                    assays {
                        meta
                        id
                        }
                    }
                }
            }
        }
    }
    """
)


@click.command()
@click.option(
    '--project',
    required=True,
    help='The name of the project to add samples to.',
)
@click.option(
    '--sample-path-mappings',
    required=True,
    help="""The path to a CSV file containing mappings of `main` CPG ID's,
            the `external_id` and `gvcf` paths.
            The file should have at least four columns: sgid, ext_id, gvcf, and gvcf_idx.
            Here's an example of what the first couple of lines might look like:

            sgid,ext_id,gvcf,gvcf_idx
            sg1,ext1,gvcf1,gvcf_idx1
            sg2,ext2,gvcf2,gvcf_idx2
        """,
)
@click.option(
    '--project-id',
    required=True,
    type=int,
    help="""The ID of the project to add samples to.
            For example: iterate over `main` project (identified by the --project flag) to get data for each sample, then create a new participant
            with the same data, but with a new external ID that has the suffix (--suffix) specified by the user.
            Then upsert these into the `test` project.
        """,
)
@click.option(
    '--suffix',
    required=True,
    help="""The suffix to add to the external ID's of the participants.
            For example, if the suffix is `test`, then the external ID's of the participants
            will be `ext_id1-test`, `ext_id2-test`, etc.
        """,
)
def main(project: str, project_id: int, sample_path_mappings: str, suffix: str):
    """
    Iterate over `main` project to get data for each sample, then create a new participant
    with the same data, but with a new external ID that has the suffix specified by the user.
    Then upsert these into the `test` project.
    """
    # Read the CSV file into a dictionary
    ext_id_to_row = {}
    with to_path(sample_path_mappings).open() as f:
        reader = csv.reader(f)
        next(reader)  # skip the header
        for row in reader:
            data = RowData(*row[:4])
            ext_id_to_row[data.ext_id] = data

    query_response = query(PARTICIPANT_QUERY, {'project': project})
    p_upserts = []
    # pylint: disable=unsubscriptable-object
    for participant in query_response['project']['participants']:
        if participant['externalId'] not in ext_id_to_row:
            continue
        ext_id = f"{participant['externalId']}-{suffix}"
        p = ParticipantUpsert(
            external_id=ext_id,
            active=None,
            samples=[],
        )
        for sample in participant['samples']:
            s = SampleUpsert(
                external_id=ext_id,
                project=project_id,
                sequencing_groups=[],
            )
            for sg in sample['sequencingGroups']:
                pprint(sg)
                s.sequencing_groups.append(
                    SequencingGroupUpsert(
                        type=sg['type'],
                        technology=sg['technology'],
                        platform=sg['platform'],
                        meta=None,
                        sample_id=None,
                        external_ids=None,
                        assays=[
                            AssayUpsert(
                                type=sg['type'],
                                meta={
                                    'sequencing_type': sg['sequencing_type'],
                                    'sequencing_platform': sg['sequencing_platform'],
                                    'sequencing_technology': sg[
                                        'sequencing_technology'
                                    ],
                                },
                            ),
                        ],
                    )
                )
            p.samples.append(s)
        p_upserts.append(p)
    # pylint: enable=unsubscriptable-object

    upserted_participants = ParticipantApi().upsert_participants(project, p_upserts)

    for participant in upserted_participants:
        for sample in participant['samples']:
            old_ext_id = sample['externalId'].removesuffix(f'-{suffix}')
            row_data = ext_id_to_row[old_ext_id]
            gvcf_path = row_data.gvcf  # get gvcf path from dictionary
            AnalysisApi().create_analysis(
                project,
                Analysis(
                    type='gvcf',
                    status=AnalysisStatus('completed'),
                    output=gvcf_path,
                    sequencing_group_id=sample['sequencingGroups'][0]['id'],
                    active=True,
                ),
            )


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
