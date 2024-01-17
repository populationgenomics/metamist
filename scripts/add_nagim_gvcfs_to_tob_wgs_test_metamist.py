from metamist.graphql import gql, query
from metamist.apis import ParticipantApi, AnalysisApi
from metamist.models import (
    ParticipantUpsert,
    SampleUpsert,
    SequencingGroupUpsert,
    AssayUpsert,
    AnalysisStatus,
    Analysis,
)
from cpg_utils import to_path
import csv

import click

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
    '--project_id',
    required=True,
    type=int,
    help='The ID of the project to add samples to.',
)
@click.option(
    '--new_samples_path',
    required=True,
    help='The path to the file containing the newly created samples.',
)
def main(project: str, projectID: int, new_samples_path: str):
    # Read the CSV file into a dictionary
    extID_to_row = {}
    with to_path(new_samples_path).open() as f:
        reader = csv.reader(f)
        next(reader)  # skip the header
        for row in reader:
            sgid, extID, gvcf, gvcf_idx = row[0], row[1], row[2], row[3]
            extID_to_row[extID] = (sgid, gvcf, gvcf_idx)

    query_response = query(PARTICIPANT_QUERY, {"project": project})
    p_upserts = []
    for participant in query_response['project']['participants']:
        if participant['externalId'] not in extID_to_row:
            continue
        extID = f"{participant['externalId']}-test"
        p = ParticipantUpsert(
            external_id=extID,
            active=None,
            samples=[],
        )
        for sample in participant['samples']:
            s = SampleUpsert(
                external_id=extID,
                project=projectID,
                sequencing_groups=[],
            )
            for sg in sample['sequencingGroups']:
                s.sequencing_groups.append(
                    sg=SequencingGroupUpsert(
                        type=sg['type'],
                        technology=sg['technology'],
                        platform=sg['platform'],
                        meta=None,
                        sample_id=None,
                        external_ids=None,
                        assays=[
                            AssayUpsert(
                                type=sg['assays'][0]['type'],
                                meta={
                                    'sequencing_type': sg['assays'][0]['meta'][
                                        'sequencing_type'
                                    ],
                                    'sequencing_platform': sg['assays'][0]['meta'][
                                        'sequencing_platform'
                                    ],
                                    'sequencing_technology': sg['assays'][0]['meta'][
                                        'sequencing_technology'
                                    ],
                                },
                            ),
                        ],
                    )
                )
            p.samples.append(s)
        p_upserts.append(p)

    upserted_participants = ParticipantApi().upsert_participants(project, p_upserts)

    for participant in upserted_participants:
        for sample in participant['samples']:
            old_extID = sample['externalId'][:-5]  # remove '-test' from the end
            gvcf_path = extID_to_row[old_extID][1]  # get gvcf path from dictionary
            AnalysisApi().create_analysis(
                project,
                Analysis(
                    type='gvcf',
                    status=AnalysisStatus('completed'),
                    output=gvcf_path,
                    sequencing_group_id=sample['sequencingGroups '][0]['id'],
                    project=projectID,
                    active=True,
                    external_id=sample['externalId'],
                    sample=sample['id'],
                ),
            )


if __name__ == '__main__':
    main()
