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
        status=AnalysisStatus('completed'),
        output=gvcf_link,
        sequencing_group_ids=[sgid],
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
    new_samples_path: str,
    sgid_gvcf_mapping_file: str,
):
    # Read in the newly created samples
    with to_path(new_samples_path).open() as f:
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
        new_sgid = sample['sequencingGroups'][0]['id']
        if ext_id in extID_to_row:
            _, gvcf, gvcf_idx = extID_to_row[ext_id]
            analysis_upserts.append(
                add_analysis_to_metamist(gvcf, new_sgid, project_id)
            )

    # Add the analyses to metamist
    for upsert in analysis_upserts:
        api_response = aapi.create_analysis(project, upsert)
        print(api_response)


if __name__ == '__main__':
    main()


# Analyses added
# 193733
# 193734
# 193735
# 193736
# 193737
# 193738
# 193739
# 193740
# 193741
# 193742
# 193743
# 193744
# 193745
# 193746
# 193747
# 193748
# 193749
# 193750
# 193751
# 193752
# 193753
# 193754
# 193755
# 193756
# 193757
# 193758
# 193759
# 193760
# 193761
# 193762
# 193763
# 193764
# 193765
# 193766
# 193767
# 193768
# 193769
# 193770
# 193771
# 193772
# 193773
# 193774
# 193775
# 193776
# 193777
# 193778
# 193779
# 193780
# 193781
# 193782
# 193783
# 193784
# 193785
# 193786
# 193787
# 193788
# 193789
# 193790
# 193791
# 193792
# 193793
# 193794
# 193795
# 193796
# 193797
# 193798
# 193799
# 193800
# 193801
# 193802
# 193803
# 193804
# 193805
# 193806
# 193807
# 193808
# 193809
# 193810
# 193811
# 193812
# 193813
# 193814
# 193815
# 193816
# 193817
# 193818
# 193819
# 193820
# 193821
# 193822
# 193823
# 193824
# 193825
# 193826
# 193827
