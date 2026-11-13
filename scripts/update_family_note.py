import asyncio
from collections import defaultdict
import click

from sample_metadata.api.family_api import FamilyApi, FamilyUpdateModel
from sample_metadata.graphql import query

FAPI = FamilyApi()


def get_rd_families(excluded: list[str]) -> list[dict[str, dict]]:
    """Uses a graphql query to get all families from all non-test rd datasets"""

    _query = """
        query MyQuery {
            myProjects {
                dataset
                families {
                    description
                    externalId
                    id
                }
            }
        }
    """

    my_datasets = query(_query)['myProjects']

    # Filter out any empty, test, or excluded datasets
    return [
        {dataset['dataset']: dataset['families']}
        for dataset in my_datasets
        if dataset['families']
        and 'test' not in dataset['dataset']
        and dataset['dataset'] not in excluded
    ]


def get_current_family_descriptions(
    datasets_families: list[dict[str, dict]]
) -> dict[int, str]:
    """Extracts the current family description field for all families"""
    family_descriptions = {}
    for dataset_families in datasets_families:
        for _, families in dataset_families.items():
            for family in families:
                family_id = family['id']
                family_desc = family['description']

                family_descriptions[family_id] = family_desc

    return family_descriptions


def get_families_analyses(
    family_descriptions: dict[int, str]
) -> defaultdict[int, list]:
    """Uses graphql to get the analysis entries for participants in each family"""
    _query = """
            query FamilyAnalyses($familyId: Int!) {
                family(familyId: $familyId) {
                    id
                    participants {
                        id
                        samples {
                            id
                            analyses(status: COMPLETED) {
                                id
                                meta
                                type
                                status
                                output
                                timestampCompleted
                            }
                        }
                    }
                }
            }
    """

    families_analyses = defaultdict(list)
    for family_id, _ in family_descriptions.items():
        family_participants = query(_query, {'familyId': family_id})['family'][
            'participants'
        ]

        participants_samples = [
            {participant['id']: participant['samples']}
            for participant in family_participants
            if participant['samples']
        ]

        samples_analyses = [
            {sample['id']: sample['analyses']}
            for participant in participants_samples
            for sample in participant.values()
        ]

        family_sample_ids = [
            sample_id
            for sample_analysis in samples_analyses
            for sample_id in sample_analysis.keys()
        ]

        analyses = [
            analysis
            for sample_analysis in samples_analyses
            for analysis in sample_analysis.values()
        ]

        # Iterate through the analyses and look for the ones to save to the description
        for analysis in analyses:
            if analysis['type'] not in [
                'QC',  # What analysis types are we looking for?
            ]:
                continue
            output = analysis['output']
            meta = analysis['meta']
            meta_samples = meta['samples']

            # Save the analysis if all samples in the family are part of the analysis
            if all(sample_id in meta_samples for sample_id in family_sample_ids):
                families_analyses[family_id].append(analysis)

        # TODO: define the analyses to collect for each family/participant/sample

        # Analysis links to collect:
        # Project:
        #  - [CRAM multiQC](link) - QC statistics for all ccram files in this project
        #  - [AIP report](link) - Variant prioritisation report
        # Family:
        #  - [VAARP report](link) - Variant prioritisation report
        #  - [Peddy relatedness](link) - Relatedness checks for family
        # Individual:
        #  - {externalId}: [Stripy](link), [ReportX](link)

        return families_analyses


def update_family_descriptions(
    family_analysis_entries: defaultdict[int, list], family_descriptions: dict[int, str]
):
    """Appends the analysis links of interest to the bottom of the family description field"""
    for family_id, analyses in family_analysis_entries:
        current_desc = ''
        # If a description already exists, add a linebreak, hashes, and another linebreak
        if family_descriptions[family_id]:
            current_desc = family_descriptions[family_id]

        # Define the start of the external links as a markdown header
        updated_desc = current_desc + '\n##########\n# External Analysis Links\n'
        for analysis in analyses:
            if analysis['type'] == 'QC':
                updated_desc += f' - [CRAM multiQC]({analysis["output"]})'

            # TODO: add additional links to updated_desc once they are defined and extracted

        # For each family ID, use the FamilyUpdateModel api with the new description
        FAPI.update_family(FamilyUpdateModel(id=family_id, description=updated_desc))


@click.command(
    help='Add formatted analysis links to the family notes field for all RD projects in seqr'
)
@click.argument('excluded', nargs=-1)
def main(
    excluded: list[str],
):
    """
    Updates all RD dataset families description field to append analysis links.
    Include dataset names as arguments to exclude datasets from the updates.
    """
    datasets_families = get_rd_families(excluded)
    family_descriptions = get_current_family_descriptions(datasets_families)
    families_analyses = get_families_analyses(family_descriptions)

    update_family_descriptions(families_analyses, family_descriptions)


if __name__ == '__main__':
    main()  # pylint: disable=no-value-for-parameter
