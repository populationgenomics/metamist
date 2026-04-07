#!/usr/bin/env python3
import asyncio
import csv
import datetime
import logging
import os
import random
import sys
import tempfile

from metamist.apis import (
    AnalysisApi,
    CohortApi,
    EnumsApi,
    FamilyApi,
    ParticipantApi,
    ProjectApi,
    SampleApi,
)
from metamist.graphql import gql, query_async
from metamist.model.analysis import Analysis
from metamist.models import (
    AnalysisStatus,
    AssayUpsert,
    BodyCreateCohortFromCriteria,
    CohortBody,
    CohortCriteria,
    SampleUpsert,
    SequencingGroupUpsert,
)
from metamist.parser.generic_parser import chunk


PRIMARY_EXTERNAL_ORG = ''

NAMES = [
    'SOLAR',
    'LUNAR',
    'MARS',
    'VENUS',
    'PLUTO',
    'COMET',
    'METEOR',
    'ORION',
    'VIRGO',
    'LEO',
    'ARIES',
    'LIBRA',
    'PHEON',
    'CYGNUS',
    'CRUX',
    'CANIS',
    'HYDRA',
    'LYNX',
    'INDUS',
    'RIGEL',
    'PERSE',
    'QUASAR',
    'PULSAR',
    'HALO',
    'NOVA',
]

PROJECTS = [
    'SIGMA',
    'DELTA',
    'ALPHA',
    'BETA',
    'GAMMA',
    'ZETA',
    'ETA',
    'THETA',
    'PHI',
    'CHI',
    'PSI',
    'OMEGA',
]

SEQ_TYPES = ['genome', 'exome', 'transcriptome']
SEQ_TECHS = ['short-read', 'long-read']
SEQ_PLATFORMS = ['illumina', 'oxford-nanopore', 'pacbio']

LOCI = [
    'ABCD',
    'EFGH',
    'IJKL',
]

QUERY_PROJECT_ID = gql(
    """
    query ProjectIdQuery($project: String!) {
        project(name: $project) {
            id
        }
    }
    """
)

QUERY_PROJECT_SGS = gql(
    """
    query MyQuery($project: String!) {
        project(name: $project) {
            sequencingGroups {
                id
                type
                technology
                platform
            }
        }
    }
    """
)

QUERY_ENUMS = gql(
    """
    query EnumsQuery {
        enum {
            analysisType
            assayType
            sampleType
            sequencingPlatform
            sequencingTechnology
            sequencingType
        }
    }
    """
)


class ped_row:  # noqa: N801
    """The pedigree row class"""

    def __init__(self, values):
        (
            self.family_id,
            self.individual_id,
            self.paternal_id,
            self.maternal_id,
            self.sex,
            self.affected,
        ) = values

    def __iter__(self):
        yield self.family_id
        yield self.individual_id
        yield self.paternal_id
        yield self.maternal_id
        yield self.sex
        yield self.affected


def generate_random_id(used_ids: set):
    """Generate a random ID using the NAMES list."""
    random_id = f'{random.choice(NAMES)}_{random.randint(1, 9999):04}'
    if random_id in used_ids:
        return generate_random_id(used_ids)
    used_ids.add(random_id)
    return random_id


def generate_pedigree_rows(num_families=1):  # noqa: D417
    """
    Generate rows for a pedigree file with random data.

    Parameters
    ----------
    - num_families: The number of families to generate.

    Returns
    -------
    A list of ped_row objects representing a project's pedigree.
    """
    used_ids: set[str] = set()
    rows: list[ped_row] = []
    for _ in range(num_families):
        num_individuals_in_family = random.randint(1, 5)
        family_id = generate_random_id(used_ids)
        founders = []

        if num_individuals_in_family == 1:  # Singleton
            individual_id = generate_random_id(used_ids)
            rows.append(
                ped_row([family_id, individual_id, '', '', random.choice([0, 1]), 2])
            )
            continue

        if num_individuals_in_family == 2:  # Duo  # noqa: PLR2004
            parent_id = generate_random_id(used_ids)
            parent_sex = random.choice([1, 2])
            parent_affected = random.choices([0, 1, 2], weights=[0.05, 0.8, 0.15], k=1)[
                0
            ]
            rows.append(
                ped_row([family_id, parent_id, '', '', parent_sex, parent_affected])
            )

            individual_id = generate_random_id(used_ids)
            if parent_sex == 1:
                rows.append(
                    ped_row(
                        [
                            family_id,
                            individual_id,
                            parent_id,
                            '',
                            random.choice([0, 1]),
                            2,
                        ]
                    )
                )
            else:
                rows.append(
                    ped_row(
                        [
                            family_id,
                            individual_id,
                            '',
                            parent_id,
                            random.choice([0, 1]),
                            2,
                        ]
                    )
                )

        else:  # trio family or larger
            for i in range(2):
                founder_id = generate_random_id(used_ids)
                sex = i + 1
                affected = random.choices([0, 1, 2], weights=[0.05, 0.8, 0.15], k=1)[0]
                founders.append(ped_row([family_id, founder_id, '', '', sex, affected]))

            rows.extend(founders)
            # Generate remaining individuals in the family
            for _ in range(num_individuals_in_family - len(founders)):
                individual_id = generate_random_id(used_ids)
                paternal_id = random.choices(
                    ['', founders[0].individual_id], weights=[0.2, 0.8], k=1
                )[0]
                maternal_id = random.choices(
                    ['', founders[1].individual_id], weights=[0.2, 0.8], k=1
                )[0]
                sex = random.choices([0, 1, 2], weights=[0.05, 0.475, 0.475], k=1)[
                    0
                ]  # Randomly assign sex
                affected = random.choices([0, 1, 2], weights=[0.05, 0.05, 0.9], k=1)[
                    0
                ]  # Randomly assign affected status
                rows.append(
                    ped_row(
                        [
                            family_id,
                            individual_id,
                            paternal_id,
                            maternal_id,
                            sex,
                            affected,
                        ]
                    )
                )

    return rows


def generate_sequencing_type(
    count_distribution: dict[int, float], sequencing_types: list[str]
):
    """Return a random length of random sequencing types"""
    k = random.choices(
        list(count_distribution.keys()),
        list(count_distribution.values()),
    )[0]
    return random.choices(sequencing_types, weights=[0.49, 0.49, 0.02], k=k)


def generate_seq_platform(sequencing_platforms: list[str], technology: str):
    """Return a random sequencing platforms, always pacbio for long-reads, biased towards illumina for short-reads"""
    if technology == 'long-read':
        return 'pacbio'
    return random.choices(sequencing_platforms, weights=[0.90, 0.8, 0.02], k=1)[0]


def generate_seq_technology(sequencing_technologies: list[str], sequencing_type: str):
    """Return a random sequencing technology, biased towards illumina for short-reads"""
    if sequencing_type == 'genome':
        return random.choices(['short-read', 'long-read'], weights=[0.95, 0.05], k=1)[0]
    if sequencing_type in ['exome', 'transcriptome']:
        return 'short-read'
    return random.choice([t for t in sequencing_technologies if 'rna' in t])


def generate_random_number_within_distribution(count_distribution: dict[int, float]):
    """Return a random number within a distribution"""
    return random.choices(
        list(count_distribution.keys()), list(count_distribution.values())
    )[0]


async def generate_project_pedigree(project: str):
    """
    Generates a pedigree file for a project with random families and participants
    Returns the participant internal - external id map for the project
    """
    project_pedigree = generate_pedigree_rows(num_families=random.randint(1, 100))
    participant_eids = [row.individual_id for row in project_pedigree]

    pedfile = tempfile.NamedTemporaryFile(mode='w')  # noqa: SIM115
    ped_writer = csv.writer(pedfile, delimiter='\t')
    for row in project_pedigree:
        ped_writer.writerow(row)
    pedfile.flush()

    with open(pedfile.name) as f:  # noqa: PTH123
        await FamilyApi().import_pedigree_async(
            project=project, file=f, has_header=False, create_missing_participants=True
        )

    id_map = await ParticipantApi().get_participant_id_map_by_external_ids_async(
        project=project, request_body=participant_eids
    )

    return id_map


async def generate_sample_entries(
    project: str,
    participant_id_map: dict[str, int],
    metamist_enums: dict[str, dict[str, list[str]]],
    sapi: SampleApi,
):
    """
    Generates a number of samples for each participant in the input id map.
    Generates sequencing groups with random sequencing types, and then
    assays for each sequencing group.
    Combines and inserts the entries into the project through the sample API.
    """

    sample_types = metamist_enums['enum']['sampleType']

    # Arbitrary distribution for number of samples, sequencing groups, assays
    default_count_probabilities = {1: 0.78, 2: 0.16, 3: 0.05, 4: 0.01}

    samples = []
    for participant_eid, participant_id in participant_id_map.items():
        nsamples = generate_random_number_within_distribution(
            default_count_probabilities
        )
        for i in range(nsamples):
            sample = SampleUpsert(
                external_ids={PRIMARY_EXTERNAL_ORG: f'{participant_eid}_{i + 1}'},
                type=random.choice(sample_types),
                meta={
                    'collection_date': datetime.datetime.now()
                    - datetime.timedelta(minutes=random.randint(-100, 10000)),
                    'specimen': random.choice(
                        ['blood', 'phlegm', 'yellow bile', 'black bile']
                    ),
                },
                participant_id=participant_id,
                sequencing_groups=[],
            )
            samples.append(sample)

            for seq_type in generate_sequencing_type(
                default_count_probabilities, SEQ_TYPES
            ):
                facility = random.choice(
                    [
                        'Amazing sequence centre',
                        'Sequence central',
                        'Dept of Seq.',
                    ]
                )
                seq_tech = generate_seq_technology(SEQ_TECHS, seq_type)
                seq_platform = generate_seq_platform(SEQ_PLATFORMS, seq_tech)
                sg = SequencingGroupUpsert(
                    type=seq_type,
                    technology=seq_tech,
                    platform=seq_platform,
                    meta={
                        'facility': facility,
                    },
                    assays=[],
                )
                sample.sequencing_groups.append(sg)
                for _ in range(
                    generate_random_number_within_distribution(
                        default_count_probabilities
                    )
                ):
                    sg.assays.append(
                        AssayUpsert(
                            type='sequencing',
                            meta={
                                'facility': facility,
                                'reads': [],
                                'coverage': f'{random.choice([30, 90, 300, 9000, "?"])}x',
                                'sequencing_type': seq_type,
                                'sequencing_technology': seq_tech,
                                'sequencing_platform': seq_platform,
                            },
                        )
                    )

    await sapi.upsert_samples_async(project, samples)


async def generate_cram_analyses(
    project: str, project_id: int, analyses_to_insert: list[Analysis]
) -> list[dict]:
    """
    Queries the list of sequencing groups for a project and randomly selects some
    to generate CRAM analysis entries for.
    """
    logging.getLogger().setLevel(logging.WARN)
    sgid_response = await query_async(QUERY_PROJECT_SGS, {'project': project})
    logging.getLogger().setLevel(logging.INFO)
    sequencing_groups = list(sgid_response['project']['sequencingGroups'])

    # Randomly allocate some of the sequencing groups to be aligned
    aligned_sgs = random.sample(
        sequencing_groups,
        k=random.randint(int(len(sequencing_groups) / 2), len(sequencing_groups)),
    )

    # Insert completed CRAM analyses for the aligned sequencing groups, depending on their sequencing type, technology, and platform
    for sg in aligned_sgs:
        if sg['technology'] == 'short-read':
            if sg['type'] == 'genome':
                crampath = f'FAKE://{project}/cram/{sg["id"]}.cram'
            elif sg['type'] == 'exome':
                crampath = f'FAKE://{project}/exome/cram/{sg["id"]}.cram'
            elif sg['type'] == 'transcriptome':
                crampath = f'FAKE://{project}/transcriptome/cram/{sg["id"]}.cram'
            else:
                crampath = f'FAKE://{project}/crams/{sg["id"]}.cram'
        elif sg['technology'] == 'long-read':
            crampath = f'FAKE://{project}/long_read/{sg["id"]}.cram'
        else:
            crampath = f'FAKE://{project}/crams/{sg["id"]}.cram'
        analyses_to_insert.append(
            Analysis(
                sequencing_group_ids=[sg['id']],
                type='cram',
                status=AnalysisStatus('completed'),
                project=project_id,
                output=crampath,
                timestamp_completed=(
                    datetime.datetime.now()
                    - datetime.timedelta(days=random.randint(1, 15))
                ).isoformat(),
                meta={
                    'sequencing_type': sg['type'],
                    'sequencing_technology': sg['technology'],
                    'sequencing_platform': sg['platform'],
                    # random size between 5, 25 GB
                    'size': random.randint(5 * 1024, 25 * 1024) * 1024 * 1024,
                },
            )
        )

    return aligned_sgs


async def generate_cohorts(
    project: str,
    aligned_sequencing_groups: list[dict],
    cohort_api: CohortApi,
) -> dict[tuple[str, str], str]:
    """
    Generates "Custom cohorts" for the input project and its aligned sequencing groups, with generated names.
    Uses these cohort IDs when creating multi-SG analyses, like the QC and es-index analyses.

    Returns a dict mapping sequencing type and technology combinations to cohort IDs (platform is ignored here).
    """
    # First lists of sequencing groups by type and technology
    sgs_by_type_tech: dict[tuple[str, str], list[str]] = {}
    for sg in aligned_sequencing_groups:
        key = (sg['type'], sg['technology'])
        if key not in sgs_by_type_tech:
            sgs_by_type_tech[key] = []
        sgs_by_type_tech[key].append(sg['id'])

    # Next, create cohorts for each combination of sequencing type and technology
    cohort_ids_by_type_tech: dict[tuple[str, str], str] = {}
    for (seq_type, seq_tech), sg_ids in sgs_by_type_tech.items():
        cohort_criteria = CohortCriteria(
            sg_ids_internal=sg_ids,
        )
        cohort_body = CohortBody(
            name=f'{project} {seq_type} {seq_tech} {datetime.date.today().isoformat()}',
            description=f'{seq_type} and {seq_tech} only',
        )
        cohort = cohort_api.create_cohort_from_criteria(
            project=project,
            body_create_cohort_from_criteria=BodyCreateCohortFromCriteria(
                cohort_spec=cohort_body,
                cohort_criteria=cohort_criteria,
            ),
        )
        cohort_ids_by_type_tech[(seq_type, seq_tech)] = cohort['cohort_id']

    return cohort_ids_by_type_tech


async def generate_qc_analyses(
    project: str,
    cohort_ids_by_type_tech: dict[tuple[str, str], str],
    analyses_to_insert: list[Analysis],
):
    """
    Generates multi-SG analyses of type QC for the input project and cohorts, with random QC metrics in the meta field.
    Created QC analyses include all SGs in the given cohorts, with results from stages `CramMultiQC` and `SomalierPedigree`.
    """
    for (seq_type, seq_tech), cohort_id in cohort_ids_by_type_tech.items():
        if seq_tech == 'long-read':
            continue
        if seq_type in ['exome', 'genome']:
            analyses_to_insert.append(
                Analysis(
                    cohort_ids=[cohort_id],
                    type='qc',
                    status=AnalysisStatus('completed'),
                    output=f'FAKE::{project}-{seq_type}-qc-{datetime.date.today()}.json',
                    meta={'stage': 'CramMultiQC', 'sequencing_type': seq_type},
                )
            )
            analyses_to_insert.append(
                Analysis(
                    cohort_ids=[cohort_id],
                    type='web',
                    status=AnalysisStatus('completed'),
                    output=f'FAKE::{project}-{seq_type}-web/somalierpedigree-{datetime.date.today()}.html',
                    meta={'stage': 'SomalierPedigree', 'sequencing_type': seq_type},
                )
            )


async def generate_seqr_loader_analyses(
    project: str,
    cohort_ids_by_type_tech: dict[tuple[str, str], str],
    analyses_to_insert: list[Analysis],
):
    """
    Generates matrixtable analyses from the AnnotateDataset stage, and ES-index analyses
    from the MtToEs stage, using the generated cohorts for the input project.

    Extends the `analyses_to_insert` list with the new analyses, which will be inserted
    in bulk at the end of the main function.
    """
    for (seq_type, seq_tech), cohort_id in cohort_ids_by_type_tech.items():
        if seq_tech == 'long-read':
            continue
        if seq_type in ['exome', 'genome']:
            analyses_to_insert.extend(
                [
                    Analysis(
                        cohort_ids=[cohort_id],
                        type='matrixtable',
                        status=AnalysisStatus('completed'),
                        output=f'FAKE::{project}-{seq_type}-{datetime.date.today()}.mt',
                        meta={'stage': 'AnnotateDataset', 'sequencing_type': seq_type},
                    ),
                    Analysis(
                        cohort_ids=[cohort_id],
                        type='es-index',
                        status=AnalysisStatus('completed'),
                        output=f'FAKE::{project}-{seq_type}-es-{datetime.date.today()}.done',
                        meta={'stage': 'MtToEs', 'sequencing_type': seq_type},
                    ),
                ]
            )
            # Insert an SV es-index for genomes, and a GCNV es-index for exomes
            if seq_type == 'genome':
                analyses_to_insert.extend(
                    [
                        Analysis(
                            cohort_ids=[cohort_id],
                            type='es-index',
                            status=AnalysisStatus('completed'),
                            output=f'FAKE::{project}-{seq_type}-sv-{datetime.date.today()}.done',
                            meta={'stage': 'MtToEsSv', 'sequencing_type': seq_type},
                        ),
                    ]
                )
            elif seq_type == 'exome':
                analyses_to_insert.extend(
                    [
                        Analysis(
                            cohort_ids=[cohort_id],
                            type='es-index',
                            status=AnalysisStatus('completed'),
                            output=f'FAKE::{project}-{seq_type}-gcnv-{datetime.date.today()}.done',
                            meta={'stage': 'MtToEsCNV', 'sequencing_type': seq_type},
                        ),
                    ]
                )


async def generate_web_report_analyses(
    project: str,
    project_id: int,
    aligned_sequencing_groups: list[dict],
    analyses_to_insert: list[Analysis],
):
    """
    Queries the list of sequencing groups for a project and generates web analysis (STRipy
    and MITO report) entries for those with completed a CRAM analysis.
    Stripy analyses have a random chance of having outliers detected, and a random number
    of loci flagged as outliers.
    """

    def get_stripy_outliers():
        """
        Generate a the outliers_detected bool, and then the outlier_loci dict
        """
        outlier_loci = {}
        outliers_detected = random.choice([True, False])
        if outliers_detected:
            for loci in random.sample(LOCI, k=random.randint(1, len(LOCI))):
                outlier_loci[loci] = random.choice(['1', '2', '3'])

        return {'outliers_detected': outliers_detected, 'outlier_loci': outlier_loci}

    # Insert a completed web analysis for MitoReport and STRipy reports for each of the aligned sequencing groups
    for sg in aligned_sequencing_groups:
        stripy_outliers = get_stripy_outliers()
        analyses_to_insert.extend(
            [
                Analysis(
                    sequencing_group_ids=[sg['id']],
                    type='web',
                    status=AnalysisStatus('completed'),
                    project=project_id,
                    output=f'FAKE://{project}/stripy/{sg["id"]}.stripy.html',
                    timestamp_completed=(
                        datetime.datetime.now()
                        - datetime.timedelta(days=random.randint(1, 15))
                    ).isoformat(),
                    meta={
                        'stage': 'Stripy',
                        'sequencing_type': sg['type'],
                        # random size between 0.5, 2.5 MB
                        'size': random.randint(512, 2560) * 1024,
                        'outliers_detected': stripy_outliers['outliers_detected'],
                        'outlier_loci': stripy_outliers['outlier_loci'],
                    },
                ),
                Analysis(
                    sequencing_group_ids=[sg['id']],
                    type='web',
                    status=AnalysisStatus('completed'),
                    project=project_id,
                    output=f'FAKE://{project}/mito/mitoreport-{sg["id"]}/index.html',
                    timestamp_completed=(
                        datetime.datetime.now()
                        - datetime.timedelta(days=random.randint(1, 15))
                    ).isoformat(),
                    meta={
                        'stage': 'MitoReport',
                        'sequencing_type': sg['type'],
                        # random size between 0.5, 2.5 MB
                        'size': random.randint(512, 2560) * 1024,
                    },
                ),
            ]
        )
    # Also insert a web report for the STRipy index for the whole project
    analyses_to_insert.append(
        Analysis(
            sequencing_group_ids=[sg['id'] for sg in aligned_sequencing_groups],
            project=project_id,
            type='web',
            status=AnalysisStatus('completed'),
            output=f'FAKE://{project}/stripy/index.html',
            timestamp_completed=datetime.datetime.now().isoformat(),
            meta={
                'stage': 'MakeIndexPage',
                'sequencing_type': 'genome',
                # random size between 5, 50 MB
                'size': random.randint(5 * 1024, 50 * 1024) * 1024,
            },
        )
    )


async def main():
    """
    Generates a number of projects and populates them with randomly generated pedigrees.
    Sets each project as a seqr project via the project's meta field.
    Creates family, participant, sample, and sequencing group records for the projects.
    Upserts a number of analyses for each project to test seqr related endpoints.
    The upserted analyses include CRAMs, joint-called AnnotateDatasets, and ES-indexes.
    """
    aapi = AnalysisApi()
    papi = ProjectApi()
    sapi = SampleApi()
    logging.getLogger().setLevel(logging.WARN)
    metamist_enums: dict[str, dict[str, list[str]]] = await query_async(QUERY_ENUMS)
    logging.getLogger().setLevel(logging.INFO)
    await EnumsApi().post_analysis_types_async('matrixtable')

    existing_projects = await papi.get_my_projects_async()
    for project in PROJECTS:
        analyses_to_insert: list[Analysis] = []
        if project not in existing_projects:
            await papi.create_project_async(
                name=project, dataset=project, create_test_project=False
            )

            default_user = os.getenv('SM_LOCALONLY_DEFAULTUSER')
            if not default_user:
                print(
                    'SM_LOCALONLY_DEFAULTUSER env var is not set, please set it before generating data'
                )
                sys.exit(1)

            await papi.update_project_members_async(
                project=project,
                project_member_update=[
                    {'member': default_user, 'roles': ['reader', 'writer']}
                ],
            )

            default_user = os.getenv('SM_LOCALONLY_DEFAULTUSER')
            if not default_user:
                print(
                    'SM_LOCALONLY_DEFAULTUSER env var is not set, please set it before generating data'
                )
                sys.exit(1)

            await papi.update_project_members_async(
                project=project,
                project_member_update=[
                    {'member': default_user, 'roles': ['reader', 'writer']}
                ],
            )

            logging.info(f'Created project "{project}"')
            await papi.update_project_async(
                project=project,
                body={'meta': {'is_seqr': 'true'}},
            )
            logging.info(f'Set {project} as seqr project')

        project_id_query_result = await query_async(
            QUERY_PROJECT_ID, {'project': project}
        )
        project_id = project_id_query_result['project']['id']

        participant_id_map = await generate_project_pedigree(project)

        await generate_sample_entries(project, participant_id_map, metamist_enums, sapi)

        aligned_sgs = await generate_cram_analyses(
            project, project_id, analyses_to_insert
        )
        cohort_ids_by_type_tech = await generate_cohorts(
            project, aligned_sgs, CohortApi()
        )

        await generate_web_report_analyses(
            project, project_id, aligned_sgs, analyses_to_insert
        )

        await generate_seqr_loader_analyses(
            project, cohort_ids_by_type_tech, analyses_to_insert
        )

        for analyses in chunk(analyses_to_insert, 50):
            logging.info(f'Inserting {len(analyses)} analysis entries into {project}')
            await asyncio.gather(
                *[aapi.create_analysis_async(project, a) for a in analyses]
            )


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stderr,
    )
    logging.getLogger().setLevel(logging.INFO)
    asyncio.new_event_loop().run_until_complete(main())
