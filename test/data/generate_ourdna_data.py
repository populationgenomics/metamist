#!/usr/bin/env python3
"""
This is a simple script to generate some participants and samples for testing ourdna
Local Backend API needs to run prior executing this script

"""

import argparse
import datetime
import os
import random
import sys
import uuid
from collections.abc import Sequence

from metamist.apis import AnalysisApi, EnumsApi, ParticipantApi, ProjectApi, SampleApi
from metamist.graphql import gql, query
from metamist.model.analysis import Analysis
from metamist.model.analysis_status import AnalysisStatus
from metamist.models import (
    AssayUpsert,
    ParticipantUpsert,
    SampleUpsert,
    SequencingGroupUpsert,
)


SG_TYPE_GENOME = 'genome'
SG_TYPE_ARRAY = 'genotypingarray'

SG_DEFINITIONS = {
    SG_TYPE_GENOME: {
        'platform': 'illumina',
        'technology': 'short-read',
        'analyses': ['cram', 'gvcf'],
    },
    SG_TYPE_ARRAY: {
        'platform': 'illumina',
        'technology': 'infinium-global-diversity-array-v1.0',
        'analyses': ['genotypingarray_gtc'],
    },
}

ANALYSIS_PATH_TEMPLATES = {
    'cram': 'FAKE://cpg-ourdna-main/cram/{sg_id}.cram',
    'gvcf': 'FAKE://cpg-ourdna-main/gvcf/{sg_id}.g.vcf.gz',
    'genotypingarray_gtc': 'FAKE://cpg-ourdna-main/gtc/{sg_id}.gtc',
}

SG_QUERY = gql(
    """
    query SGsForOurdnaTestData($project: String!) {
        project(name: $project) {
            sequencingGroups {
                id
                type
                analyses(project: {eq: $project}) {
                    id
                    type
                }
            }
        }
    }
    """
)

WHOLE_BLOOD_WITHOUT_SGS_QUERY = gql(
    """
    query WholeBloodWithoutSGs($project: String!) {
        project(name: $project) {
            samples(type: {eq: "whole-blood"}) {
                id
                externalId
                sequencingGroups { id }
            }
        }
    }
    """
)


PRIMARY_EXTERNAL_ORG = ''


ANCESTRIES = [
    'Vietnamese',
    'Filipino',
    'Australian',
    'Spanish',
    'Acehnese',
    'Afghan',
    'African American',
    'American',
    'Amhara',
    'British',
    'Chinese',
    'English',
    'German',
    'Greek',
    'Indian',
    'Irish',
    'Italian',
    'Japanese',
    'Malay',
    'Norwegian',
    'Scottish',
    'Venezuelan',
]

BIRTHPLACES = [
    'Philippines',
    'Vietnam',
    'Cambodia',
    'Australia',
    "I don't know",
    "I'd prefer not to say",
    'Thailand',
]

LANGUAGES = [
    'Vietnamese',
    'Filipino',
    'Tagalog',
    'Cebuano',
    'English',
    'Bisaya',
    'Ilonggo (Hiligaynon)',
    'Cantonese',
    'Other Southern Asian Languages',
    'Spanish',
    'Ilokano',
    'Bikol',
    'American Languages',
    'IIokano',
    'Hawaiian English',
    'Armenian',
    'Khmer',
    'Acehnese',
    'Other Southeast Asian Languages',
    'Urdu',
    'French',
    'Japanese',
    'Thai',
    'Italian',
    'Croatian',
    'Chin Haka',
    'Arabic',
]


event_type = ['one-stop-shop', 'walk-in']
processing_site = ['bbv', 'westmead']


def random_dates(
    start_between: tuple[datetime.datetime, datetime.datetime],
    rough_gaps: list[datetime.timedelta],
):
    """Generate a list of random dates in order"""
    # random datetime between specificed datetimes
    start_date = start_between[0] + datetime.timedelta(
        seconds=random.randint(
            0, int((start_between[1] - start_between[0]).total_seconds())
        )
    )

    dates: list[datetime.datetime] = [start_date]

    for rough_gap in rough_gaps:
        rand_change = (random.random() * 1.5) + 0.5
        gap = rough_gap * rand_change
        next_date = dates[-1] + gap
        dates.append(next_date)

    return [date.strftime('%Y-%m-%dT%H:%M:%S') for date in dates]


def random_choice(choices: Sequence[str | bool | int], weight_by_index: bool = False):
    """Pick a random choice from a list of choices"""
    weighted_choices = list(choices)
    if weight_by_index:
        for i, choice in enumerate(choices):
            weighted_choices.extend([choice] * (len(choices) - i))
    return weighted_choices[random.randint(0, len(weighted_choices) - 1)]


def random_list(
    choices: Sequence[str | bool | int],
    weight_by_index: bool = False,
    min_len: int = 1,
    max_len: int = 5,
):
    """Generate a random list of choices"""
    result: list[str | bool | int] = []
    desired_len = random.randint(min_len, max_len)
    if desired_len > len(choices):
        raise ValueError(
            f'Desired length {desired_len} is greater than the number of choices {len(choices)}'
        )
    while len(result) < desired_len:
        choice = random_choice(choices, weight_by_index)
        if choice not in result:
            result.append(choice)

    return result


def make_sequencing_groups(root_external_id: str) -> list[SequencingGroupUpsert]:
    """Build the two SGs that hang off a whole-blood sample.

    Every whole-blood sample gets both a genome SG (one R1+R2 fastq assay) and a
    genotypingarray SG (no assays). This mirrors the prod ourdna 'multi-SG-type'
    arrangement and exercises the SG matching code in create_test_subset.py.
    """
    upload_prefix = f'FAKE://cpg-ourdna-main-upload/{root_external_id}'

    genome_assay = AssayUpsert(
        type='sequencing',
        meta={
            'reads_type': 'fastq',
            'reads': [
                {
                    'location': f'{upload_prefix}_R1.fastq.gz',
                    'basename': f'{root_external_id}_R1.fastq.gz',
                    'class': 'File',
                },
                {
                    'location': f'{upload_prefix}_R2.fastq.gz',
                    'basename': f'{root_external_id}_R2.fastq.gz',
                    'class': 'File',
                },
            ],
            'sequencing_type': SG_TYPE_GENOME,
            'sequencing_technology': SG_DEFINITIONS[SG_TYPE_GENOME]['technology'],
            'sequencing_platform': SG_DEFINITIONS[SG_TYPE_GENOME]['platform'],
        },
    )

    return [
        SequencingGroupUpsert(
            type=SG_TYPE_GENOME,
            platform=SG_DEFINITIONS[SG_TYPE_GENOME]['platform'],
            technology=SG_DEFINITIONS[SG_TYPE_GENOME]['technology'],
            meta={},
            assays=[genome_assay],
        ),
        SequencingGroupUpsert(
            type=SG_TYPE_ARRAY,
            platform=SG_DEFINITIONS[SG_TYPE_ARRAY]['platform'],
            technology=SG_DEFINITIONS[SG_TYPE_ARRAY]['technology'],
            meta={},
            assays=[],
        ),
    ]


def create_samples():
    """Create a sample with nested samples"""

    processing_times = random_dates(
        start_between=(
            # 1 year ago and now
            datetime.datetime.now() - datetime.timedelta(days=365),
            datetime.datetime.now(),
        ),
        rough_gaps=[
            datetime.timedelta(hours=36),  # collection to processing received
            datetime.timedelta(hours=24),  # processing received to processing start
            datetime.timedelta(hours=8),  # processing start to processing end
        ],
    )

    collection_time = processing_times[0]
    processing_received_time = processing_times[1]
    processing_start_time = processing_times[2]
    processing_end_time = processing_times[3]

    sm_processing_site = random_choice(processing_site)

    root_meta = {
        'collection_lab': random_choice(['Sonic']),
        'collection_datetime': collection_time,
        'collection_courier': random_choice(['Toll', 'StarTrack']),
        'processing_received_datetime': processing_received_time,
        'processing_site': sm_processing_site,
        'collection_event_type': random_choice(event_type),
        'courier_tracking_number': str(uuid.uuid4()),
        'container_count': random.randint(1, 5),
        'container_volume_total': str(random.randint(1, 5) * 10),
        'container_volume_unit': 'mL',
    }

    root_external_id = str(uuid.uuid4())

    sample = SampleUpsert(
        external_ids={PRIMARY_EXTERNAL_ORG: root_external_id},
        type='blood',
        active=True,
        nested_samples=[
            SampleUpsert(
                external_ids={
                    PRIMARY_EXTERNAL_ORG: f'{root_external_id}-whole-blood',
                    'sonic': str(uuid.uuid4()),
                },
                type='whole-blood',
                active=True,
                meta={
                    'volume_per_aliquot': str(random.randint(200, 1000)),
                    'aliquot_count': random.randint(1, 5),
                    'aliquot_barcodes': [
                        str(uuid.uuid4()) for _ in range(random.randint(1, 5))
                    ],
                    'volume_unit': 'ul',
                    'processing_start_datetime': processing_start_time,
                    'processing_end_datetime': processing_end_time,
                    'processing_sop_version': f'WIMR v.{random.randint(1, 3)}.{random.randint(0, 9)}',
                    'processing_site': sm_processing_site,
                },
            ),
            SampleUpsert(
                external_ids={PRIMARY_EXTERNAL_ORG: f'{root_external_id}-guthrie-card'},
                type='guthrie-card',
                active=True,
                meta={
                    'processing_start_datetime': processing_start_time,
                    'processing_end_datetime': processing_end_time,
                    'processing_sop_version': f'WIMR v.{random.randint(1, 3)}.{random.randint(0, 9)}',
                    'processing_site': sm_processing_site,
                    'parent_inventory_code': f'{root_external_id}-whole-blood',
                    'spot_quantity': random.randint(20, 30),
                },
            ),
            SampleUpsert(
                external_ids={PRIMARY_EXTERNAL_ORG: f'{root_external_id}-plasma'},
                type='plasma',
                active=True,
                meta={
                    'volume_per_aliquot': str(random.randint(200, 1000)),
                    'parent_inventory_code': f'{root_external_id}-whole-blood',
                    'aliquot_count': random.randint(1, 5),
                    'aliquot_barcodes': [
                        str(uuid.uuid4()) for _ in range(random.randint(1, 5))
                    ],
                    'volume_unit': 'ul',
                    'processing_start_datetime': processing_start_time,
                    'processing_end_datetime': processing_end_time,
                    'processing_sop_version': f'WIMR v.{random.randint(1, 3)}.{random.randint(0, 9)}',
                    'processing_site': sm_processing_site,
                },
            ),
            SampleUpsert(
                external_ids={PRIMARY_EXTERNAL_ORG: f'{root_external_id}-buffy-coat'},
                type='buffy-coat',
                active=True,
                meta={
                    'volume_per_aliquot': str(random.randint(200, 1000)),
                    'parent_inventory_code': f'{root_external_id}-whole-blood',
                    'aliquot_count': random.randint(1, 5),
                    'aliquot_barcodes': [
                        str(uuid.uuid4()) for _ in range(random.randint(1, 5))
                    ],
                    'volume_unit': 'ul',
                    'processing_start_datetime': processing_start_time,
                    'processing_end_datetime': processing_end_time,
                    'processing_sop_version': f'WIMR v.{random.randint(1, 3)}.{random.randint(0, 9)}',
                    'processing_site': sm_processing_site,
                },
            ),
            SampleUpsert(
                external_ids={PRIMARY_EXTERNAL_ORG: f'{root_external_id}-pbmc'},
                type='pbmc',
                active=True,
                meta={
                    'volume_per_aliquot': str(random.randint(200, 1000)),
                    'parent_inventory_code': f'{root_external_id}-whole-blood',
                    'aliquot_count': random.randint(1, 5),
                    'aliquot_barcodes': [
                        str(uuid.uuid4()) for _ in range(random.randint(1, 5))
                    ],
                    'volume_unit': 'ul',
                    'processing_start_datetime': processing_start_time,
                    'processing_end_datetime': processing_end_time,
                    'processing_sop_version': f'WIMR v.{random.randint(1, 3)}.{random.randint(0, 9)}',
                    'processing_site': sm_processing_site,
                    'percent_viability': random.uniform(80, 100),
                    'total_viable_cells': random.uniform(20, 60),
                    'viable_cells_per_aliquot': '4M',
                },
            ),
        ],
        meta=root_meta,
    )

    return sample


def create_participant():
    """Create a participant with nested samples"""
    birth_year = random.randint(1900, 2010)
    reported_sex = random_choice([1, 2])

    weguide_id = str(uuid.uuid4())
    external_ids = {
        PRIMARY_EXTERNAL_ORG: weguide_id,
        'weguide': f'weguide_{weguide_id}',
    }

    if random.random() < 0.3:  # noqa: PLR2004
        external_ids['sano'] = f'sano_{str(uuid.uuid4())}'

    participant = ParticipantUpsert(
        external_ids=external_ids,
        reported_sex=reported_sex,
        meta={
            'ancestry_participant_ancestry': random_list(
                ANCESTRIES, weight_by_index=True, min_len=1, max_len=2
            ),
            'ancestry_mother_ancestry': random_list(
                ANCESTRIES, weight_by_index=True, min_len=1, max_len=2
            ),
            'ancestry_father_ancestry': random_list(
                ANCESTRIES, weight_by_index=True, min_len=1, max_len=2
            ),
            'ancestry_mother_birthplace': random_list(
                BIRTHPLACES, weight_by_index=True, min_len=1, max_len=2
            ),
            'ancestry_father_birthplace': random_list(
                BIRTHPLACES, weight_by_index=True, min_len=1, max_len=2
            ),
            'ancestry_language_other_than_english': random_list(
                LANGUAGES, weight_by_index=True, min_len=1, max_len=2
            ),
            'birth_year': birth_year,
            'participant_portal_birth_year': birth_year,
            'participant_portal_reported_sex': reported_sex,
            'processing_site_birth_year': birth_year,
            'consent_blood_consent': random_choice(['yes', 'no']),
            'consent_informed_consent': random_choice(['yes', 'no']),
            'choice_receive_genetic_info': random_choice(['yes', 'no']),
            'choice_family_receive_genetic_info': random_choice(['yes', 'no']),
            'choice_recontact': random_choice(['yes', 'no']),
            'choice_general_updates': random_choice(['yes', 'no']),
            'choice_use_of_cells_in_future_research_consent': random_choice(
                ['yes', 'no']
            ),
            'event_recorded_sonic_id': str(uuid.uuid4()),
            'have_donated_either_blood_or_plasma': random_choice(['yes', 'no']),
            'choice_data_linkage': random_choice(['yes', 'no']),
            'choice_use_of_cells_in_future_research_understanding': random_list(
                [
                    'grown_indefinitely',
                    'used_by_approved_researchers',
                ],
                min_len=1,
                max_len=2,
            ),
        },
        samples=[create_samples()],
    )

    return participant


def register_enums(enums_api: EnumsApi) -> None:
    """Register the sample/SG/assay/analysis enum values this generator needs."""
    sample_types = [
        'blood',
        'whole-blood',
        'guthrie-card',
        'plasma',
        'pbmc',
        'buffy-coat',
    ]
    for typ in sample_types:
        enums_api.post_sample_types(new_type=typ)

    for typ in (SG_TYPE_GENOME, SG_TYPE_ARRAY):
        enums_api.post_sequencing_types(new_type=typ)

    for plat in {defn['platform'] for defn in SG_DEFINITIONS.values()}:
        enums_api.post_sequencing_platforms(new_type=plat)

    for tech in {defn['technology'] for defn in SG_DEFINITIONS.values()}:
        enums_api.post_sequencing_technologys(new_type=tech)

    enums_api.post_assay_types(new_type='sequencing')

    for analysis_type in {a for defn in SG_DEFINITIONS.values() for a in defn['analyses']}:
        enums_api.post_analysis_types(new_type=analysis_type)


def attach_sgs_to_whole_blood_samples(sample_api: SampleApi, project: str) -> None:
    """Find every whole-blood sample without SGs and attach the two-SG set.

    The participant upsert path only collects SGs from top-level samples, so
    SGs declared on nested whole-blood samples are silently dropped. Run this
    as a second pass against the freshly-created whole-blood sample IDs.
    """
    resp = query(WHOLE_BLOOD_WITHOUT_SGS_QUERY, {'project': project})
    samples = [
        s for s in resp['project']['samples'] if not s.get('sequencingGroups')
    ]
    if not samples:
        return

    print(f'attaching SGs to {len(samples)} whole-blood samples')
    upserts = [
        SampleUpsert(
            id=s['id'],
            sequencing_groups=make_sequencing_groups(s['externalId']),
        )
        for s in samples
    ]
    sample_api.upsert_samples(project, upserts)


def create_analyses_for_new_sgs(
    analysis_api: AnalysisApi, project: str, existing_sg_ids: set[str]
) -> None:
    """Create one analysis per analysis type for every SG newly added to the project.

    Genome SGs get a cram + gvcf, array SGs get a genotypingarray_gtc. Paths use
    a FAKE:// scheme so metamist skips GCS validation and stores the path as a
    plain string in analysis_outputs — Harper's PR exercises the SG-matching
    code regardless of whether outputs comes back as a string or a dict.
    """
    sg_resp = query(SG_QUERY, {'project': project})
    new_sgs = [
        sg
        for sg in sg_resp['project']['sequencingGroups']
        if sg['id'] not in existing_sg_ids
    ]
    print(f'creating analyses for {len(new_sgs)} new sequencing groups')

    for sg in new_sgs:
        sg_id = sg['id']
        sg_type = sg['type']
        existing_analysis_types = {a['type'] for a in sg.get('analyses') or []}

        for analysis_type in SG_DEFINITIONS[sg_type]['analyses']:
            if analysis_type in existing_analysis_types:
                continue
            path = ANALYSIS_PATH_TEMPLATES[analysis_type].format(sg_id=sg_id)
            analysis_api.create_analysis(
                project=project,
                analysis=Analysis(
                    type=analysis_type,
                    status=AnalysisStatus('completed'),
                    sequencing_group_ids=[sg_id],
                    output=path,
                    meta={'sequencing_type': sg_type},
                ),
            )


def main(project='ourdna', num_participants=5):
    """Doing the generation for you"""
    project_api = ProjectApi()
    participant_api = ParticipantApi()
    sample_api = SampleApi()
    enums_api = EnumsApi()
    analysis_api = AnalysisApi()

    register_enums(enums_api)

    # Create the project if it doesn't exist
    existing_projects = project_api.get_my_projects()
    if project not in existing_projects:
        project_api.create_project(
            name=project, dataset=project, create_test_project=False
        )
        default_user = os.getenv('SM_LOCALONLY_DEFAULTUSER')
        if not default_user:
            print(
                'SM_LOCALONLY_DEFAULTUSER env var is not set, please set it before generating data'
            )
            sys.exit(1)

        project_api.update_project_members(
            project=project,
            project_member_update=[
                {'member': default_user, 'roles': ['reader', 'writer']}
            ],
        )

    # Snapshot existing SGs so we only create analyses for fresh ones this run.
    pre_sgs = query(SG_QUERY, {'project': project})
    existing_sg_ids = {sg['id'] for sg in pre_sgs['project']['sequencingGroups']}

    participants = [create_participant() for _ in range(num_participants)]
    participants_rec = participant_api.upsert_participants(project, participants)
    print(f'inserted {len(participants_rec)} participants')

    attach_sgs_to_whole_blood_samples(sample_api, project)
    create_analyses_for_new_sgs(analysis_api, project, existing_sg_ids)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Script for generating data in the ourdna test project'
    )
    parser.add_argument('--project', type=str, default='ourdna')
    parser.add_argument('--num-participants', type=int, default=5)
    args = vars(parser.parse_args())
    main(**args)
