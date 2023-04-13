#!/usr/bin/env python3
# pylint: disable=too-many-locals
import asyncio
from pprint import pprint
import random
import argparse
import datetime

from metamist.api.analysis_api import AnalysisApi

from metamist.api.enums_api import EnumsApi
from metamist.model.analysis_status import AnalysisStatus
from metamist.model.analysis import Analysis

from metamist.models import (
    SampleUpsert,
    AssayUpsert,
    SequencingGroupUpsert,
)

from metamist.apis import (
    ProjectApi,
    FamilyApi,
    ParticipantApi,
    SampleApi,
    SequencingGroupApi,
)

# from metamist.configuration import m
from metamist.parser.generic_parser import chunk

EMOJIS = [':)', ':(', ':/', ':\'(']


async def main(ped_path='greek-myth-forgeneration.ped', project='greek-myth'):
    """Doing the generation for you"""

    sapi = SampleApi()

    papi = ProjectApi()
    enum_api = EnumsApi()
    sgapi = SequencingGroupApi()

    sample_types = enum_api.get_sample_types()
    sequencing_technologies = enum_api.get_sequencing_technologys()
    sequencing_platforms = enum_api.get_sequencing_platforms()
    sequencing_types = enum_api.get_sequencing_types()
    assay_type = 'sequencing'

    existing_projects = await papi.get_my_projects_async()
    if project not in existing_projects:
        await papi.create_project_async(
            name=project, dataset=project, create_test_project=False
        )

    with open(ped_path, encoding='utf-8') as f:
        # skip the first line
        _ = f.readline()
        participant_eids = [line.split('\t')[1] for line in f]

    with open(ped_path) as f:
        await FamilyApi().import_pedigree_async(
            project=project, file=f, has_header=True, create_missing_participants=True
        )

    id_map = await ParticipantApi().get_participant_id_map_by_external_ids_async(
        project=project, request_body=participant_eids
    )

    how_many_samples = {1: 0.78, 2: 0.16, 3: 0.05, 4: 0.01}

    def generate_random_number_within_distribution():
        return random.choices(
            list(how_many_samples.keys()), list(how_many_samples.values())
        )[0]

    samples = []
    sample_id_index = 1003

    for participant_eid in participant_eids:
        pid = id_map[participant_eid]

        nsamples = generate_random_number_within_distribution()
        for _ in range(nsamples):
            sample = SampleUpsert(
                external_id=f'GRK{sample_id_index}',
                type=random.choice(sample_types),
                meta={
                    'collection_date': datetime.datetime.now()
                    - datetime.timedelta(minutes=random.randint(-100, 10000)),
                    'specimen': random.choice(
                        ['blood', 'phlegm', 'yellow bile', 'black bile']
                    ),
                },
                participant_id=pid,
                sequences=[],
            )

            sample_id_index += random.randint(1, 4)
            for _ in range(generate_random_number_within_distribution()):
                facility = random.choice(
                    [
                        'Amazing sequence centre',
                        'Sequence central',
                        'Dept of Seq.',
                    ]
                )
                sg = SequencingGroupUpsert(
                    type=random.choice(sequencing_types),
                    technology=random.choice(sequencing_technologies),
                    platform=random.choice(sequencing_platforms),
                    meta={
                        'facility': facility,
                    },
                )
                sg.assays = []
                for _ in range(generate_random_number_within_distribution()):
                    sg.assays.append(
                        AssayUpsert(
                            type=assay_type,
                            meta={
                                'facility': facility,
                                'emoji': random.choice(EMOJIS),
                                'coverage': f'{random.choice([30, 90, 300, 9000, "?"])}x',
                            },
                        )
                    )

            samples.append(sample)

    response = await sapi.upsert_samples_async(project, samples)
    pprint(response)

    sample_id_map = await sgapi.get_all_sequencing_group_ids_by_sample_by_type_async(
        project=project
    )
    sequencing_group_ids = [sg for sgs in sample_id_map.values() for sg in sgs]

    analyses_to_insert = [
        Analysis(
            sequencing_group_ids=[s],
            type='cram',
            status=AnalysisStatus('completed'),
            output=f'FAKE://greek-myth/crams/{s}.cram',
            meta={'sequencing_type': 'genome'},
        )
        for s in sequencing_group_ids
    ]

    # es-index
    analyses_to_insert.append(
        Analysis(
            sequencing_group_ids=random.sample(
                sequencing_group_ids, len(sequencing_group_ids) // 2
            ),
            type='es-index',
            status=AnalysisStatus('completed'),
            output=f'FAKE::greek-myth-genome-{datetime.date.today()}',
            meta={},
        )
    )

    aapi = AnalysisApi()
    for ans in chunk(analyses_to_insert, 50):
        print(f'Inserting {len(ans)} analysis entries')
        await asyncio.gather(
            *[aapi.create_analysis_async(project, analysis_model=a) for a in ans]
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Script for generating data in the greek-myth project'
    )
    parser.add_argument(
        '--ped-path',
        type=str,
        default='greek-myth-forgeneration.ped',
        help='Path to the pedigree file',
    )
    parser.add_argument('--project', type=str, default='greek-myth')
    args = vars(parser.parse_args())
    asyncio.new_event_loop().run_until_complete(main(**args))
