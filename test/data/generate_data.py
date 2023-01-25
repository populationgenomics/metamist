#!/usr/bin/env python3
import asyncio
from pprint import pprint
import random
import argparse
import datetime

from sample_metadata.api.analysis_api import AnalysisApi

from sample_metadata.model.analysis_status import AnalysisStatus

from sample_metadata.model.analysis_type import AnalysisType

from sample_metadata.model.analysis_model import AnalysisModel

from sample_metadata.models import (
    SampleType,
    SequenceStatus,
    SampleBatchUpsertBody,
    SampleBatchUpsert,
    SequenceUpsert,
    SequenceType,
    SequenceTechnology,
)

from sample_metadata.apis import (
    ProjectApi,
    FamilyApi,
    ParticipantApi,
    SampleApi,
)

# from sample_metadata.configuration import m
from sample_metadata.parser.generic_parser import chunk

EMOJIS = [':)', ':(', ':/', ':\'(']


async def main(ped_path='greek-myth-forgeneration.ped', project='greek-myth'):
    """Doing the generation for you"""

    sapi = SampleApi()

    papi = ProjectApi()
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
            sample = SampleBatchUpsert(
                external_id=f'GRK{sample_id_index}',
                type=SampleType('blood'),
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

            sequences = []
            sample_id_index += random.randint(1, 4)
            for _ in range(generate_random_number_within_distribution()):
                sequences.append(
                    SequenceUpsert(
                        status=SequenceStatus('uploaded'),
                        type=SequenceType(
                            random.choice(
                                list(
                                    next(
                                        iter(SequenceType.allowed_values.values())
                                    ).values()
                                )
                            )
                        ),
                        technology=SequenceTechnology(
                            random.choice(
                                list(
                                    next(
                                        iter(SequenceTechnology.allowed_values.values())
                                    ).values()
                                )
                            )
                        ),
                        meta={
                            'facility': random.choice(
                                [
                                    'Amazing sequence centre',
                                    'Sequence central',
                                    'Dept of Seq.',
                                ]
                            ),
                            'emoji': random.choice(EMOJIS),
                            'technology': random.choice(
                                ['magnifying glass', 'guessing', 'math.random']
                            ),
                            'coverage': f'{random.choice([30, 90, 300, 9000, "?"])}x',
                        },
                    )
                )

            sample.sequences = sequences
            samples.append(sample)

    batch_samples = SampleBatchUpsertBody(samples=samples)
    response = await sapi.batch_upsert_samples_async(project, batch_samples)
    pprint(response)

    sample_id_map = await sapi.get_all_sample_id_map_by_internal_async(project=project)
    sample_ids = list(sample_id_map.keys())

    analyses_to_insert = [
        AnalysisModel(
            sample_ids=[s],
            type=AnalysisType('cram'),
            status=AnalysisStatus('completed'),
            output=f'FAKE://greek-myth/crams/{s}.cram',
            meta={'sequencing_type': 'genome'},
        )
        for s in sample_ids
    ]

    # es-index
    analyses_to_insert.append(
        AnalysisModel(
            sample_ids=random.sample(sample_ids, len(sample_ids) // 2),
            type=AnalysisType('es-index'),
            status=AnalysisStatus('completed'),
            output=f'FAKE::greek-myth-genome-{datetime.date.today()}',
            meta={},
        )
    )

    aapi = AnalysisApi()
    for ans in chunk(analyses_to_insert, 50):
        print(f'Inserting {len(ans)} analysis entries')
        await asyncio.gather(
            *[aapi.create_new_analysis_async(project, analysis_model=a) for a in ans]
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
