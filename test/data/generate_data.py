#!/usr/bin/env python3
# pylint: disable=too-many-locals,unsubscriptable-object
import argparse
import asyncio
import datetime
import random
from pathlib import Path
from pprint import pprint

from metamist.apis import AnalysisApi, FamilyApi, ParticipantApi, ProjectApi, SampleApi
from metamist.graphql import gql, query_async
from metamist.model.analysis import Analysis
from metamist.model.analysis_status import AnalysisStatus
from metamist.models import AssayUpsert, SampleUpsert, SequencingGroupUpsert
from metamist.parser.generic_parser import chunk

EMOJIS = [':)', ':(', ':/', ':\'(']

default_ped_location = str(Path(__file__).parent / 'greek-myth-forgeneration.ped')

QUERY_SG_ID = gql(
    """
query MyQuery($project: String!) {
  project(name: $project) {
    sequencingGroups {
      id
    }
  }
}"""
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
}"""
)


async def main(ped_path=default_ped_location, project='greek-myth'):
    """Doing the generation for you"""

    papi = ProjectApi()
    sapi = SampleApi()
    aapi = AnalysisApi()

    enum_resp: dict[str, dict[str, list[str]]] = await query_async(QUERY_ENUMS)
    # analysis_types = enum_resp['enum']['analysisType']
    sample_types = enum_resp['enum']['sampleType']
    sequencing_technologies = enum_resp['enum']['sequencingTechnology']
    sequencing_platforms = enum_resp['enum']['sequencingPlatform']
    sequencing_types = enum_resp['enum']['sequencingType']

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
    how_many_sequencing_groups = {1: 0.78, 2: 0.16, 3: 0.05}

    def get_sequencing_types():
        """Return a random length of random sequencing types"""
        k = random.choices(
            list(how_many_sequencing_groups.keys()),
            list(how_many_sequencing_groups.values()),
        )[0]
        return random.choices(sequencing_types, k=k)

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
                assays=[],
                sequencing_groups=[],
            )
            samples.append(sample)

            sample_id_index += random.randint(1, 4)
            for stype in get_sequencing_types():
                facility = random.choice(
                    [
                        'Amazing sequence centre',
                        'Sequence central',
                        'Dept of Seq.',
                    ]
                )
                stechnology = random.choice(sequencing_technologies)
                splatform = random.choice(sequencing_platforms)
                sg = SequencingGroupUpsert(
                    type=stype,
                    technology=stechnology,
                    platform=splatform,
                    meta={
                        'facility': facility,
                    },
                    assays=[],
                )
                sample.sequencing_groups.append(sg)
                for _ in range(generate_random_number_within_distribution()):
                    sg.assays.append(
                        AssayUpsert(
                            type=assay_type,
                            meta={
                                'facility': facility,
                                'emoji': random.choice(EMOJIS),
                                'coverage': f'{random.choice([30, 90, 300, 9000, "?"])}x',
                                'sequencing_type': stype,
                                'sequencing_technology': stechnology,
                                'sequencing_platform': splatform,
                            },
                        )
                    )

    response = await sapi.upsert_samples_async(project, samples)
    pprint(response)

    # practice what you preach I guess

    sgid_response = await query_async(QUERY_SG_ID, {'project': project})
    sequencing_group_ids = [
        sg['id'] for sg in sgid_response['project']['sequencingGroups']
    ]

    analyses_to_insert = [
        Analysis(
            sequencing_group_ids=random.sample(
                sequencing_group_ids, len(sequencing_group_ids) // 2
            ),
            type='es-index',
            status=AnalysisStatus('completed'),
            outputs={
                'qc_results': {
                    'basename': 'gs://sm-dev-test/file1.txt'
                    },
                'cram': {
                    'basename': 'gs://sm-dev-test/file1.cram'
                },
                'qc': {
                    'cram': {
                        'basename': 'gs://sm-dev-test/file2.cram',
                        'secondaryFiles':
                            {
                                'cram_ext': {
                                    'basename': 'gs://sm-dev-test/file2.cram.ext'
                                },
                                'cram_meta': {
                                    'basename': 'gs://sm-dev-test/file2.cram.meta'
                                }
                            }
                    },
                    'aggregate': {
                        'cram': {
                            'basename': 'gs://sm-dev-test/file3.cram',
                            'secondaryFiles': {
                                    'cram_ext': {
                                        'basename': 'gs://sm-dev-test/file3.cram.ext'
                                    },
                                    'cram_meta': {
                                        'basename': 'gs://sm-dev-test/file3.cram.meta'
                                    }
                                }
                        },
                        'qc': {
                            'basename': 'gs://sm-dev-test/file1.qc',
                            'secondaryFiles': {
                                'qc_ext': {
                                    'basename': 'gs://sm-dev-test/file1.qc.ext'
                                },
                                'qc_meta': {
                                    'basename': 'gs://sm-dev-test/file1.qc.meta'
                                }
                            }
                        },
                    }
                }
            },
            meta={},
        )
    ]

    analyses_to_insert.extend([
        Analysis(
            sequencing_group_ids=[s],
            type='cram',
            status=AnalysisStatus('completed'),
            outputs={'basename': f'FAKE://greek-myth/crams/{s}.cram'},
            timestamp_completed=(
                datetime.datetime.now() - datetime.timedelta(days=random.randint(1, 15))
            ).isoformat(),
            meta={
                'sequencing_type': 'genome',
                # random size between 5, 25 GB
                'size': random.randint(5 * 1024, 25 * 1024) * 1024 * 1024,
            },
        )
        for s in sequencing_group_ids
    ])

    analyses_to_insert.extend(
        [
            Analysis(
                sample_ids=[],
                type='analysis-runner',
                status=AnalysisStatus('completed'),
                outputs={'basename': f'FAKE://greek-myth-test/joint-calling/{s}.joint'},
                active=True,
                meta={
                    'accessLevel': 'full',
                    'commit': 'some-hash',
                    'script': 'myFakeScript.py',
                    'description': 'just analysis things',
                    'hailVersion': '1.0',
                    'source': 'analysis-runner',
                    'cwd': 'scripts',
                    'repo': 'some-repo',
                    'driverImage': 'fake-australia-southeast1-fake-docker.pkg',
                    'batch_url': f'FAKE://batch.hail.populationgenomics.org.au/batches/fake_{s}',
                },
            )
            for s in random.choices(sequencing_group_ids, k=10)
        ]
    )

    # es-index
    analyses_to_insert.append(
        Analysis(
            sequencing_group_ids=random.sample(
                sequencing_group_ids, len(sequencing_group_ids) // 2
            ),
            type='es-index',
            status=AnalysisStatus('completed'),
            outputs={'basename': f'FAKE::greek-myth-genome-{datetime.date.today()}'},
            meta={},
        )
    )

    for ans in chunk(analyses_to_insert, 50):
        print(f'Inserting {len(ans)} analysis entries')
        await asyncio.gather(*[aapi.create_analysis_async(project, a) for a in ans])

    # create some fake analysis-runner entries
    ar_entries = 20
    print(f'Inserting {ar_entries} analysis-runner entries')
    await asyncio.gather(
        *(
            aapi.create_analysis_async(
                project,
                Analysis(
                    sequencing_group_ids=[],
                    type='analysis-runner',
                    status=AnalysisStatus('unknown'),
                    outputs={'basename': 'gs://cpg-fake-bucket/output'},
                    meta={
                        'timestamp': f'2022-08-{i+1}T10:00:00.0000+00:00',
                        'accessLevel': 'standard',
                        'repo': 'sample-metadata',
                        'commit': '7234c13855cc15b3471d340757ce87e7441abeb9',
                        'script': 'python3 -m <script>',
                        'description': f'Run {i+1}',
                        'driverImage': 'australia-southeast1-docker.pkg.dev/ar/images/image:tag',
                        'configPath': 'gs://cpg-config/<guid>.toml',
                        'cwd': None,
                        'hailVersion': '0.2.126-cac7ac4164b2',
                        'batch_url': 'https://batch.hail.populationgenomics.org.au/batches/0',
                        'source': 'analysis-runner',
                    },
                ),
            )
            for i in range(ar_entries)
        )
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Script for generating data in the greek-myth project'
    )
    parser.add_argument(
        '--ped-path',
        type=str,
        default=default_ped_location,
        help='Path to the pedigree file',
    )
    parser.add_argument('--project', type=str, default='greek-myth')
    args = vars(parser.parse_args())
    asyncio.new_event_loop().run_until_complete(main(**args))
