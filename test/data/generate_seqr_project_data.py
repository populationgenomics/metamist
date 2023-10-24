#!/usr/bin/env python3
# pylint: disable=too-many-locals,unsubscriptable-object
import argparse
import asyncio
import csv
import datetime
import logging
import random
import tempfile

import sys
from pathlib import Path
from pprint import pprint

from metamist.apis import AnalysisApi, FamilyApi, ParticipantApi, ProjectApi, SampleApi
from metamist.graphql import gql, query_async
from metamist.model.analysis import Analysis
from metamist.model.analysis_status import AnalysisStatus
from metamist.models import AssayUpsert, SampleUpsert, SequencingGroupUpsert
from metamist.parser.generic_parser import chunk

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


class ped_row:
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
    random_id = '{:s}_{:04d}'.format(random.choice(NAMES), random.randint(1, 9999))
    if random_id in used_ids:
        return generate_random_id(used_ids)
    used_ids.add(random_id)
    return random_id


def generate_pedigree_rows(num_families=1):
    """
    Generate rows for a pedigree file with random data.

    Parameters:
    - num_families: The number of families to generate.

    Returns:
    A list of rows, each row represented as a list of column values.
    """
    used_ids = set()
    rows = []
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

        elif num_individuals_in_family == 2:  # Duo
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
                    ped_row([
                        family_id,
                        individual_id,
                        parent_id,
                        '',
                        random.choice([0, 1]),
                        2,]
                    )
                )
            else:
                rows.append(
                    ped_row([
                        family_id,
                        individual_id,
                        '',
                        parent_id,
                        random.choice([0, 1]),
                        2,]
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
                    ped_row([family_id, individual_id, paternal_id, maternal_id, sex, affected])
                )

    return rows

QUERY_PROJECT_SGS = gql(
    """
query MyQuery($project: String!) {
  project(name: $project) {
    sequencingGroups {
      id
      type
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


async def main():
    """
    Generates a number of projects and populates them with randomly generated pedigrees.
    Creates family, participant, sample, and sequencing group records for the projects.
    Upserts a number of analyses for each project and sequencing group to test seqr related endpoints.
    """
    aapi = AnalysisApi()
    papi = ProjectApi()
    sapi = SampleApi()
    
    how_many_samples = {1: 0.78, 2: 0.16, 3: 0.05, 4: 0.01}
    how_many_sequencing_groups = {1: 0.78, 2: 0.16, 3: 0.05}

    def get_sequencing_types():
        """Return a random length of random sequencing types"""
        k = random.choices(
            list(how_many_sequencing_groups.keys()),
            list(how_many_sequencing_groups.values()),
        )[0]
        return random.choices(sequencing_types, weights=[0.49, 0.49, 0.02], k=k)

    def generate_random_number_within_distribution():
        return random.choices(
            list(how_many_samples.keys()), list(how_many_samples.values())
        )[0]

    enum_resp: dict[str, dict[str, list[str]]] = await query_async(QUERY_ENUMS)
    # analysis_types = enum_resp['enum']['analysisType']
    sample_types = enum_resp['enum']['sampleType']
    sequencing_technologies = enum_resp['enum']['sequencingTechnology']
    sequencing_platforms = enum_resp['enum']['sequencingPlatform']
    # sequencing_types = enum_resp['enum']['sequencingType']
    sequencing_types = ['genome', 'exome', 'transcriptome']
    assay_type = 'sequencing'

    existing_projects = await papi.get_my_projects_async()
    for project in PROJECTS:
        if project not in existing_projects:
            await papi.create_project_async(
                name=project, dataset=project, create_test_project=False
            )
            print('Created project', project)
            await papi.update_project_async(
                project=project, body={'meta': {'is_seqr': "true"}},
            )
            print('Updated project', project, 'to be seqr project')
            
        project_pedigree = generate_pedigree_rows(num_families=random.randint(1, 10))
        participant_eids = [row.individual_id for row in project_pedigree]
        
        pedfile = tempfile.NamedTemporaryFile(mode='w')
        ped_writer = csv.writer(pedfile, delimiter='\t')
        for row in project_pedigree:
            ped_writer.writerow(row)
        pedfile.flush()

        with open(pedfile.name) as f:
            await FamilyApi().import_pedigree_async(
                project=project, file=f, has_header=False, create_missing_participants=True
            )

        id_map = await ParticipantApi().get_participant_id_map_by_external_ids_async(
            project=project, request_body=participant_eids
        )

        samples = []
        for participant_eid in participant_eids:
            pid = id_map[participant_eid]

            nsamples = generate_random_number_within_distribution()
            for i in range(nsamples):
                sample = SampleUpsert(
                    external_id=f'{participant_eid}_{i+1}',
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
                                    'coverage': f'{random.choice([30, 90, 300, 9000, "?"])}x',
                                    'sequencing_type': stype,
                                    'sequencing_technology': stechnology,
                                    'sequencing_platform': splatform,
                                },
                            )
                        )

        response = await sapi.upsert_samples_async(project, samples)
        pprint(response)

        sgid_response = await query_async(QUERY_PROJECT_SGS, {'project': project})
        sequencing_group_ids = [
            sg for sg in sgid_response['project']['sequencingGroups']
        ]
        aligned_sgs = random.sample(sequencing_group_ids, k=random.randint(int(len(sequencing_group_ids)/2), len(sequencing_group_ids)))
        
        genome_sgs = [sg['id'] for sg in aligned_sgs if sg['type'] == 'genome']
        exome_sgs = [sg['id'] for sg in aligned_sgs if sg['type'] == 'exome']
        transcriptome_sgs = [sg['id'] for sg in aligned_sgs if sg['type'] == 'transcriptome']

        analyses_to_insert = [
            Analysis(
                sequencing_group_ids=[sg['id']],
                type='cram',
                status=AnalysisStatus('completed'),
                output=f'FAKE://{project}/crams/{sg["id"]}.cram',
                timestamp_completed=(
                    datetime.datetime.now() - datetime.timedelta(days=random.randint(1, 15))
                ).isoformat(),
                meta={
                    # random size between 5, 25 GB
                    'size': random.randint(5 * 1024, 25 * 1024) * 1024 * 1024,
                },
            )
            for sg in aligned_sgs
        ]
        
        # joint-call / AnnotateDataset stage + es-index stage
        for seq_type, sg_list in {'genome': genome_sgs, 'exome': exome_sgs, 'transcriptome': transcriptome_sgs}.items():
            if not sg_list:
                continue
            joint_called_sgs = random.sample(sg_list, k=random.randint(1, len(sg_list)))
            
            analyses_to_insert.extend(
                [
                    Analysis(
                        sequencing_group_ids=joint_called_sgs,
                        type='custom',
                        status=AnalysisStatus('completed'),
                        output=f'FAKE::{project}-{seq_type}-{datetime.date.today()}.mt',
                        meta={'stage': 'AnnotateDataset', 'sequencing_type': seq_type},
                    ),
                    Analysis(
                        sequencing_group_ids=joint_called_sgs,
                        type='es-index',
                        status=AnalysisStatus('completed'),
                        output=f'FAKE::{project}-{seq_type}-es-{datetime.date.today()}',
                        meta={'stage': 'MtToEs', 'sequencing_type': seq_type},
                    )
                ]
            )

        for ans in chunk(analyses_to_insert, 50):
            print(f'Inserting {len(ans)} analysis entries')
            await asyncio.gather(*[aapi.create_analysis_async(project, a) for a in ans])


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stderr,
    )
    asyncio.new_event_loop().run_until_complete(main())
