import datetime
import random
from typing import IO

from sample_metadata.models import (
    SampleType,
    SequenceStatus,
    NewSample,
    NewSequence,
    SequenceType,
)

from sample_metadata.apis import (
    ProjectApi,
    FamilyApi,
    ParticipantApi,
    SampleApi,
    SequenceApi,
)

# from sample_metadata.configuration import m

EMOJIS = [':)', ':(', ':/', ':\'(']


def main(
    ped_path='greek-myth-forgeneration.ped', project='greek-myth', create_project=False
):
    """Doing the generation for you"""
    if create_project:
        # gcp_id is ignored
        ProjectApi().create_project(
            name=project, dataset=project, gcp_id=project, create_test_project=False
        )

    with open(ped_path, encoding='utf-8') as f:
        # skip the first line
        _ = f.readline()
        participant_eids = [line.split('\t')[1] for line in f]

    with open(ped_path) as f:  # type: IO[str]
        FamilyApi().import_pedigree(
            project=project, file=f, has_header=True, create_missing_participants=True
        )

    id_map = ParticipantApi().get_participant_id_map_by_external_ids(
        project=project, request_body=participant_eids
    )

    how_many_samples = {1: 0.78, 2: 0.16, 3: 0.05, 4: 0.01}

    def generate_random_number_within_distribution():
        return random.choices(
            list(how_many_samples.keys()), list(how_many_samples.values())
        )[0]

    sample_id_index = 1003
    for participant_eid in participant_eids:
        pid = id_map[participant_eid]

        nsamples = generate_random_number_within_distribution()
        for _ in range(nsamples):
            sample_id = SampleApi().create_new_sample(
                project=project,
                new_sample=NewSample(
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
                ),
            )
            print(f'Created sample {sample_id} for {participant_eid}')
            sample_id_index += random.randint(1, 4)
            for _ in range(generate_random_number_within_distribution()):
                _ = SequenceApi().create_new_sequence(
                    NewSequence(
                        sample_id=sample_id,
                        status=SequenceStatus('uploaded'),
                        type=SequenceType(
                            random.choice(['wgs', 'exome', 'single-cell'])
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


if __name__ == '__main__':
    main()
