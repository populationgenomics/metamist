import datetime
import random
from typing import IO

from sample_metadata.models import SampleType, SequenceStatus, NewSample, NewSequence, SequenceType

from sample_metadata.apis import *

# from sample_metadata.configuration import m

EMOJIS = [':)', ':(', ':/', ':\'(']
# https://en.wikipedia.org/wiki/Ancient_Greek_medicine#:~:text=four%20humors%3A%20blood%2C%20phlegm%2C%20yellow%20bile%2C%20and%20black%20bile
sample_balance = ['blood', 'phlegm', 'yellow bile', 'black bile']
def main(
    ped_path="greek-myth-forgeneration.ped", project="greek-myth", create_project=False
):
    if create_project:
        # gcp_id is ignored
        ProjectApi().create_project(name=project, dataset=project, gcp_id=project, create_test_project=False)

    with open(ped_path, encoding='utf-8') as f:
        # skip the first line
        _ = f.readline()
        participant_eids = [l.split("\t")[1] for l in f]

    # with open(ped_path) as f:  # type: IO[str]
    #     FamilyApi().import_pedigree(
    #         project=project, file=f, has_header=True, create_missing_participants=True
    #     )

    id_map = ParticipantApi().get_participant_id_map_by_external_ids(
        project=project, request_body=participant_eids
    )

    how_many_samples = {1: 0.80, 2: 0.15, 3: 0.05}

    def generate_random_number_within_distribution():
        return random.choices(
            list(how_many_samples.keys()), list(how_many_samples.values())
        )[0]

    sample_id_index = 1003
    for participant_eid in participant_eids:
        pid = id_map[participant_eid]

        nsamples = generate_random_number_within_distribution()
        for sample_number in range(nsamples):
            sample_id = SampleApi().create_new_sample(
                project=project,
                new_sample=NewSample(
                    external_id=f'GRK{sample_id_index}',
                    type=SampleType('blood'),
                    meta={
                        'generated_data': True,
                        'collection_date': datetime.datetime.now() - datetime.timedelta(minutes=random.randint(-100, 10000)),
                    },
                    participant_id=pid,
                ),
            )
            print(f'Created sample {sample_id} for {participant_eid}')
            sample_id_index += 1
            for sequence_number in range(generate_random_number_within_distribution()):
                sequence = SequenceApi().create_new_sequence(
                    NewSequence(
                        sample_id=sample_id,
                        status=SequenceStatus('uploaded'),
                        type=SequenceType(random.choice(['wgs', 'exome', 'single-cell'])),
                        meta={
                            'facility': 'Software team',
                            'emoji': random.choice(EMOJIS),
                            'technology': 'pacbio',
                            'coverage': '900x'
                        },
                    )
                )


if __name__ == '__main__':
    main()
