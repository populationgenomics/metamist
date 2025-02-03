#!/usr/bin/env python3
"""
This is a simple script to generate some participants and samples for testing ourdna
Local Backend API needs to run prior executing this script

"""

import argparse
import asyncio
import datetime
import random
import uuid
from typing import Sequence, Union

from metamist.apis import ParticipantApi
from metamist.models import ParticipantUpsert, SampleUpsert

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


event_type = ['OSS', 'Walk in']
processing_site = ['bbv', 'Westmead']


def random_date_range():
    "Generate a random date range"
    # Throw in the occasional invalid date to simulate the current state of the data
    # this should be removed once the data is cleaned up
    if random.randint(0, 10) == 0:
        return 'N/A', 'N/A'
    start_date = datetime.datetime.now() - datetime.timedelta(
        days=random.randint(1, 365)
    )
    end_date = start_date + datetime.timedelta(hours=random.randint(1, 150))
    return start_date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime(
        '%Y-%m-%d %H:%M:%S'
    )


def random_choice(
    choices: Sequence[Union[str, bool, int]], weight_by_index: bool = False
):
    "Pick a random choice from a list of choices"
    weighted_choices = list(choices)
    if weight_by_index:
        for i, choice in enumerate(choices):
            weighted_choices.extend([choice] * (len(choices) - i))
    return weighted_choices[random.randint(0, len(weighted_choices) - 1)]


def random_list(
    choices: Sequence[Union[str, bool, int]],
    weight_by_index: bool = False,
    min_len: int = 1,
    max_len: int = 5,
):
    "Generate a random list of choices"
    result: list[Union[str, bool, int]] = []
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


def create_samples():
    """Create a sample with nested samples"""
    start_date, end_date = random_date_range()

    meta = {
        'collection-time': start_date,
        'process-end-time': end_date,
        'collection-event-type': random_choice(event_type),
        'processing-site': random_choice(processing_site),
    }

    sample = SampleUpsert(
        external_ids={PRIMARY_EXTERNAL_ORG: str(uuid.uuid4())},
        type='blood',
        active=True,
        nested_samples=[
            SampleUpsert(
                external_ids={PRIMARY_EXTERNAL_ORG: str(uuid.uuid4())},
                type='guthrie-card',
                active=True,
                meta=meta,
            ),
            SampleUpsert(
                external_ids={PRIMARY_EXTERNAL_ORG: str(uuid.uuid4())},
                type='plasma',
                active=True,
                meta=meta,
            ),
            SampleUpsert(
                external_ids={PRIMARY_EXTERNAL_ORG: str(uuid.uuid4())},
                type='buffy-coat',
                active=True,
                meta=meta,
            ),
            SampleUpsert(
                external_ids={PRIMARY_EXTERNAL_ORG: str(uuid.uuid4())},
                type='pbmc',
                active=True,
                meta=meta,
            ),
        ],
        meta=meta,
    )

    return sample


def create_participant():
    """Create a participant with nested samples"""
    participant = ParticipantUpsert(
        external_ids={PRIMARY_EXTERNAL_ORG: str(uuid.uuid4())},
        reported_sex=random_choice([1, 2]),
        meta={
            'ancestry-participant-ancestry': random_list(
                ANCESTRIES, weight_by_index=True, min_len=1, max_len=2
            ),
            'ancestry-mother-ancestry': random_list(
                ANCESTRIES, weight_by_index=True, min_len=1, max_len=2
            ),
            'ancestry-father-ancestry': random_list(
                ANCESTRIES, weight_by_index=True, min_len=1, max_len=2
            ),
            'ancestry-mother-birthplace': random_list(
                BIRTHPLACES, weight_by_index=True, min_len=1, max_len=2
            ),
            'ancestry-father-birthplace': random_list(
                BIRTHPLACES, weight_by_index=True, min_len=1, max_len=2
            ),
            'ancestry-language-other-than-english': random_list(
                LANGUAGES, weight_by_index=True, min_len=1, max_len=2
            ),
            'birth-year': random.randint(1900, 2010),
            'blood-consent': random_choice(['yes', 'no']),
            'informed-consent': random_choice(['yes', 'no']),
            'choice-receive-genetic-info': random_choice(['yes', 'no']),
            'choice-family-receive-genetic-info': random_choice(['yes', 'no']),
            'choice-recontact': random_choice(['yes', 'no']),
            'choice-general-updates': random_choice(['yes', 'no']),
            'choice-use-of-cells-in-future-research-consent': random_choice(
                ['yes', 'no']
            ),
            'choice-use-of-cells-in-future-research-understanding': random_list(
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


async def main(project='ourdna', num_participants=10):
    """Doing the generation for you"""
    participant_api = ParticipantApi()

    participants = [create_participant() for _ in range(num_participants)]
    participants_rec = participant_api.upsert_participants(project, participants)
    print('inserted participants:', participants_rec)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Script for generating data in the ourdna test project'
    )
    parser.add_argument('--project', type=str, default='ourdna')
    parser.add_argument('--num-participants', type=str, default=10)
    args = vars(parser.parse_args())
    asyncio.new_event_loop().run_until_complete(main(**args))
