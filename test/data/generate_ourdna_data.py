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


event_type = ['one-stop-shop', 'walk-in']
processing_site = ['bbv', 'westmead']


def random_dates(
    start_between: tuple[datetime.datetime, datetime.datetime],
    rough_gaps: list[datetime.timedelta],
):
    "Generate a list of random dates in order"
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
                    'processing_sop_version': f'WIMR v.{random.randint(1,3)}.{random.randint(0,9)}',
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
                    'processing_sop_version': f'WIMR v.{random.randint(1,3)}.{random.randint(0,9)}',
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
                    'processing_sop_version': f'WIMR v.{random.randint(1,3)}.{random.randint(0,9)}',
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
                    'processing_sop_version': f'WIMR v.{random.randint(1,3)}.{random.randint(0,9)}',
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
                    'processing_sop_version': f'WIMR v.{random.randint(1,3)}.{random.randint(0,9)}',
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

    if random.random() < 0.3:
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
    parser.add_argument('--num-participants', type=int, default=10)
    args = vars(parser.parse_args())
    asyncio.new_event_loop().run_until_complete(main(**args))
