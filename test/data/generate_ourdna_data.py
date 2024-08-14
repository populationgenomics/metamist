#!/usr/bin/env python3
"""
This is a simple script to generate 3 participants & its samples in the ourdna project
Local Backend API needs to run prior executing this script

NOTE: This is WIP and will be updated with more features
If you want to regenerate the data you would need to
delete records from table sample and participant first
"""

import argparse
import asyncio

from metamist.apis import ParticipantApi
from metamist.models import ParticipantUpsert, SampleUpsert

PRIMARY_EXTERNAL_ORG = ''

PARTICIPANTS = [
    ParticipantUpsert(
        external_ids={PRIMARY_EXTERNAL_ORG: 'EX01'},
        reported_sex=2,
        karyotype='XX',
        meta={'consent': True, 'field': 1},
        samples=[
            SampleUpsert(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                active=True,
                meta={
                    'collection-time': '2022-07-03 13:28:00',
                    'processing-site': 'Garvan',
                    'process-start-time': '2022-07-06 16:28:00',
                    'process-end-time': '2022-07-06 19:28:00',
                    'received-time': '2022-07-03 14:28:00',
                    'received-by': 'YP',
                    'collection-lab': 'XYZ LAB',
                    'collection-event-name': 'walk-in',
                    'courier': 'ABC COURIERS',
                    'courier-tracking-number': 'ABCDEF12562',
                    'courier-scheduled-pickup-time': '2022-07-03 13:28:00',
                    'courier-actual-pickup-time': '2022-07-03 13:28:00',
                    'courier-scheduled-dropoff-time': '2022-07-03 13:28:00',
                    'courier-actual-dropoff-time': '2022-07-03 13:28:00',
                    'concentration': 1.45,
                },
            )
        ],
    ),
    ParticipantUpsert(
        external_ids={PRIMARY_EXTERNAL_ORG: 'EX02'},
        reported_sex=1,
        karyotype='XY',
        meta={'field': 2},
        samples=[
            SampleUpsert(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test02'},
                type='blood',
                active=True,
                meta={
                    'collection-time': '2022-07-03 13:28:00',
                    'processing-site': 'BBV',
                    'process-start-time': '2022-07-06 16:28:00',
                    'process-end-time': '2022-07-06 19:28:00',
                    'received-time': '2022-07-03 14:28:00',
                    'received-by': 'YP',
                    'collection-lab': 'XYZ LAB',
                    'collection-event-name': 'EventA',
                    'courier': 'ABC COURIERS',
                    'courier-tracking-number': 'ABCDEF12562',
                    'courier-scheduled-pickup-time': '2022-07-03 13:28:00',
                    'courier-actual-pickup-time': '2022-07-03 13:28:00',
                    'courier-scheduled-dropoff-time': '2022-07-03 13:28:00',
                    'courier-actual-dropoff-time': '2022-07-03 13:28:00',
                    'concentration': 0.98,
                },
            )
        ],
    ),
    ParticipantUpsert(
        external_ids={PRIMARY_EXTERNAL_ORG: 'EX03'},
        reported_sex=2,
        karyotype='XX',
        meta={'consent': True, 'field': 3},
        samples=[
            SampleUpsert(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test03'},
                type='blood',
                active=True,
                meta={
                    # 'collection-time': '2022-07-03 13:28:00',
                    'processing-site': 'Garvan',
                    # 'process-start-time': '2022-07-03 16:28:00',
                    # 'process-end-time': '2022-07-03 19:28:00',
                    'received-time': '2022-07-03 14:28:00',
                    'received-by': 'YP',
                    'collection-lab': 'XYZ LAB',
                    'courier': 'ABC COURIERS',
                    'courier-tracking-number': 'ABCDEF12562',
                    'courier-scheduled-pickup-time': '2022-07-03 13:28:00',
                    'courier-actual-pickup-time': '2022-07-03 13:28:00',
                    'courier-scheduled-dropoff-time': '2022-07-03 13:28:00',
                    'courier-actual-dropoff-time': '2022-07-03 13:28:00',
                    'concentration': 1.66,
                },
            )
        ],
    ),
]


async def main(project='ourdna'):
    """Doing the generation for you"""
    participant_api = ParticipantApi()
    participants_rec = participant_api.upsert_participants(project, PARTICIPANTS)
    print('inserted participants:', participants_rec)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Script for generating data in the ourdna test project'
    )
    parser.add_argument('--project', type=str, default='ourdna')
    args = vars(parser.parse_args())
    asyncio.new_event_loop().run_until_complete(main(**args))
