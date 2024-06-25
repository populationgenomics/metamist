from collections import defaultdict
from datetime import datetime
from math import ceil
from test.testbase import DbIsolatedTest, run_as_sync

from db.python.enum_tables.sample_type import SampleTypeTable
from db.python.layers.ourdna.dashboard import OurDnaDashboardLayer
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    OurDNADashboard,
    ParticipantUpsertInternal,
    SampleUpsert,
    SampleUpsertInternal,
)


def str_to_datetime(timestamp_str):
    """Convert string timestamp to datetime"""
    return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')


class OurDNADashboardTest(DbIsolatedTest):
    """Test sample class"""

    @run_as_sync
    async def setUp(self) -> None:
        # don't need to await because it's tagged @run_as_sync
        super().setUp()
        self.odd = OurDnaDashboardLayer(self.connection)
        self.sl = SampleLayer(self.connection)
        self.pl = ParticipantLayer(self.connection)

        self.stt = SampleTypeTable(self.connection)
        await self.stt.insert('ebld')

        participants = await self.pl.upsert_participants(
            [
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'EX01'},
                    reported_sex=2,
                    karyotype='XX',
                    meta={'consent': True, 'field': 1},
                    samples=[
                        SampleUpsertInternal(
                            external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
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
                            type='ebld',
                            active=True,
                            nested_samples=[
                                # add a random sample here to test it's not collected
                                SampleUpsertInternal(
                                    external_ids={PRIMARY_EXTERNAL_ORG: 'Test01-01'},
                                    type="blood",
                                    meta={
                                        # something wild
                                        'collection-time': '1999-01-01 12:34:56',
                                    },
                                )
                            ],
                        )
                    ],
                ),
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'EX02'},
                    reported_sex=1,
                    karyotype='XY',
                    meta={'field': 2},
                    samples=[
                        SampleUpsertInternal(
                            external_ids={PRIMARY_EXTERNAL_ORG: 'Test02'},
                            meta={
                                'collection-time': '2022-07-03 13:28:00',
                                'processing-site': 'BBV',
                                # 'process-start-time': '2022-07-06 16:28:00',
                                # 'process-end-time': '2022-07-06 19:28:00',
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
                            type='ebld',
                            active=True,
                        )
                    ],
                ),
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'EX03'},
                    reported_sex=2,
                    karyotype='XX',
                    meta={'consent': True, 'field': 3},
                    samples=[
                        SampleUpsertInternal(
                            external_ids={PRIMARY_EXTERNAL_ORG: 'Test03'},
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
                            type='ebld',
                            active=True,
                        )
                    ],
                ),
            ],
        )

        self.participants_external_objects = [
            participant.to_external() for participant in participants
        ]

        self.sample_external_objects: list[SampleUpsert] = []
        self.sample_internal_objects: list[SampleUpsertInternal] = []

        self.sample_internal_objects.extend(
            sample for participant in participants for sample in participant.samples
        )
        self.sample_external_objects.extend(
            sample.to_external() for sample in self.sample_internal_objects
        )

    @run_as_sync
    async def test_get_dashboard(self):
        """Test get_dashboard"""
        dashboard = await self.odd.query(project_id=self.project_id)

        # Check that the dashboard is not empty and is a dict
        self.assertTrue(dashboard)
        self.assertIsInstance(dashboard, OurDNADashboard)

    @run_as_sync
    async def test_collection_to_process_end_time(self):
        """I want to know how long it took between blood collection and sample processing"""
        dashboard = await self.odd.query(project_id=self.project_id)
        collection_to_process_end_time = dashboard.collection_to_process_end_time

        # Check that collection_to_process_end_time is not empty and is a dict
        self.assertTrue(collection_to_process_end_time)
        self.assertIsInstance(collection_to_process_end_time, dict)

        samples_filtered: list[str] = []
        for sample in self.sample_external_objects:
            assert isinstance(sample.meta, dict)
            collection_time = sample.meta.get('collection-time')
            process_end_time = sample.meta.get('process-end-time')
            # Skip samples that don't have collection_time or process_end_time
            if not collection_time or not process_end_time:
                continue
            time_difference = str_to_datetime(process_end_time) - str_to_datetime(
                collection_time
            )
            if time_difference.total_seconds():
                # Check that the time difference matches
                assert isinstance(sample.id, str)

                samples_filtered.append(sample.id)

                self.assertEqual(
                    time_difference.total_seconds(),
                    collection_to_process_end_time[sample.id],
                )

        # Check the number of samples in the cohort
        self.assertCountEqual(collection_to_process_end_time.keys(), samples_filtered)

        for _sample_id, time_diff in collection_to_process_end_time.items():
            self.assertGreater(time_diff, 0)
            self.assertIsInstance(time_diff, int)

    @run_as_sync
    async def test_collection_to_process_end_time_24h(self):
        """I want to know which samples took more than 24 hours between blood collection and sample processing completion"""
        dashboard = await self.odd.query(project_id=self.project_id)
        collection_to_process_end_time_24h = (
            dashboard.collection_to_process_end_time_24h
        )

        # Check that collection_to_process_end_time is not empty and is a dict
        self.assertTrue(collection_to_process_end_time_24h)
        self.assertIsInstance(collection_to_process_end_time_24h, dict)

        samples_filtered: list[str] = []
        for sample in self.sample_external_objects:
            assert isinstance(sample.meta, dict)
            collection_time = sample.meta.get('collection-time')
            process_end_time = sample.meta.get('process-end-time')
            # Skip samples that don't have collection_time or process_end_time
            if not collection_time or not process_end_time:
                continue
            time_difference = str_to_datetime(process_end_time) - str_to_datetime(
                collection_time
            )
            if time_difference.total_seconds() > 24 * 3600:
                # Check that the time difference matches
                assert isinstance(sample.id, str)

                samples_filtered.append(sample.id)

                self.assertEqual(
                    time_difference.total_seconds(),
                    collection_to_process_end_time_24h[sample.id],
                )

        # check that there are a correct number of matching results
        self.assertCountEqual(
            collection_to_process_end_time_24h.keys(), samples_filtered
        )

    @run_as_sync
    async def test_processing_times_per_site(self):
        """I want to know what the sample processing times were for samples at each designated site"""
        dashboard = await self.odd.query(project_id=self.project_id)
        processing_times_by_site = dashboard.processing_times_by_site

        # Check that processing_times_per_site is not empty and is a dict
        self.assertTrue(processing_times_by_site)
        self.assertIsInstance(processing_times_by_site, dict)

        sample_tally: dict[str, dict[int, int]] = defaultdict()
        for sample in self.sample_external_objects:
            assert isinstance(sample.meta, dict)
            processing_site = sample.meta.get('processing-site', 'Unknown')
            process_start_time = sample.meta.get('process-start-time')
            process_end_time = sample.meta.get('process-end-time')
            # Skip samples that don't have process_start_time or process_end_time
            if not process_start_time or not process_end_time:
                continue
            time_difference = str_to_datetime(process_end_time) - str_to_datetime(
                process_start_time
            )
            current_bucket = ceil(time_difference.total_seconds() / 3600)
            if processing_site in sample_tally:
                sample_tally[processing_site][current_bucket] += 1
            else:
                sample_tally[processing_site] = {}
                sample_tally[processing_site][current_bucket] = 1

        # Checks that we have identical dicts (by extension, keys and their values)
        self.assertDictEqual(processing_times_by_site, sample_tally)

    @run_as_sync
    async def test_total_samples_by_collection_event_name(self):
        """I want to know how many samples were collected from walk-ins vs during events or scheduled activities"""
        dashboard = await self.odd.query(project_id=self.project_id)
        total_samples_by_collection_event_name = (
            dashboard.total_samples_by_collection_event_name
        )

        # Check that total_samples_by_collection_event_name is not empty and is a dict
        self.assertTrue(total_samples_by_collection_event_name)
        self.assertIsInstance(total_samples_by_collection_event_name, dict)

        sample_tally: dict[str, int] = defaultdict()
        for sample in self.sample_external_objects:
            assert isinstance(sample.meta, dict)
            event_name = sample.meta.get('collection-event-name', 'Unknown')
            if event_name in sample_tally:
                sample_tally[event_name] += 1
            else:
                sample_tally[event_name] = 1

        # Check that the tally and the total_samples_by_collection_event_name are the same, by extension, keys and their values
        self.assertDictEqual(total_samples_by_collection_event_name, sample_tally)

    @run_as_sync
    async def test_samples_lost_after_collection(self):
        """I need to know how many samples have been lost, EG: participants have been consented, blood collected, not processed"""
        dashboard = await self.odd.query(project_id=self.project_id)
        samples_lost_after_collection = dashboard.samples_lost_after_collection

        # Check that samples_lost_after_collection is not empty and is a dict
        self.assertTrue(samples_lost_after_collection)
        self.assertIsInstance(samples_lost_after_collection, list)

        # Check that the number of samples in the list is correct
        samples_filtered: list[str] = []
        sample_ids_lost_after_collection = [
            sample.sample_id for sample in samples_lost_after_collection
        ]
        for sample in self.sample_external_objects:
            assert isinstance(sample.meta, dict)
            collection_time = sample.meta.get('collection-time')
            process_start_time = sample.meta.get('process-start-time')
            # Skip samples that don't have collection_time or process_end_time
            if not collection_time:
                continue
            time_difference = datetime.now() - str_to_datetime(collection_time)
            if time_difference.total_seconds() > 72 * 3600 and not process_start_time:
                # Check that the time difference matches
                assert isinstance(sample.id, str)

                samples_filtered.append(sample.id)

                for sample_data in samples_lost_after_collection:
                    if sample_data.sample_id == sample.id:
                        self.assertEqual(
                            int(time_difference.total_seconds()),
                            sample_data.time_since_collection,
                        )

        # check that there are a correct number of matching results
        self.assertCountEqual(sample_ids_lost_after_collection, samples_filtered)

        # TODO: Add assertions VB

    @run_as_sync
    async def test_samples_more_than_1ug_dna(self):
        """I want to generate a list of samples containing more than 1 ug of DNA to prioritise them for long-read sequencing applications"""
        dashboard = await self.odd.query(project_id=self.project_id)
        samples_more_than_1ug_dna = dashboard.samples_concentration_gt_1ug

        # Check that samples_concentratiom_gt_1ug is not empty and is a dict
        self.assertTrue(samples_more_than_1ug_dna)
        self.assertIsInstance(samples_more_than_1ug_dna, dict)

        # Check that the number of samples in the list is correct
        samples_filtered: list[str] = []
        for sample in self.sample_external_objects:
            assert isinstance(sample.meta, dict)
            if sample.meta['concentration'] and sample.meta['concentration'] > 1:
                assert isinstance(sample.id, str)

                samples_filtered.append(sample.id)

        self.assertCountEqual(samples_more_than_1ug_dna, samples_filtered)

    @run_as_sync
    async def test_participants_consented_not_collected(self):
        """I want to know how many people who have consented and NOT given blood"""
        dashboard = await self.odd.query(project_id=self.project_id)
        # print(dashboard)
        participants_consented_not_collected = (
            dashboard.participants_consented_not_collected
        )

        # Check that participants_consented_not_collected is not empty and is a dict
        self.assertTrue(participants_consented_not_collected)
        self.assertIsInstance(participants_consented_not_collected, list)

        # Check that the number of participants in the list is correct
        participants_filtered: list[int] = []
        for participant in self.participants_external_objects:
            assert isinstance(participant.meta, dict)
            samples_for_participant = [
                sample
                for sample in self.sample_external_objects
                if sample.participant_id == participant.id
                and isinstance(sample.meta, dict)
            ]
            if participant.meta.get('consent') and any(
                isinstance(sample.meta, dict)
                and sample.meta.get('collection-time') is None
                for sample in samples_for_participant
            ):
                assert isinstance(participant.id, int)
                participants_filtered.append(participant.id)

        self.assertCountEqual(
            participants_consented_not_collected, participants_filtered
        )

    @run_as_sync
    async def test_participants_signed_not_consented(self):
        """I want to know how many people have signed up but not consented"""
        dashboard = await self.odd.query(project_id=self.project_id)
        # print(dashboard)
        participants_signed_not_consented = dashboard.participants_signed_not_consented

        # Check that participants_signed_not_consented is not empty and is a dict
        self.assertTrue(participants_signed_not_consented)
        self.assertIsInstance(participants_signed_not_consented, list)

        # Check that the number of participants in the list is correct
        participants_filtered: list[int] = []
        for participant in self.participants_external_objects:
            assert isinstance(participant.meta, dict)
            if not participant.meta.get('consent'):
                assert isinstance(participant.id, int)
                participants_filtered.append(participant.id)

        self.assertCountEqual(participants_signed_not_consented, participants_filtered)
