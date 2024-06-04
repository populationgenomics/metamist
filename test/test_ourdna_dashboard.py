from collections import OrderedDict
from datetime import datetime
from math import ceil
from test.testbase import DbIsolatedTest, run_as_sync

from db.python.layers.ourdna.dashboard import OurDnaDashboardLayer
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from db.python.tables.sample import SampleFilter
from db.python.utils import GenericFilter
from models.models import (
    ParticipantUpsert,
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

        participants = await self.pl.upsert_participants(
            [
                ParticipantUpsertInternal(
                    external_id='EX01',
                    reported_sex=2,
                    karyotype='XX',
                    meta={'consent': True, 'field': 1},
                    samples=[
                        SampleUpsertInternal(
                            external_id='Test01',
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
                            type='blood',
                            active=True,
                        )
                    ],
                ),
                ParticipantUpsertInternal(
                    external_id='EX02',
                    reported_sex=1,
                    karyotype='XY',
                    meta={'field': 2},
                    samples=[
                        SampleUpsertInternal(
                            external_id='Test02',
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
                            type='blood',
                            active=True,
                        )
                    ],
                ),
                ParticipantUpsertInternal(
                    external_id='EX03',
                    reported_sex=2,
                    karyotype='XX',
                    meta={'consent': True, 'field': 3},
                    samples=[
                        SampleUpsertInternal(
                            external_id='Test03',
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
                            type='blood',
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
        sample_filter = SampleFilter(project=GenericFilter(eq=self.project_id))
        dashboard = (await self.odd.query(sample_filter)).to_dict()

        # Check that the dashboard is not empty and is a dict
        assert dashboard
        assert isinstance(dashboard, dict)

    @run_as_sync
    async def test_collection_to_process_end_time(self):
        """I want to know how long it took between blood collection and sample processing"""
        sample_filter = SampleFilter(project=GenericFilter(eq=self.project_id))
        dashboard = (await self.odd.query(sample_filter)).to_dict()
        collection_to_process_end_time = dashboard['collection_to_process_end_time']

        # Check that collection_to_process_end_time is not empty and is a dict
        assert collection_to_process_end_time
        assert isinstance(collection_to_process_end_time, dict)

        samples_filtered: list[SampleUpsert] = []
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
                samples_filtered.append(sample)

                # Check that the time difference matches
                assert (
                    time_difference.total_seconds()
                    == collection_to_process_end_time[sample.id]
                )

                # Check that the sample id is in the dict
                assert sample.id in collection_to_process_end_time

        # Check the number of samples in the cohort
        assert len(collection_to_process_end_time.keys()) == len(samples_filtered)

        for _sample_id, time_diff in collection_to_process_end_time.items():
            assert (
                time_diff > 0
            )  # NOTE: Should we actually check this explicitly in the code instead?
            assert isinstance(time_diff, int)

    @run_as_sync
    async def test_collection_to_process_end_time_24h(self):
        """I want to know which samples took more than 24 hours between blood collection and sample processing completion"""
        sample_filter = SampleFilter(project=GenericFilter(eq=self.project_id))
        dashboard = (await self.odd.query(sample_filter)).to_dict()
        collection_to_process_end_time_24h = dashboard.get(
            'collection_to_process_end_time_24h'
        )

        # Check that collection_to_process_end_time is not empty and is a dict
        assert collection_to_process_end_time_24h
        assert isinstance(collection_to_process_end_time_24h, dict)

        samples_filtered: list[SampleUpsert] = []
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
                samples_filtered.append(sample)

                # Check that the time difference matches
                assert (
                    time_difference.total_seconds()
                    == collection_to_process_end_time_24h[sample.id]
                )

                # Check that the sample id is in the dict
                assert sample.id in collection_to_process_end_time_24h

        # check that there are a correct number of matching results
        assert len(collection_to_process_end_time_24h.keys()) == len(samples_filtered)

    @run_as_sync
    async def test_processing_times_per_site(self):
        """I want to know what the sample processing times were for samples at each designated site"""
        sample_filter = SampleFilter(project=GenericFilter(eq=self.project_id))
        dashboard = (await self.odd.query(sample_filter)).to_dict()
        processing_times_by_site = dashboard.get('processing_times_by_site')

        # Check that processing_times_per_site is not empty and is a dict
        assert processing_times_by_site
        assert isinstance(processing_times_by_site, dict)

        sample_tally: dict[str, dict[str, int]] = OrderedDict()
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
        assert OrderedDict(processing_times_by_site) == sample_tally

    @run_as_sync
    async def test_total_samples_by_collection_event_name(self):
        """I want to know how many samples were collected from walk-ins vs during events or scheduled activities"""
        sample_filter = SampleFilter(project=GenericFilter(eq=self.project_id))
        dashboard = (await self.odd.query(sample_filter)).to_dict()
        total_samples_by_collection_event_name = dashboard.get(
            'total_samples_by_collection_event_name'
        )

        # Check that total_samples_by_collection_event_name is not empty and is a dict
        assert total_samples_by_collection_event_name
        assert isinstance(total_samples_by_collection_event_name, dict)

        sample_tally: dict[str, int] = OrderedDict()
        for sample in self.sample_external_objects:
            assert isinstance(sample.meta, dict)
            event_name = sample.meta.get('collection-event-name', 'Unknown')
            if event_name in sample_tally:
                sample_tally[event_name] += 1
            else:
                sample_tally[event_name] = 1

        # Check that the tally and the total_samples_by_collection_event_name are the same, by extension, keys and their values
        assert OrderedDict(total_samples_by_collection_event_name) == sample_tally

    @run_as_sync
    async def test_samples_lost_after_collection(self):
        """I need to know how many samples have been lost, EG: participants have been consented, blood collected, not processed"""
        sample_filter = SampleFilter(project=GenericFilter(eq=self.project_id))
        dashboard = (await self.odd.query(sample_filter)).to_dict()
        samples_lost_after_collection = dashboard.get('samples_lost_after_collection')

        # Check that samples_lost_after_collection is not empty and is a dict
        assert samples_lost_after_collection
        assert isinstance(samples_lost_after_collection, dict)

        # Check that the number of samples in the list is correct
        samples_filtered: list[SampleUpsert] = []
        for sample in self.sample_external_objects:
            assert isinstance(sample.meta, dict)
            collection_time = sample.meta.get('collection-time')
            process_start_time = sample.meta.get('process-start-time')
            # Skip samples that don't have collection_time or process_end_time
            if not collection_time or not process_start_time:
                continue
            time_difference = str_to_datetime(process_start_time) - str_to_datetime(
                collection_time
            )
            if time_difference.total_seconds() > 72 * 3600:
                samples_filtered.append(sample)

                # Check that the time difference matches
                assert (
                    time_difference.total_seconds()
                    == samples_lost_after_collection[sample.id]['time_to_process_start']
                )

                # Check that the sample id is in the dict
                assert sample.id in samples_lost_after_collection

        # check that there are a correct number of matching results
        assert len(samples_lost_after_collection.keys()) == len(samples_filtered)

        # TODO: Add assertions VB

    @run_as_sync
    async def test_samples_more_than_1ug_dna(self):
        """I want to generate a list of samples containing more than 1 ug of DNA to prioritise them for long-read sequencing applications"""
        sample_filter = SampleFilter(project=GenericFilter(eq=self.project_id))
        dashboard = (await self.odd.query(sample_filter)).to_dict()
        samples_more_than_1ug_dna = dashboard.get('samples_concentration_gt_1ug')

        # Check that samples_concentratiom_gt_1ug is not empty and is a dict
        assert samples_more_than_1ug_dna
        assert isinstance(samples_more_than_1ug_dna, dict)

        # Check that the number of samples in the list is correct
        samples_filtered: list[SampleUpsert] = []
        for sample in self.sample_external_objects:
            assert isinstance(sample.meta, dict)
            if (
                sample.meta.get('concentration')
                and sample.meta.get('concentration') > 1
            ):
                samples_filtered.append(sample)

                # Check that the sample id is in the dict
                assert sample.id in samples_more_than_1ug_dna

        assert len(samples_more_than_1ug_dna) == len(samples_filtered)

    @run_as_sync
    async def test_participants_consented_not_collected(self):
        """I want to know how many people who have consented and NOT given blood"""
        sample_filter = SampleFilter(project=GenericFilter(eq=self.project_id))
        dashboard = (
            await self.odd.query(sample_filter, project_id=self.project_id)
        ).to_dict()
        # print(dashboard)
        participants_consented_not_collected = dashboard.get(
            'participants_consented_not_collected'
        )

        # Check that participants_signed_not_consented is not empty and is a dict
        assert participants_consented_not_collected
        assert isinstance(participants_consented_not_collected, list)

        # Check that the number of participants in the list is correct
        participants_filtered: list[ParticipantUpsert] = []
        for participant in self.participants_external_objects:
            assert isinstance(participant.meta, dict)
            samples_for_participant = [
                sample
                for sample in self.sample_external_objects
                if sample.participant_id == participant.id
                and isinstance(sample.meta, dict)
            ]
            if participant.meta.get('consent') and not any(
                isinstance(sample.meta, dict) and sample.meta.get('collection-time')
                for sample in samples_for_participant
            ):
                participants_filtered.append(participant)

                # Check that the participant id is in the dict
                assert participant.id in participants_consented_not_collected

        assert len(participants_consented_not_collected) == len(participants_filtered)

    @run_as_sync
    async def test_participants_signed_not_consented(self):
        """I want to know how many people have signed up but not consented"""
        sample_filter = SampleFilter(project=GenericFilter(eq=self.project_id))
        dashboard = (
            await self.odd.query(sample_filter, project_id=self.project_id)
        ).to_dict()
        # print(dashboard)
        participants_signed_not_consented = dashboard.get(
            'participants_signed_not_consented'
        )

        # Check that participants_signed_not_consented is not empty and is a dict
        assert participants_signed_not_consented
        assert isinstance(participants_signed_not_consented, list)

        # Check that the number of participants in the list is correct
        participants_filtered: list[ParticipantUpsert] = []
        for participant in self.participants_external_objects:
            assert isinstance(participant.meta, dict)
            if not participant.meta.get('consent'):
                participants_filtered.append(participant)

                # Check that the participant id is in the dict
                assert participant.id in participants_signed_not_consented

        assert len(participants_signed_not_consented) == len(participants_filtered)