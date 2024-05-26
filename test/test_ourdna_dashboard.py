from datetime import datetime
from test.testbase import DbIsolatedTest, run_as_sync

from db.python.layers.ourdna.dashboard import OurDnaDashboardLayer
from db.python.layers.sample import SampleLayer
from db.python.tables.sample import SampleFilter
from db.python.utils import GenericFilter
from models.models import SampleUpsert, SampleUpsertInternal


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

        samples_data = [
            {
                'external_id': 'Test01',
                'meta': {
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
                'type': 'blood',
                'active': True,
            },
            {
                'external_id': 'Test02',
                'meta': {
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
                'type': 'blood',
                'active': True,
            },
            {
                'external_id': 'Test03',
                'meta': {
                    'collection-time': '2022-07-03 13:28:00',
                    'processing-site': 'Garvan',
                    'process-start-time': '2022-07-03 16:28:00',
                    'process-end-time': '2022-07-03 19:28:00',
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
                'type': 'blood',
                'active': True,
            },
        ]

        sample_names = ['sample_one', 'sample_two', 'sample_three']

        self.sample_external_objects: list[SampleUpsert] = []

        for sample_name, sample_data in zip(sample_names, samples_data):
            assert isinstance(sample_data['meta'], dict)
            assert isinstance(sample_data['active'], bool)
            sample = await self.sl.upsert_sample(
                SampleUpsertInternal(
                    external_id=str(sample_data['external_id']),
                    meta=sample_data['meta'],
                    type=str(sample_data['type']),
                    active=sample_data['active'],
                )
            )
            sample_external = sample.to_external()
            self.sample_external_objects.append(sample_external)
            setattr(self, f'test_{sample_name}', sample_external)
            # NOTE: We probably don't need to set the sample_external_objects as attributes of the test class
            assert sample.id

        self.number_of_samples = len(samples_data)

    @run_as_sync
    async def test_get_dashboard(self):
        """Test get_dashboard"""
        sample_filter = SampleFilter(project=GenericFilter(eq=self.project_id))
        dashboard = await self.odd.query(sample_filter)
        print(dashboard)

    @run_as_sync
    async def test_collection_to_process_end_time(self):
        """I want to know how long it took between blood collection and sample processing"""
        sample_filter = SampleFilter(project=GenericFilter(eq=self.project_id))
        dashboard = await self.odd.query(sample_filter)
        collection_to_process_end_time = dashboard['collection_to_process_end_time']

        # Check that collection_to_process_end_time is not empty and is a dict
        assert collection_to_process_end_time
        assert isinstance(collection_to_process_end_time, dict)

        # Check the number of samples in the cohort
        assert len(collection_to_process_end_time.keys()) == self.number_of_samples

        # Check that the ids of the samples are the keys of the dict
        sample_ids = [sample.id for sample in self.sample_external_objects]
        assert set(collection_to_process_end_time.keys()) == set(sample_ids)

        # Check that the values are the difference between the process end time and the collection time
        for sample in self.sample_external_objects:
            assert isinstance(sample.meta, dict)

            collection_time = str_to_datetime(sample.meta['collection-time'])
            process_end_time = str_to_datetime(sample.meta['process-end-time'])
            time_difference = (
                process_end_time - collection_time
            ).total_seconds()  # Difference in seconds

            assert collection_to_process_end_time[sample.id] == time_difference

        for _sample_id, time_diff in collection_to_process_end_time.items():
            assert (
                time_diff > 0
            )  # NOTE: Should we actually check this explicitly in the code instead?
            assert isinstance(time_diff, int)

    @run_as_sync
    async def test_collection_to_process_end_time_24h(self):
        """I want to know which samples took more than 24 hours between blood collection and sample processing completion"""
        sample_filter = SampleFilter(project=GenericFilter(eq=self.project_id))
        dashboard = await self.odd.query(sample_filter)
        print(dashboard)

        # TODO: Add assertions YP

    @run_as_sync
    async def test_processing_times_per_site(self):
        """I want to know what the sample processing times were for samples at each designated site"""
        sample_filter = SampleFilter(project=GenericFilter(eq=self.project_id))
        dashboard = await self.odd.query(sample_filter)
        print(dashboard)

        # TODO: Add assertions VB

    @run_as_sync
    async def test_total_samples_by_collection_event_name(self):
        """I want to know how many samples were collected from walk-ins vs during events or scheduled activities"""
        sample_filter = SampleFilter(project=GenericFilter(eq=self.project_id))
        dashboard = await self.odd.query(sample_filter)
        print(dashboard)

        # TODO: Add assertions YP

    @run_as_sync
    async def test_samples_lost_after_collection(self):
        """I need to know how many samples have been lost, EG: participants have been consented, blood collected, not processed"""
        sample_filter = SampleFilter(project=GenericFilter(eq=self.project_id))
        dashboard = await self.odd.query(sample_filter)
        print(dashboard)

        # TODO: Add assertions VB

    @run_as_sync
    async def test_samples_more_than_1ug_dna(self):
        """I want to generate a list of samples containing more than 1 ug of DNA to prioritise them for long-read sequencing applications"""
        sample_filter = SampleFilter(project=GenericFilter(eq=self.project_id))
        dashboard = await self.odd.query(sample_filter)
        print(dashboard)

        # TODO: Add assertions YP

    @run_as_sync
    async def participants_consented_not_collected(self):
        """I want to know how many people who have consented and NOT given blood"""
        sample_filter = SampleFilter(project=GenericFilter(eq=self.project_id))
        dashboard = await self.odd.query(sample_filter)
        print(dashboard)

        # TODO: Add assertions VB

    @run_as_sync
    async def test_participants_signed_not_consented(self):
        """I want to know how many people have signed up but not consented"""
        sample_filter = SampleFilter(project=GenericFilter(eq=self.project_id))
        dashboard = await self.odd.query(sample_filter)
        print(dashboard)

        # TODO: Add assertions YP
