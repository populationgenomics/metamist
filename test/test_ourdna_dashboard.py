from test.testbase import DbIsolatedTest, run_as_sync

from db.python.layers.ourdna.dashboard import OurDnaDashboardLayer
from db.python.layers.sample import SampleLayer
from db.python.tables.sample import SampleFilter
from db.python.utils import GenericFilter
from models.models import SampleUpsertInternal


class OurDNADashboardTest(DbIsolatedTest):
    """Test sample class"""

    @run_as_sync
    async def setUp(self) -> None:
        # don't need to await because it's tagged @run_as_sync
        super().setUp()
        self.odd = OurDnaDashboardLayer(self.connection)

        self.sl = SampleLayer(self.connection)

        # TODO: Add additional samples to test

        self.test_sample_one = await self.sl.upsert_sample(
            SampleUpsertInternal(
                external_id='Test01',
                type='blood',
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
                active=True,
            )
        )

        self.test_sample_two = await self.sl.upsert_sample(
            SampleUpsertInternal(
                external_id='Test02',
                type='blood',
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
                active=True,
            )
        )

        self.test_sample_three = await self.sl.upsert_sample(
            SampleUpsertInternal(
                external_id='Test03',
                type='blood',
                meta={
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
                active=True,
            )
        )

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
        print(dashboard)

        # TODO: Add assertions VB

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
