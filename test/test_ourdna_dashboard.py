from test.testbase import DbIsolatedTest, run_as_sync

from db.python.layers.analysis import AnalysisLayer
from db.python.layers.assay import AssayLayer
from db.python.layers.ourdna.dashboard import OurDnaDashboardLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
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
        self.sgl = SequencingGroupLayer(self.connection)
        self.asl = AssayLayer(self.connection)
        self.al = AnalysisLayer(self.connection)

        sample = await self.sl.upsert_sample(
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
        self.sample_id = sample.id

    @run_as_sync
    async def test_get_dashboard(self):
        """Test get_dashboard"""
        sample_filter = SampleFilter(project=GenericFilter(eq=self.project_id))
        dashboard = await self.odd.query(sample_filter)
        print(dashboard)
