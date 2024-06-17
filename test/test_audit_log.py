from test.testbase import DbIsolatedTest, run_as_sync

from db.python.layers import AuditLogLayer, SampleLayer
from models.models import PRIMARY_EXTERNAL_ORG, SampleUpsertInternal


class TestChangelog(DbIsolatedTest):
    """Test audit_log"""

    @run_as_sync
    async def setUp(self):
        super().setUp()
        self.allayer = AuditLogLayer(self.connection)

    @run_as_sync
    async def test_insert_sample(self):
        """
        Test inserting a sample, and check that the audit_log_id reflects the current
        change
        """
        slayer = SampleLayer(self.connection)
        sample = await slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                active=True,
                meta={'meta': 'meta ;)'},
            )
        )

        sample_cl_id = await self.connection.connection.fetch_val(
            'SELECT audit_log_id FROM sample WHERE id = :sid', {'sid': sample.id}
        )

        self.assertEqual(await self.audit_log_id(), sample_cl_id)

        result = await self.allayer.get_for_ids([sample_cl_id])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].id, sample_cl_id)
