import pytest

from db.python.connect import Connection
from db.python.layers.sample import SampleLayer
from models.models import PRIMARY_EXTERNAL_ORG, SampleUpsertInternal


class TestChangelog:
    """Test audit_log"""

    @pytest.mark.asyncio
    async def test_insert_sample(self, connection_with_project: Connection):
        """
        Test inserting a sample, and check that the audit_log_id reflects the current
        change
        """
        slayer = SampleLayer(connection_with_project)
        sample = await slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                active=True,
                meta={'meta': 'meta ;)'},
            )
        )

        sample_cl_id = (
            await connection_with_project.execute_must_fetch_one(
                t'SELECT audit_log_id FROM sample WHERE id = {sample.id}'
            )
        )['audit_log_id']

        assert await connection_with_project.audit_log_id() == sample_cl_id
