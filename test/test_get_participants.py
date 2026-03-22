import pytest

from db.python.connect import Connection
from db.python.layers.participant import ParticipantLayer
from models.models import PRIMARY_EXTERNAL_ORG, ParticipantUpsertInternal


class TestParticipant:
    """Test getting participants"""

    @pytest.fixture(autouse=True)
    async def setup(self, connection_with_project: Connection):
        self.ex01 = {PRIMARY_EXTERNAL_ORG: 'EX01', 'other': 'OTHER1'}
        self.ex02 = {PRIMARY_EXTERNAL_ORG: 'EX02'}

        pl = ParticipantLayer(connection_with_project)
        await pl.upsert_participants(
            [
                ParticipantUpsertInternal(
                    external_ids=self.ex01,
                    reported_sex=2,
                    karyotype='XX',
                    meta={'field': 1},
                ),
                ParticipantUpsertInternal(
                    external_ids=self.ex02,
                    reported_sex=1,
                    karyotype='XY',
                    meta={'field': 2},
                ),
            ]
        )

    @pytest.mark.asyncio
    async def test_get_all_participants(self, connection_with_project: Connection):
        """Test getting all participants"""
        pl = ParticipantLayer(connection_with_project)
        ps = await pl.get_participants(project=connection_with_project.project_id)

        assert len(ps) == 2

        assert self.ex01 == ps[0].external_ids
        assert ps[0].meta['field'] == 1
        assert ps[0].karyotype == 'XX'

        assert self.ex02 == ps[1].external_ids

    @pytest.mark.asyncio
    async def test_get_participant_by_eid(self, connection_with_project: Connection):
        """Test to see what's in the database"""
        pl = ParticipantLayer(connection_with_project)
        ps = await pl.get_participants(
            project=connection_with_project.project_id,
            external_participant_ids=['EX02'],
        )

        assert len(ps) == 1

        assert self.ex02 == ps[0].external_ids
        assert ps[0].meta['field'] == 2
        assert ps[0].karyotype == 'XY'
