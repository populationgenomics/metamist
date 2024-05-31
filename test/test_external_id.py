from test.testbase import DbIsolatedTest, run_as_sync

from pymysql.err import IntegrityError

from db.python.layers import ParticipantLayer, SampleLayer
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    ParticipantUpsertInternal,
    SampleUpsertInternal,
)


class TestParticipant(DbIsolatedTest):
    """Test participant external ids"""

    @run_as_sync
    async def setUp(self):
        super().setUp()
        self.player = ParticipantLayer(self.connection)

        self.p1 = await self.player.upsert_participant(
            ParticipantUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'P1', 'CONTROL': '86', 'KAOS': 'shoe'},
            )
        )
        self.p1_external_ids = {k.lower(): v for k, v in self.p1.external_ids.items()}

        self.p2 = await self.player.upsert_participant(
            ParticipantUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'P2'},
            )
        )

    async def insert(self, participant_id, org, external_id):
        """Directly insert into participant_external_id table"""
        query = """
        INSERT INTO participant_external_id (project, participant_id, name, external_id)
        VALUES (:project, :id, :org, :external_id)
        """
        values = {'project': self.project_id, 'id': participant_id, 'org': org, 'external_id': external_id}
        await self.connection.connection.execute(query, values)

    @run_as_sync
    async def test_constraints(self):
        """Verify that database constraints prevent duplicate external_ids"""
        # Can't have two primary eids
        with self.assertRaises(IntegrityError):
            await self.insert(self.p1.id, PRIMARY_EXTERNAL_ORG, 'P86')

        # Can't have two eids in the same external organisation
        with self.assertRaises(IntegrityError):
            await self.insert(self.p1.id, 'CONTROL', 'Maxwell')

        # Can have eids in lots of organisations, even if the eid duplicates one in a different org
        await self.insert(self.p1.id, 'OTHER1', 'Maxwell')
        await self.insert(self.p1.id, 'OTHER2', '86')

        # Another participant can't have the same eid
        with self.assertRaises(IntegrityError):
            await self.insert(self.p2.id, 'CONTROL', '86')

        # But it can have its own eid
        await self.insert(self.p2.id, 'CONTROL', '99')

    @run_as_sync
    async def test_insert(self):
        """Test inserting new participants with various external_id combinations"""
        with self.assertRaises(ValueError):
            _ = await self.player.upsert_participant(
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: None},
                    )
                )

        result = await self.player.upsert_participant(
            ParticipantUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'P10', 'A': 'A1', 'B': None, 'C': 'A1'},
                )
            )
        participants = await self.player.get_participants_by_ids([result.id])
        self.assertDictEqual(participants[0].external_ids, {PRIMARY_EXTERNAL_ORG: 'P10', 'a': 'A1', 'c': 'A1'})

    @run_as_sync
    async def test_update(self):
        """Test updating existing participants with various external_id combinations"""
        with self.assertRaises(ValueError):
            _ = await self.player.upsert_participant(
                ParticipantUpsertInternal(
                    id=self.p1.id,
                    external_ids={PRIMARY_EXTERNAL_ORG: None},
                    )
                )

        result = await self.player.upsert_participant(
            ParticipantUpsertInternal(
                id=self.p1.id,
                external_ids={PRIMARY_EXTERNAL_ORG: 'P1B', 'CONTROL': '86B', 'KAOS': None, 'B': None, 'C': 'A1'},
                )
            )
        participants = await self.player.get_participants_by_ids([result.id])
        self.assertDictEqual(participants[0].external_ids, {PRIMARY_EXTERNAL_ORG: 'P1B', 'control': '86B', 'c': 'A1'})

    @run_as_sync
    async def test_fill_in_missing(self):
        """Exercise fill_in_missing_participants() method"""
        slayer = SampleLayer(self.connection)

        s1 = await slayer.upsert_sample(
            SampleUpsertInternal(external_ids={PRIMARY_EXTERNAL_ORG: 'E1'}, participant_id=self.p1.id),
        )
        sa = await slayer.upsert_sample(SampleUpsertInternal(external_ids={PRIMARY_EXTERNAL_ORG: 'EA', 'foo': 'FA'}))
        sb = await slayer.upsert_sample(SampleUpsertInternal(external_ids={PRIMARY_EXTERNAL_ORG: 'EB', 'foo': 'FB'}))

        result = await self.player.fill_in_missing_participants()
        self.assertEqual(result, 'Updated 2 records')

        samples = await slayer.get_samples_by(sample_ids=[s1.id, sa.id, sb.id])

        participants = await self.player.get_participants_by_ids([s.participant_id for s in samples])
        p_map = {p.id: p for p in participants}

        for s in samples:
            expected_eids = self.p1_external_ids if s.id == s1.id else s.external_ids
            self.assertEqual(p_map[s.participant_id].external_ids, expected_eids)


class TestSample(DbIsolatedTest):
    """Test sample external ids"""

    @run_as_sync
    async def setUp(self):
        super().setUp()
        self.slayer = SampleLayer(self.connection)

        self.s1 = await self.slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'S1', 'CONTROL': '86', 'KAOS': 'shoe'},
            )
        )

        self.s2 = await self.slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'S2'},
            )
        )

    async def insert(self, sample_id, org, external_id):
        """Directly insert into sample_external_id table"""
        query = """
        INSERT INTO sample_external_id (project, sample_id, name, external_id)
        VALUES (:project, :id, :org, :external_id)
        """
        values = {'project': self.project_id, 'id': sample_id, 'org': org, 'external_id': external_id}
        await self.connection.connection.execute(query, values)

    @run_as_sync
    async def test_constraints(self):
        """Verify that database constraints prevent duplicate external_ids"""
        # Can't have two primary eids
        with self.assertRaises(IntegrityError):
            await self.insert(self.s1.id, PRIMARY_EXTERNAL_ORG, 'S86')

        # Can't have two eids in the same external organisation
        with self.assertRaises(IntegrityError):
            await self.insert(self.s1.id, 'CONTROL', 'Maxwell')

        # Can have eids in lots of organisations, even if the eid duplicates one in a different org
        await self.insert(self.s1.id, 'OTHER1', 'Maxwell')
        await self.insert(self.s1.id, 'OTHER2', '86')

        # Another sample can't have the same eid
        with self.assertRaises(IntegrityError):
            await self.insert(self.s2.id, 'CONTROL', '86')

        # But it can have its own eid
        await self.insert(self.s2.id, 'CONTROL', '99')

    @run_as_sync
    async def test_insert(self):
        """Test inserting new samples with various external_id combinations"""
        with self.assertRaises(ValueError):
            _ = await self.slayer.upsert_sample(
                SampleUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: None},
                    )
                )

        result = await self.slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'S10', 'A': 'A1', 'B': None, 'C': 'A1'},
                )
            )
        sample = await self.slayer.get_sample_by_id(result.id)
        self.assertDictEqual(sample.external_ids, {PRIMARY_EXTERNAL_ORG: 'S10', 'a': 'A1', 'c': 'A1'})

    @run_as_sync
    async def test_update(self):
        """Test updating existing samples with various external_id combinations"""
        with self.assertRaises(ValueError):
            _ = await self.slayer.upsert_sample(
                SampleUpsertInternal(
                    id=self.s1.id,
                    external_ids={PRIMARY_EXTERNAL_ORG: None},
                    )
                )

        result = await self.slayer.upsert_sample(
            SampleUpsertInternal(
                id=self.s1.id,
                external_ids={PRIMARY_EXTERNAL_ORG: 'S1B', 'CONTROL': '86B', 'KAOS': None, 'B': None, 'C': 'A1'},
                )
            )
        sample = await self.slayer.get_sample_by_id(result.id)
        self.assertDictEqual(sample.external_ids, {PRIMARY_EXTERNAL_ORG: 'S1B', 'control': '86B', 'c': 'A1'})
