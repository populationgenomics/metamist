from test.testbase import DbIsolatedTest, run_as_sync

from pymysql.err import IntegrityError

from db.python.filters import GenericFilter
from db.python.layers import FamilyLayer, ParticipantLayer, SampleLayer
from db.python.tables.family import FamilyFilter, FamilyTable
from db.python.utils import NotFoundError
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    ParticipantUpsertInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)


class TestParticipant(DbIsolatedTest):
    """Test participant external ids"""

    @run_as_sync
    async def setUp(self):
        super().setUp()
        self.player = ParticipantLayer(self.connection)

        self.p1 = await self.player.upsert_participant(
            ParticipantUpsertInternal(
                external_ids={
                    PRIMARY_EXTERNAL_ORG: 'P1',
                    'CONTROL': '86',
                    'KAOS': 'shoe',
                },
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
        INSERT INTO participant_external_id (project, participant_id, name, external_id, audit_log_id)
        VALUES (:project, :id, :org, :external_id, :audit_log_id)
        """
        values = {
            'project': self.project_id,
            'id': participant_id,
            'org': org,
            'external_id': external_id,
            'audit_log_id': await self.audit_log_id(),
        }
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

        # Can have eids in lots of organisations, but not if the eid duplicates one in a different org
        await self.insert(self.p1.id, 'OTHER1', 'Maxwell')
        with self.assertRaises(IntegrityError):
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

        with self.assertRaises(ValueError):
            _ = await self.player.upsert_participant(
                ParticipantUpsertInternal(external_ids={'OTHER': 'P1'})
            )

        result = await self.player.upsert_participant(
            ParticipantUpsertInternal(
                external_ids={
                    PRIMARY_EXTERNAL_ORG: 'P10',
                    'A': 'A1',
                    'B': None,
                    'C': 'C1',
                },
            )
        )
        participants = await self.player.get_participants_by_ids([result.id])
        self.assertDictEqual(
            participants[0].external_ids,
            {PRIMARY_EXTERNAL_ORG: 'P10', 'a': 'A1', 'c': 'C1'},
        )

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
                external_ids={
                    PRIMARY_EXTERNAL_ORG: 'P1B',
                    'CONTROL': '86B',
                    'KAOS': None,
                    'B': None,
                    'C': 'A1',
                },
            )
        )
        participants = await self.player.get_participants_by_ids([result.id])
        self.assertDictEqual(
            participants[0].external_ids,
            {PRIMARY_EXTERNAL_ORG: 'P1B', 'control': '86B', 'c': 'A1'},
        )

    @run_as_sync
    async def test_fill_in_missing(self):
        """Exercise fill_in_missing_participants() method"""
        slayer = SampleLayer(self.connection)

        s1 = await slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'E1'}, participant_id=self.p1.id
            ),
        )
        sa = await slayer.upsert_sample(
            SampleUpsertInternal(external_ids={PRIMARY_EXTERNAL_ORG: 'EA', 'foo': 'FA'})
        )
        sb = await slayer.upsert_sample(
            SampleUpsertInternal(external_ids={PRIMARY_EXTERNAL_ORG: 'EB', 'foo': 'FB'})
        )

        result = await self.player.fill_in_missing_participants()
        self.assertEqual(result, 'Updated 2 records')

        samples = await slayer.get_samples_by(sample_ids=[s1.id, sa.id, sb.id])

        participants = await self.player.get_participants_by_ids(
            [s.participant_id for s in samples]
        )
        p_map = {p.id: p for p in participants}

        for s in samples:
            expected_eids = self.p1_external_ids if s.id == s1.id else s.external_ids
            self.assertEqual(p_map[s.participant_id].external_ids, expected_eids)

    @run_as_sync
    async def test_get_by_families(self):
        """Exercise get_participants_by_families() method"""
        flayer = FamilyLayer(self.connection)
        fid = await flayer.create_family(external_ids={'org': 'Jones'})

        child = await self.player.upsert_participant(
            ParticipantUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'P20', 'd': 'D20'}
            ),
        )

        await self.player.add_participant_to_family(
            family_id=fid,
            participant_id=child.id,
            paternal_id=self.p1.id,
            maternal_id=self.p2.id,
            affected=2,
        )

        result = await self.player.get_participants_by_families([fid])
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[fid]), 1)
        self.assertDictEqual(
            result[fid][0].external_ids, {PRIMARY_EXTERNAL_ORG: 'P20', 'd': 'D20'}
        )

    @run_as_sync
    async def test_get_families_by_participants(self):
        """Exercise FamilyLayer's get_families_by_participants() method"""
        flayer = FamilyLayer(self.connection)
        fid = await flayer.create_family(
            external_ids={PRIMARY_EXTERNAL_ORG: 'Smith'},
            description='Blacksmiths',
            coded_phenotype='burnt',
        )

        child = await self.player.upsert_participant(
            ParticipantUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'P20', 'd': 'D20'}
            ),
        )

        await self.player.add_participant_to_family(
            family_id=fid,
            participant_id=child.id,
            paternal_id=self.p1.id,
            maternal_id=self.p2.id,
            affected=2,
        )

        result = await flayer.get_families_by_participants([child.id, self.p1.id])
        self.assertEqual(len(result), 1)
        self.assertEqual(len(result[child.id]), 1)
        self.assertEqual(result[child.id][0].description, 'Blacksmiths')

    @run_as_sync
    async def test_update_many(self):
        """Exercise update_many_participant_external_ids() method"""
        result = await self.player.update_many_participant_external_ids(
            {self.p1.id: 'P1B', self.p2.id: 'P2B'}
        )
        self.assertTrue(result)

        participants = await self.player.get_participants_by_ids(
            [self.p1.id, self.p2.id]
        )
        p_map = {p.id: p for p in participants}
        outp1 = p_map[self.p1.id]
        outp2 = p_map[self.p2.id]
        self.assertDictEqual(
            outp1.external_ids,
            {PRIMARY_EXTERNAL_ORG: 'P1B', 'control': '86', 'kaos': 'shoe'},
        )
        self.assertDictEqual(outp2.external_ids, {PRIMARY_EXTERNAL_ORG: 'P2B'})

    @run_as_sync
    async def test_get_etoi_map(self):
        """Exercise get_external_participant_id_to_internal_sequencing_group_id_map() method"""
        slayer = SampleLayer(self.connection)

        _ = await slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'SE1'}, participant_id=self.p1.id
            ),
        )

        s2 = await slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'SE2', 'other': 'SO1'},
                participant_id=self.p1.id,
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        assays=[],
                    ),
                ],
            ),
        )

        result = await self.player.get_external_participant_id_to_internal_sequencing_group_id_map(
            self.project_id
        )
        self.assertEqual(len(result), 3)
        for eid, sgid in result:
            self.assertIn(eid, self.p1.external_ids.values())
            self.assertEqual(sgid, s2.sequencing_groups[0].id)


class TestSample(DbIsolatedTest):
    """Test sample external ids"""

    @run_as_sync
    async def setUp(self):
        super().setUp()
        self.slayer = SampleLayer(self.connection)

        self.s1 = await self.slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={
                    PRIMARY_EXTERNAL_ORG: 'S1',
                    'CONTROL': '86',
                    'KAOS': 'shoe',
                },
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
        INSERT INTO sample_external_id (project, sample_id, name, external_id, audit_log_id)
        VALUES (:project, :id, :org, :external_id, :audit_log_id)
        """
        values = {
            'project': self.project_id,
            'id': sample_id,
            'org': org,
            'external_id': external_id,
            'audit_log_id': await self.audit_log_id(),
        }
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

        # Can have eids in lots of organisations, but not if the eid duplicates one in a different org
        await self.insert(self.s1.id, 'OTHER1', 'Maxwell')
        with self.assertRaises(IntegrityError):
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

        with self.assertRaises(ValueError):
            _ = await self.slayer.upsert_sample(
                SampleUpsertInternal(external_ids={'OTHER': 'S1'})
            )

        result = await self.slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={
                    PRIMARY_EXTERNAL_ORG: 'S10',
                    'A': 'A1',
                    'B': None,
                    'C': 'C1',
                },
            )
        )
        sample = await self.slayer.get_sample_by_id(result.id)
        self.assertDictEqual(
            sample.external_ids, {PRIMARY_EXTERNAL_ORG: 'S10', 'a': 'A1', 'c': 'C1'}
        )

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
                external_ids={
                    PRIMARY_EXTERNAL_ORG: 'S1B',
                    'CONTROL': '86B',
                    'KAOS': None,
                    'B': None,
                    'C': 'A1',
                },
            )
        )
        sample = await self.slayer.get_sample_by_id(result.id)
        self.assertDictEqual(
            sample.external_ids,
            {PRIMARY_EXTERNAL_ORG: 'S1B', 'control': '86B', 'c': 'A1'},
        )

    @run_as_sync
    async def test_get_single(self):
        """Exercise get_single_by_external_id() method"""
        with self.assertRaises(NotFoundError):
            _ = await self.slayer.get_single_by_external_id(
                'non-existent', self.project_id
            )

        result = await self.slayer.get_single_by_external_id('86', self.project_id)
        self.assertEqual(result.id, self.s1.id)

        result = await self.slayer.get_single_by_external_id('S2', self.project_id)
        self.assertEqual(result.id, self.s2.id)

    @run_as_sync
    async def test_get_internal_to_external(self):
        """Exercise get_internal_to_external_sample_id_map() method"""
        result = await self.slayer.get_internal_to_external_sample_id_map(
            [self.s1.id, self.s2.id]
        )
        self.assertDictEqual(result, {self.s1.id: 'S1', self.s2.id: 'S2'})

    @run_as_sync
    async def test_get_all(self):
        """Exercise get_all_sample_id_map_by_internal_ids() method"""
        result = await self.slayer.get_all_sample_id_map_by_internal_ids(
            self.project_id
        )
        self.assertDictEqual(result, {self.s1.id: 'S1', self.s2.id: 'S2'})

    @run_as_sync
    async def test_get_history(self):
        """Exercise get_history_of_sample() method"""
        # First create some history
        await self.slayer.upsert_sample(
            SampleUpsertInternal(id=self.s1.id, meta={'foo': 'bar'})
        )

        await self.slayer.upsert_sample(
            SampleUpsertInternal(
                id=self.s1.id,
                external_ids={'fruit': 'apple'},
                meta={'fruit': 'banana'},
            )
        )

        sample = await self.slayer.get_sample_by_id(self.s1.id)

        result = await self.slayer.get_history_of_sample(self.s1.id)
        self.assertEqual(len(result), 3)
        self.assertDictEqual(result[0].meta, {})
        self.assertDictEqual(result[1].meta, {'foo': 'bar'})
        self.assertDictEqual(result[2].meta, {'foo': 'bar', 'fruit': 'banana'})
        self.assertDictEqual(result[2].meta, sample.meta)


class TestFamily(DbIsolatedTest):
    """Test family external ids"""

    @run_as_sync
    async def setUp(self):
        super().setUp()
        self.flayer = FamilyLayer(self.connection)

    @run_as_sync
    async def test_create_update(self):
        """Exercise create_family() and update_family() methods"""
        family_id = await self.flayer.create_family(
            external_ids={PRIMARY_EXTERNAL_ORG: 'Smith'},
            description='Blacksmiths',
            coded_phenotype='burnt',
        )

        family = await self.flayer.get_family_by_internal_id(family_id)
        self.assertDictEqual(family.external_ids, {PRIMARY_EXTERNAL_ORG: 'Smith'})
        self.assertEqual(family.description, 'Blacksmiths')
        self.assertEqual(family.coded_phenotype, 'burnt')

        await self.flayer.update_family(family_id, external_ids={'foo': 'bar'})
        family = await self.flayer.get_family_by_internal_id(family_id)
        self.assertEqual(family.external_ids['foo'], 'bar')

        await self.flayer.update_family(family_id, external_ids={'foo': 'baz'})
        family = await self.flayer.get_family_by_internal_id(family_id)
        self.assertEqual(family.external_ids['foo'], 'baz')

        await self.flayer.update_family(family_id, external_ids={'foo': None})
        family = await self.flayer.get_family_by_internal_id(family_id)
        self.assertDictEqual(family.external_ids, {PRIMARY_EXTERNAL_ORG: 'Smith'})

        await self.flayer.update_family(family_id, description='Goldsmiths')
        family = await self.flayer.get_family_by_internal_id(family_id)
        self.assertEqual(family.description, 'Goldsmiths')
        self.assertEqual(family.coded_phenotype, 'burnt')

        await self.flayer.update_family(family_id, coded_phenotype='gilt')
        family = await self.flayer.get_family_by_internal_id(family_id)
        self.assertEqual(family.description, 'Goldsmiths')
        self.assertEqual(family.coded_phenotype, 'gilt')

    @run_as_sync
    async def test_bad_query(self):
        """Exercise invalid query() usage"""
        with self.assertRaises(ValueError):
            await self.flayer.query(FamilyFilter())

    @run_as_sync
    async def test_none_by_participants(self):
        """Exercise get_families_by_participants() method"""
        result = await self.flayer.get_families_by_participants([])
        self.assertDictEqual(result, {})

    @run_as_sync
    async def test_import_families(self):
        """Exercise import_families() method"""
        await self.flayer.import_families(
            ['familyid', 'description', 'phenotype'],
            [
                ['Smith', 'Blacksmiths', 'burnt'],
                ['Jones', 'From Wales', 'sings well'],
                ['Taylor', 'Post Norman', 'sews'],
            ],
        )

        result = await self.flayer.query(
            FamilyFilter(project=GenericFilter(eq=self.project_id))
        )
        self.assertEqual(len(result), 3)
        family = {f.external_ids[PRIMARY_EXTERNAL_ORG]: f for f in result}
        self.assertEqual(family['Smith'].description, 'Blacksmiths')
        self.assertEqual(family['Smith'].coded_phenotype, 'burnt')
        self.assertEqual(family['Jones'].description, 'From Wales')
        self.assertEqual(family['Jones'].coded_phenotype, 'sings well')
        self.assertEqual(family['Taylor'].description, 'Post Norman')
        self.assertEqual(family['Taylor'].coded_phenotype, 'sews')

        await self.flayer.import_families(
            ['familyid', 'description', 'phenotype'],
            [
                ['Smith', 'Goldsmiths actually', 'gilt'],
                ['Brown', 'From Jamaica', 'brunette'],
            ],
        )

        result = await self.flayer.query(
            FamilyFilter(project=GenericFilter(eq=self.project_id))
        )
        self.assertEqual(len(result), 4)
        family = {f.external_ids[PRIMARY_EXTERNAL_ORG]: f for f in result}
        self.assertEqual(family['Smith'].description, 'Goldsmiths actually')
        self.assertEqual(family['Smith'].coded_phenotype, 'gilt')
        self.assertEqual(family['Brown'].description, 'From Jamaica')
        self.assertEqual(family['Brown'].coded_phenotype, 'brunette')
        self.assertEqual(family['Jones'].description, 'From Wales')
        self.assertEqual(family['Jones'].coded_phenotype, 'sings well')
        self.assertEqual(family['Taylor'].description, 'Post Norman')
        self.assertEqual(family['Taylor'].coded_phenotype, 'sews')

    @run_as_sync
    async def test_direct_get_id_map(self):
        """Exercise the table's get_id_map_by_internal_ids() method"""
        ftable = FamilyTable(self.connection)

        result = await ftable.get_id_map_by_internal_ids([])
        self.assertDictEqual(result, {})

        result = await ftable.get_id_map_by_internal_ids([42], allow_missing=True)
        self.assertDictEqual(result, {})

        with self.assertRaises(NotFoundError):
            _ = await ftable.get_id_map_by_internal_ids([42])
