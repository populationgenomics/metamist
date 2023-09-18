from test.testbase import DbIsolatedTest, run_as_sync

from db.python.layers.family import FamilyLayer
from db.python.layers.participant import ParticipantLayer
from models.models.participant import ParticipantUpsertInternal


class TestPedigree(DbIsolatedTest):
    """Pedigree testing methods"""

    @run_as_sync
    async def test_import_get_pedigree(self):
        """Test import + get pedigree"""
        fl = FamilyLayer(self.connection)

        rows: list[list[str]] = [
            ['FAM01', 'EX01_father', '', '', '1', '1'],
            ['FAM01', 'EX01_mother', '', '', '2', '1'],
            ['FAM01', 'EX01_subject', 'EX01_father', 'EX01_mother', '1', '2'],
        ]

        await fl.import_pedigree(
            header=None, rows=rows, create_missing_participants=True
        )

        pedigree_dicts = await fl.get_pedigree(
            project=self.connection.project,
            replace_with_participant_external_ids=True,
            replace_with_family_external_ids=True,
        )

        by_key = {r['individual_id']: r for r in pedigree_dicts}

        self.assertEqual(3, len(pedigree_dicts))
        father = by_key['EX01_father']
        mother = by_key['EX01_mother']
        subject = by_key['EX01_subject']

        self.assertIsNone(father['paternal_id'])
        self.assertIsNone(mother['paternal_id'])
        self.assertEqual('EX01_father', subject['paternal_id'])
        self.assertEqual('EX01_mother', subject['maternal_id'])

    @run_as_sync
    async def test_pedigree_without_family(self):
        """
        Test getting pedigree where participants do not belong to a family
        """
        pl = ParticipantLayer(self.connection)
        fl = FamilyLayer(self.connection)

        await pl.upsert_participant(
            ParticipantUpsertInternal(
                external_id='EX01',
                reported_sex=1,
            )
        )
        await pl.upsert_participant(
            ParticipantUpsertInternal(external_id='EX02', reported_sex=None)
        )

        rows = await fl.get_pedigree(
            project=self.connection.project,
            include_participants_not_in_families=True,
            replace_with_participant_external_ids=True,
        )

        by_id = {r['individual_id']: r for r in rows}
        self.assertEqual(2, len(rows))
        self.assertEqual(1, by_id['EX01']['sex'])
        self.assertIsNone(by_id['EX02']['sex'])
