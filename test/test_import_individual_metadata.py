from test.testbase import DbIsolatedTest, run_as_sync
from db.python.layers.participant import ParticipantLayer


class TestImportIndividualMetadata(DbIsolatedTest):
    """Test importing individual metadata"""

    @run_as_sync
    async def test_import_many_hpo_terms(self):
        """Test import hpo terms from many columns"""
        pl = ParticipantLayer(self.connection)

        await pl.create_participant('TP01')

        headers = [
            'Individual ID',
            'HPO Term 1',
            'HPO Term 2',
            'HPO Term 3',
            'HPO Term 20',
        ]
        rows = [['TP01', 'HP:0000001', 'HP:0000002', 'HP:0000003', 'HP:0000004']]

        await pl.generic_individual_metadata_importer(headers, rows)

        rows = list(
            await self.connection.connection.fetch_all(
                'SELECT participant_id, description, value FROM participant_phenotypes'
            )
        )

        self.assertEqual(1, len(rows))
        self.assertEqual('HPO Terms (present)', rows[0]['description'])
        self.assertEqual(
            '"HP:0000001,HP:0000002,HP:0000003,HP:0000004"', rows[0]['value']
        )

    @run_as_sync
    async def test_import_basic_metadata(self):
        """Test basic data for 2 participants and 2 columns"""
        pl = ParticipantLayer(self.connection)

        await pl.create_participant('TP01')
        await pl.create_participant('TP02')

        headers = ['Individual ID', 'HPO Term 20', 'Age of Onset']
        rows = [
            ['TP01', 'HP:0000020', 'Congenital'],
            ['TP02', 'HP:00000021; HP:023', 'Infantile'],
        ]

        await pl.generic_individual_metadata_importer(headers, rows)

        rows = list(
            await self.connection.connection.fetch_all(
                'SELECT participant_id, description, value FROM participant_phenotypes'
            )
        )

        self.assertEqual(4, len(rows))

        first_p_rows = rows[:2]
        second_p_rows = rows[2:]

        self.assertEqual('Age of Onset', first_p_rows[0]['description'])
        self.assertEqual('"Congenital onset"', first_p_rows[0]['value'])
        self.assertEqual('HPO Terms (present)', first_p_rows[1]['description'])
        self.assertEqual('"HP:0000020"', first_p_rows[1]['value'])

        self.assertEqual('Age of Onset', second_p_rows[0]['description'])
        self.assertEqual('"Infantile onset"', second_p_rows[0]['value'])
        self.assertEqual('HPO Terms (present)', second_p_rows[1]['description'])
        self.assertEqual('"HP:00000021,HP:023"', second_p_rows[1]['value'])
