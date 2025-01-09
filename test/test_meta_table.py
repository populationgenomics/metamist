# pylint: disable=invalid-overridden-method

import tempfile
from io import BytesIO
from typing import Any

import duckdb

from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from models.models import (
    PRIMARY_EXTERNAL_ORG,
)
from models.models.participant import ParticipantUpsertInternal
from models.models.sample import SampleUpsertInternal
from test.testbase import DbIsolatedTest, run_as_sync


def query_parquet(tables: dict[str, BytesIO], query: str) -> list[dict[str, Any]]:
    """
    Handle getting data out of a parquet file, this writes the file to a temporary
    folder and then reads it back in to duckdb to get the data. This probably isn't
    the most efficient, but the files are small so it shouldn't be too slow.
    """
    duck = duckdb.connect()

    with tempfile.TemporaryDirectory() as td:
        for table_name, table_bytes in tables.items():
            filename = f'{td}/{table_name}.parquet'
            with open(filename, 'wb') as f:
                table_bytes.seek(0)
                file_val = table_bytes.getvalue()
                f.write(file_val)
                f.close()

            duck.register(table_name, duck.read_parquet(filename))

        return duck.query(query).arrow().to_pylist()


class TestMetaTable(DbIsolatedTest):
    """Test meta table operations"""

    # pylint: disable=too-many-instance-attributes

    @run_as_sync
    async def setUp(self) -> None:
        # don't need to await because it's tagged @run_as_sync
        super().setUp()
        self.sl = SampleLayer(self.connection)
        self.pl = ParticipantLayer(self.connection)

    @run_as_sync
    async def test_export_participants(self):
        """
        Test getting a participant table from the export layer
        """

        await self.pl.upsert_participants(
            [
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'EX01', 'other': 'OTHER1'},
                    reported_sex=2,
                    karyotype='XX',
                    meta={'field': 1},
                ),
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'EX02'},
                    reported_sex=1,
                    karyotype='XY',
                    meta={'field': 2},
                ),
            ]
        )

        pts = await self.pl.export_participant_table(self.project_id)
        assert pts
        result = query_parquet(
            {'participants': pts},
            'SELECT * FROM participants order by participant_id',
        )

        self.assertEqual(2, len(result))
        self.assertEqual(1, result[0]['meta_field'])
        self.assertEqual(2, result[1]['meta_field'])
        self.assertEqual('EX01', result[0]['external_id'])
        self.assertEqual('EX02', result[1]['external_id'])
        self.assertEqual('OTHER1', result[0]['external_id_other'])

    @run_as_sync
    async def test_export_empty_table(self):
        """
        Test that exporting an empty table returns none
        """

        pts = await self.pl.export_participant_table(self.project_id)
        self.assertIsNone(pts)

    @run_as_sync
    async def test_export_samples(self):
        """
        Test getting a sample table from the export layer
        """

        await self.sl.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                active=True,
                meta={'field_1': 'field_1_value'},
            )
        )

        await self.sl.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test02'},
                type='blood',
                active=True,
                meta={'field_2': 'field_2_value'},
            )
        )

        samples = await self.sl.export_sample_table(self.project_id)
        assert samples
        result = query_parquet(
            {'samples': samples},
            'SELECT * FROM samples order by sample_id',
        )

        self.assertEqual(2, len(result))
        self.assertEqual('field_1_value', result[0]['meta_field_1'])
        self.assertEqual('field_2_value', result[1]['meta_field_2'])
        self.assertEqual('Test01', result[0]['external_id'])
        self.assertEqual('Test02', result[1]['external_id'])
